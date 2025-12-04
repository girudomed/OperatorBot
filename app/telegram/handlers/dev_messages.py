"""
Telegram handler для системы сообщений разработчику.
Позволяет пользователям отправлять сообщения Dev/SuperAdmin.
"""

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, CommandHandler, CallbackQueryHandler, MessageHandler, filters

from app.db.manager import DatabaseManager
from app.db.repositories.users import UserRepository
from app.services.permissions import PermissionChecker, ROLE_DEV, ROLE_SUPER_ADMIN
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)


class DevMessagesHandler:
    """Handler для отправки сообщений разработчикам."""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.user_repo = UserRepository(db_manager)
        self.permission_checker = PermissionChecker(db_manager)
        # Хранилище для отслеживания состояния ожидания сообщения
        self.waiting_for_message = {}
    
    async def message_dev_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Команда /message_dev
        Начинает процесс отправки сообщения разработчику.
        """
        user_id = update.effective_user.id
        
        # Проверяем доступ
        has_access = await self.permission_checker.can_message_dev(user_id)
        if not has_access:
            await update.message.reply_text(
                "🔒 Вы должны быть зарегистрированы в системе для отправки сообщений."
            )
            return
        
        # Устанавливаем флаг ожидания сообщения
        self.waiting_for_message[user_id] = True
        
        await update.message.reply_text(
            "📨 <b>Отправка сообщения разработчику</b>\n\n"
            "Введите ваше сообщение. Оно будет отправлено всем разработчикам и администраторам.\n\n"
            "Отправьте /cancel для отмены.",
            parse_mode='HTML'
        )
    
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка сообщения от пользователя."""
        user_id = update.effective_user.id
        
        # Проверяем, ждем ли мы сообщение от этого пользователя
        if user_id not in self.waiting_for_message:
            return
        
        # Убираем флаг ожидания
        del self.waiting_for_message[user_id]
        
        message_text = update.message.text
        
        if not message_text or len(message_text.strip()) < 5:
            await update.message.reply_text(
                "❌ Сообщение слишком короткое. Попробуйте еще раз: /message_dev"
            )
            return
        
        # Получаем информацию об отправителе
        user_record = await self.user_repo.get_user_by_telegram_id(user_id)
        
        sender_name = update.effective_user.full_name
        sender_username = update.effective_user.username
        operator_name = user_record.get('operator_name', 'Не указан') if user_record else 'Не указан'
        
        # Получаем всех Dev и SuperAdmin
        devs_and_admins = await self._get_devs_and_admins()
        
        if not devs_and_admins:
            await update.message.reply_text(
                "❌ Не удалось найти разработчиков для отправки сообщения.\n"
                "Пожалуйста, обратитесь к администратору напрямую."
            )
            return
        
        # Формируем сообщение для разработчиков
        dev_message = f"""
📨 <b>Новое сообщение</b>

<b>От:</b> {sender_name}
<b>Username:</b> @{sender_username or 'не указан'}
<b>Оператор:</b> {operator_name}
<b>Telegram ID:</b> <code>{user_id}</code>

━━━━━━━━━━━━━━━━━━━━

{message_text}
"""
        
        # Отправляем сообщение всем разработчикам
        sent_count = 0
        for dev in devs_and_admins:
            dev_telegram_id = dev.get('user_id')
            if dev_telegram_id and dev_telegram_id != user_id:  # Не отправляем себе
                try:
                    # Кнопка для ответа
                    keyboard = [[
                        InlineKeyboardButton(
                            "✉️ Ответить",
                            callback_data=f"reply_to_{user_id}"
                        )
                    ]]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    
                    await context.bot.send_message(
                        chat_id=dev_telegram_id,
                        text=dev_message,
                        parse_mode='HTML',
                        reply_markup=reply_markup
                    )
                    sent_count += 1
                except Exception as e:
                    logger.error(f"Ошибка отправки сообщения dev {dev_telegram_id}: {e}")
        
        if sent_count > 0:
            await update.message.reply_text(
                f"✅ Ваше сообщение отправлено {sent_count} разработчикам.\n"
                "Они свяжутся с вами в ближайшее время."
            )
        else:
            await update.message.reply_text(
                "❌ Не удалось отправить сообщение. Попробуйте позже."
            )
    
    async def cancel_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отмена отправки сообщения."""
        user_id = update.effective_user.id
        
        if user_id in self.waiting_for_message:
            del self.waiting_for_message[user_id]
            await update.message.reply_text("❌ Отправка сообщения отменена.")
        else:
            await update.message.reply_text("Нечего отменять.")
    
    async def reply_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка нажатия кнопки 'Ответить'."""
        query = update.callback_query
        await query.answer()
        
        # Извлекаем user_id из callback_data
        data = query.data
        if not data.startswith('reply_to_'):
            return
        
        try:
            target_user_id = int(data.replace('reply_to_', ''))
        except ValueError:
            await query.message.reply_text("❌ Ошибка: некорректный ID пользователя.")
            return
        
        # Устанавливаем флаг ожидания ответа
        context.user_data['replying_to'] = target_user_id
        
        await query.message.reply_text(
            f"✉️ <b>Ответ пользователю</b>\n\n"
            f"Введите ваш ответ. Он будет отправлен пользователю с ID {target_user_id}.\n\n"
            "Отправьте /cancel для отмены.",
            parse_mode='HTML'
        )
    
    async def handle_reply(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка ответа разработчика."""
        user_id = update.effective_user.id
        
        # Проверяем, это ответ разработчика?
        if 'replying_to' not in context.user_data:
            return
        
        target_user_id = context.user_data.pop('replying_to')
        reply_text = update.message.text
        
        if not reply_text or len(reply_text.strip()) < 3:
            await update.message.reply_text(
                "❌ Сообщение слишком короткое."
            )
            return
        
        # Получаем информацию о разработчике
        dev_record = await self.user_repo.get_user_by_telegram_id(user_id)
        dev_name = update.effective_user.full_name
        role_name = "Разработчик" if dev_record and dev_record.get('role_id') == ROLE_DEV else "Администратор"
        
        # Формируем сообщение для пользователя
        user_message = f"""
📬 <b>Ответ от {role_name}</b>

<b>От:</b> {dev_name}

━━━━━━━━━━━━━━━━━━━━

{reply_text}
"""
        
        try:
            await context.bot.send_message(
                chat_id=target_user_id,
                text=user_message,
                parse_mode='HTML'
            )
            
            await update.message.reply_text(
                "✅ Ваш ответ отправлен пользователю."
            )
        except Exception as e:
            logger.error(f"Ошибка отправки ответа пользователю {target_user_id}: {e}")
            await update.message.reply_text(
                "❌ Не удалось отправить ответ. Возможно, пользователь заблокировал бота."
            )
    
    async def _get_devs_and_admins(self) -> list:
        """Получить список всех Dev и SuperAdmin для отправки сообщений."""
        query = """
        SELECT user_id, username, full_name, role_id
        FROM users
        WHERE role_id IN (%s, %s)
          AND status = 'approved'
        """
        
        result = await self.db_manager.execute_query(
            query,
            (ROLE_DEV, ROLE_SUPER_ADMIN),
            fetchall=True
        )
        
        return result or []
    
    def get_handlers(self):
        """Возвращает список handlers для регистрации."""
        return [
            CommandHandler('message_dev', self.message_dev_command),
            CommandHandler('cancel', self.cancel_command),
            CallbackQueryHandler(self.reply_callback, pattern='^reply_to_'),
            # MessageHandler для перехвата текстовых сообщений (должен быть последним)
            MessageHandler(
                filters.TEXT & ~filters.COMMAND,
                self._combined_message_handler
            )
        ]
    
    async def _combined_message_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Комбинированный handler для текстовых сообщений.
        Обрабатывает как новые сообщения dev, так и ответы разработчиков.
        """
        user_id = update.effective_user.id
        
        # Проверяем, ожидаем ли сообщение или это ответ
        if 'replying_to' in context.user_data:
            await self.handle_reply(update, context)
        elif user_id in self.waiting_for_message:
            await self.handle_message(update, context)
