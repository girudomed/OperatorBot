# Файл: app/telegram/handlers/dev_messages.py

"""
Telegram handler для системы сообщений разработчику.
Позволяет пользователям отправлять сообщения Dev/SuperAdmin.
"""

from typing import Optional

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, ContextTypes, CommandHandler, MessageHandler, filters

from app.db.manager import DatabaseManager
from app.db.repositories.admin import AdminRepository
from app.telegram.middlewares.permissions import PermissionsManager
from app.telegram.utils.callback_data import AdminCB
from app.logging_config import get_watchdog_logger
from app.telegram.utils.admin_registry import register_admin_callback_handler

logger = get_watchdog_logger(__name__)


class DevMessagesHandler:
    """Handler для отправки сообщений разработчикам."""
    
    def __init__(
        self,
        db_manager: DatabaseManager,
        permissions: PermissionsManager,
        admin_repo: Optional[AdminRepository] = None,
    ):
        self.db_manager = db_manager
        self.admin_repo = admin_repo or AdminRepository(db_manager)
        self.permissions = permissions
        self.state_namespace = "dev_messages"
    
    async def message_dev_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Команда /message_dev
        Начинает процесс отправки сообщения разработчику.
        """
        user_id = update.effective_user.id
        
        user_record = await self.admin_repo.get_user_by_telegram_id(user_id)
        if not user_record or user_record.get('status') != 'approved':
            await update.message.reply_text(
                "🔒 Вы должны быть зарегистрированы в системе для отправки сообщений."
            )
            return
        
        state = self._get_state(context)
        state["awaiting_message"] = True
        
        await update.message.reply_text(
            "📨 <b>Отправка сообщения разработчику</b>\n\n"
            "Введите ваше сообщение. Оно будет отправлено всем разработчикам и администраторам.\n\n"
            "Отправьте /cancel для отмены.",
            parse_mode='HTML'
        )
    
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка сообщения от пользователя."""
        user_id = update.effective_user.id
        state = self._get_state(context)
        if not state.pop("awaiting_message", False):
            return
        
        message_text = update.message.text
        
        if not message_text or len(message_text.strip()) < 5:
            await update.message.reply_text(
                "❌ Сообщение слишком короткое. Попробуйте еще раз: /message_dev"
            )
            return
        
        # Получаем информацию об отправителе
        sender_name = update.effective_user.full_name
        sender_username = update.effective_user.username
        user_record = await self.admin_repo.get_user_by_telegram_id(user_id)
        operator_name = user_record.get('operator_name', 'Не указан') if user_record else 'Не указан'
        
        devs_and_admins = await self._get_debug_users()
        
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
                            callback_data=AdminCB.create(AdminCB.DEV_REPLY, user_id),
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
        state = self._get_state(context)
        cancelled = False
        if state.pop("awaiting_message", False):
            cancelled = True
        if state.pop("replying_to", None) is not None:
            cancelled = True
        if cancelled:
            await update.message.reply_text("❌ Отправка сообщения отменена.")
        else:
            await update.message.reply_text("Нечего отменять.")
    
    async def reply_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка нажатия кнопки 'Ответить'."""
        query = update.callback_query
        await query.answer()
        
        action, args = AdminCB.parse(query.data or "")
        if action != AdminCB.DEV_REPLY or not args:
            return
        try:
            target_user_id = int(args[0])
        except ValueError as exc:
            logger.warning("dev_messages: некорректный reply payload '%s': %s", query.data, exc)
            await query.message.reply_text("❌ Ошибка: некорректный ID пользователя.")
            return
        
        state = self._get_state(context)
        state['replying_to'] = target_user_id
        
        await query.message.reply_text(
            f"✉️ <b>Ответ пользователю</b>\n\n"
            f"Введите ваш ответ. Он будет отправлен пользователю с ID {target_user_id}.\n\n"
            "Отправьте /cancel для отмены.",
            parse_mode='HTML'
        )
    
    async def handle_reply(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка ответа разработчика."""
        user_id = update.effective_user.id
        
        state = self._get_state(context)
        if 'replying_to' not in state:
            return
        
        target_user_id = state.pop('replying_to')
        reply_text = update.message.text
        
        if not reply_text or len(reply_text.strip()) < 3:
            await update.message.reply_text(
                "❌ Сообщение слишком короткое."
            )
            return
        
        # Получаем информацию о разработчике
        can_debug = await self.permissions.has_permission(
            user_id,
            'debug',
            update.effective_user.username,
            require_approved=False,
        )
        if not can_debug:
            await update.message.reply_text("🔒 У вас нет прав для ответа пользователям.")
            return
        
        dev_record = await self.admin_repo.get_user_by_telegram_id(user_id)
        dev_name = update.effective_user.full_name
        role_payload = dev_record.get('role') if dev_record else None
        role_name = "Разработчик" if can_debug else "Администратор"
        if isinstance(role_payload, dict):
            role_name = role_payload.get('name') or role_name
        
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
        """Возвращает handlers для регистрации (без callback'ов adm:*)."""
        return [
            CommandHandler('message_dev', self.message_dev_command),
            CommandHandler('cancel', self.cancel_command),
            MessageHandler(
                filters.TEXT & ~filters.COMMAND,
                self._combined_message_handler,
                block=False,
            )
        ]
    
    async def _combined_message_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Комбинированный handler для текстовых сообщений.
        Обрабатывает как новые сообщения dev, так и ответы разработчиков.
        """
        state = self._get_state(context)
        if 'replying_to' in state:
            await self.handle_reply(update, context)
        elif state.get('awaiting_message'):
            await self.handle_message(update, context)

    async def _get_debug_users(self):
        """Получить пользователей с правом debug."""
        admins = await self.admin_repo.get_admins()
        result = []
        for admin in admins:
            telegram_id = admin.get('telegram_id')
            username = admin.get('username')
            if not telegram_id:
                continue
            if await self.permissions.has_permission(
                telegram_id,
                'debug',
                username,
                require_approved=False,
            ):
                result.append(admin)
        return result

    def _get_state(self, context: ContextTypes.DEFAULT_TYPE) -> dict:
        state = context.user_data.get(self.state_namespace)
        if not isinstance(state, dict):
            state = {}
            context.user_data[self.state_namespace] = state
        return state


def register_dev_messages_handlers(
    application: Application,
    db_manager: DatabaseManager,
    permissions: PermissionsManager,
    admin_repo: Optional[AdminRepository] = None,
) -> None:
    handler = DevMessagesHandler(db_manager, permissions, admin_repo)
    for entry in handler.get_handlers():
        application.add_handler(entry)
    register_admin_callback_handler(application, AdminCB.DEV_REPLY, handler.reply_callback)
