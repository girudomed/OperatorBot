"""
Хендлеры для управления пользователями (approve/decline/block).
"""

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackQueryHandler, ContextTypes, Application

from app.db.repositories.admin import AdminRepository
from app.telegram.middlewares.permissions import PermissionsManager
from app.services.notifications import NotificationService
from app.logging_config import get_watchdog_logger
from app.utils.error_handlers import log_async_exceptions
from app.core.roles import role_name_from_id

logger = get_watchdog_logger(__name__)


class AdminUsersHandler:
    """Управление пользователями."""
    
    def __init__(
        self,
        admin_repo: AdminRepository,
        permissions: PermissionsManager,
        notifications: NotificationService
    ):
        self.admin_repo = admin_repo
        self.permissions = permissions
        self.notifications = notifications
    
    @log_async_exceptions
    async def show_users_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показывает список пользователей."""
        query = update.callback_query
        await query.answer()
        
        # Получаем параметр фильтра из callback_data
        # Формат: admin_users:pending или admin_users:approved
        parts = query.data.split(':')
        status_filter = parts[1] if len(parts) > 1 else 'pending'
        
        users = await self.admin_repo.get_all_users(status_filter)
        
        if not users:
            message = f"📋 Нет пользователей со статусом: {status_filter}"
            keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data="admin_back")]]
        else:
            message = f"👥 <b>Пользователи ({status_filter})</b>\n\n"
            
            keyboard = []
            for user in users[:10]:  # Показываем первых 10
                user_text = f"{user.get('full_name', 'Нет имени')} (@{user.get('username', 'нет')})"
                user_id = user.get('id')
                
                keyboard.append([
                    InlineKeyboardButton(
                        user_text,
                        callback_data=f"admin_user_details:{user_id}"
                    )
                ])
            
            # Фильтры
            filters = [
                InlineKeyboardButton("⏳ Pending", callback_data="admin_users:pending"),
                InlineKeyboardButton("✅ Approved", callback_data="admin_users:approved"),
                InlineKeyboardButton("🚫 Blocked", callback_data="admin_users:blocked")
            ]
            keyboard.append(filters)
            keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="admin_back")])
        
        await query.edit_message_text(
            text=message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    
    @log_async_exceptions
    async def show_user_details(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показывает детали пользователя с кнопками действий."""
        query = update.callback_query
        await query.answer()
        
        # Извлекаем user_id из callback_data
        user_id = int(query.data.split(':')[1])
        
        # Получаем информацию о пользователе из БД
        user_query = "SELECT * FROM users WHERE id = %s"
        user = await self.admin_repo.db.execute_with_retry(
            user_query, params=(user_id,), fetchone=True
        )
        
        if not user:
            await query.edit_message_text("❌ Пользователь не найден")
            return
        
        role_name = user.get('role') or role_name_from_id(user.get('role_id'))
        
        message = (
            f"👤 <b>Пользователь #{user_id}</b>\n\n"
            f"Имя: {user.get('full_name', 'Не указано')}\n"
            f"Username: @{user.get('username', 'нет')}\n"
            f"Extension: {user.get('extension', 'нет')}\n"
            f"Роль: <b>{role_name}</b>\n"
            f"Статус: <b>{user.get('status', 'pending')}</b>\n"
        )
        
        # Кнопки действий в зависимости от статуса
        keyboard = []
        
        if user.get('status') == 'pending':
            keyboard.append([
                InlineKeyboardButton("✅ Approve", callback_data=f"admin_approve:{user_id}"),
                InlineKeyboardButton("❌ Decline", callback_data=f"admin_decline:{user_id}")
            ])
        elif user.get('status') == 'approved':
            keyboard.append([
                InlineKeyboardButton("🚫 Block", callback_data=f"admin_block:{user_id}")
            ])
        elif user.get('status') == 'blocked':
            keyboard.append([
                InlineKeyboardButton("🔓 Unblock", callback_data=f"admin_unblock:{user_id}")
            ])
        
        keyboard.append([InlineKeyboardButton("◀️ К списку", callback_data="admin_users")])
        
        await query.edit_message_text(
            text=message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    
    @log_async_exceptions
    async def handle_approve(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Утверждает пользователя."""
        query = update.callback_query
        await query.answer()
        
        user_id = int(query.data.split(':')[1])
        actor_id = update.effective_user.id
        
        # Проверяем права
        can_approve = await self.permissions.can_approve(actor_id, update.effective_user.username)
        if not can_approve:
            await query.answer("❌ Недостаточно прав", show_alert=True)
            return
        
        # Утверждаем
        success = await self.admin_repo.approve_user(user_id, actor_id)
        
        if success:
            # Получаем данные пользователя для уведомления
            user = await self.admin_repo.db.execute_with_retry(
                "SELECT telegram_id, username FROM users WHERE id = %s",
                params=(user_id,), fetchone=True
            )
            
            if user:
                await self.notifications.notify_approval(
                    user['telegram_id'],
                    update.effective_user.full_name
                )
            
            await query.edit_message_text(
                "✅ Пользователь успешно одобрен. Теперь он может пользоваться ботом.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("◀️ К списку", callback_data="admin_users")
                ]])
            )
        else:
            await query.answer("❌ Ошибка при утверждении", show_alert=True)
    
    @log_async_exceptions
    async def handle_decline(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отклоняет заявку."""
        query = update.callback_query
        await query.answer()
        
        user_id = int(query.data.split(':')[1])
        actor_id = update.effective_user.id
        
        can_approve = await self.permissions.can_approve(actor_id, update.effective_user.username)
        if not can_approve:
            await query.answer("❌ Недостаточно прав", show_alert=True)
            return
        
        success = await self.admin_repo.decline_user(user_id, actor_id)
        
        if success:
            await query.edit_message_text(
                f"❌ Заявка #{user_id} отклонена",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("◀️ К списку", callback_data="admin_users")
                ]])
            )
        else:
            await query.answer("❌ Ошибка", show_alert=True)
    
    @log_async_exceptions
    async def handle_block(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Блокирует пользователя."""
        query = update.callback_query
        user_id = int(query.data.split(':')[1])
        actor_id = update.effective_user.id
        
        can_manage = await self.permissions.can_approve(actor_id, update.effective_user.username)
        if not can_manage:
            await query.answer("❌ Недостаточно прав", show_alert=True)
            return
        
        success = await self.admin_repo.block_user(user_id, actor_id)
        
        if success:
            await query.answer("🚫 Пользователь заблокирован. Он больше не сможет пользоваться ботом.", show_alert=True)
            await self.show_user_details(update, context)
        else:
            await query.answer("❌ Ошибка", show_alert=True)
    
    @log_async_exceptions
    async def handle_unblock(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Разблокирует пользователя."""
        query = update.callback_query
        user_id = int(query.data.split(':')[1])
        actor_id = update.effective_user.id
        
        can_manage = await self.permissions.can_approve(actor_id, update.effective_user.username)
        if not can_manage:
            await query.answer("❌ Недостаточно прав", show_alert=True)
            return
        
        success = await self.admin_repo.unblock_user(user_id, actor_id)
        
        if success:
            await query.answer("✅ Пользователь разблокирован и снова активен.", show_alert=True)
            await self.show_user_details(update, context)
        else:
            await query.answer("❌ Ошибка", show_alert=True)


def register_admin_users_handlers(
    application: Application,
    admin_repo: AdminRepository,
    permissions: PermissionsManager,
    notifications: NotificationService
):
    """Регистрирует хендлеры управления пользователями."""
    handler = AdminUsersHandler(admin_repo, permissions, notifications)
    
    # Список пользователей
    application.add_handler(
        CallbackQueryHandler(handler.show_users_list, pattern="^admin_users")
    )
    
    # Детали пользователя
    application.add_handler(
        CallbackQueryHandler(handler.show_user_details, pattern="^admin_user_details:")
    )
    
    # Действия
    application.add_handler(
        CallbackQueryHandler(handler.handle_approve, pattern="^admin_approve:")
    )
    application.add_handler(
        CallbackQueryHandler(handler.handle_decline, pattern="^admin_decline:")
    )
    application.add_handler(
        CallbackQueryHandler(handler.handle_block, pattern="^admin_block:")
    )
    application.add_handler(
        CallbackQueryHandler(handler.handle_unblock, pattern="^admin_unblock:")
    )
    
    logger.info("Admin users handlers registered")
