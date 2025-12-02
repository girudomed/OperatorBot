"""
Быстрые команды для админских действий.
"""

from telegram import Update
from telegram.ext import CommandHandler, ContextTypes, Application

from app.db.repositories.admin import AdminRepository
from app.telegram.middlewares.permissions import PermissionsManager
from app.services.notifications import NotificationService
from app.logging_config import get_watchdog_logger
from app.utils.error_handlers import log_async_exceptions
from app.core.roles import role_name_from_id

logger = get_watchdog_logger(__name__)


class AdminCommandsHandler:
    """Быстрые команды для админов."""
    
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
    async def approve_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Команда /approve <user_id>
        Быстрое утверждение пользователя.
        """
        user = update.effective_user
        
        # Проверка прав
        can_approve = await self.permissions.can_approve(user.id, user.username)
        if not can_approve:
            await update.message.reply_text("❌ Недостаточно прав")
            return
        
        # Проверка аргументов
        if not context.args or len(context.args) < 1:
            await update.message.reply_text(
                "❌ Использование: /approve <user_id>\n"
                "Пример: /approve 123"
            )
            return
        
        try:
            user_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text("❌ user_id должен быть числом")
            return
        
        # Утверждаем
        success = await self.admin_repo.approve_user(user_id, user.id)
        
        if success:
            # Уведомляем пользователя
            user_data = await self.admin_repo.db.execute_with_retry(
                "SELECT telegram_id FROM users WHERE id = %s",
                params=(user_id,), fetchone=True
            )
            
            if user_data:
                await self.notifications.notify_approval(
                    user_data['telegram_id'],
                    user.full_name
                )
            
            await update.message.reply_text(f"✅ Пользователь #{user_id} утвержден!")
        else:
            await update.message.reply_text(f"❌ Не удалось утвердить пользователя #{user_id}")
    
    @log_async_exceptions
    async def make_admin_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Команда /make_admin <user_id>
        Повышает пользователя до admin.
        """
        user = update.effective_user
        
        # Проверка аргументов
        if not context.args or len(context.args) < 1:
            await update.message.reply_text(
                "❌ Использование: /make_admin <user_id>\n"
                "Пример: /make_admin 123"
            )
            return
        
        try:
            target_user_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text("❌ user_id должен быть числом")
            return
        
        # Проверка прав
        can_promote = await self.permissions.can_promote(
            user.id, 'admin', user.username
        )
        
        if not can_promote:
            await update.message.reply_text("❌ Недостаточно прав для повышения")
            return
        
        # Повышаем
        success = await self.admin_repo.promote_user(
            target_user_id, 'admin', user.id
        )
        
        if success:
            # Уведомляем
            user_data = await self.admin_repo.db.execute_with_retry(
                "SELECT telegram_id FROM users WHERE id = %s",
                params=(target_user_id,), fetchone=True
            )
            
            if user_data:
                await self.notifications.notify_promotion(
                    user_data['telegram_id'],
                    'admin',
                    user.full_name
                )
            
            await update.message.reply_text(
                "✅ Роль пользователя обновлена."
            )
        else:
            await update.message.reply_text("❌ Ошибка при повышении")
    
    @log_async_exceptions
    async def make_superadmin_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Команда /make_superadmin <user_id>
        Повышает до superadmin (только для supreme/dev admin).
        """
        user = update.effective_user
        
        # Проверка прав (только supreme/dev)
        can_promote = await self.permissions.can_promote(
            user.id, 'superadmin', user.username
        )
        
        if not can_promote:
            await update.message.reply_text(
                "❌ Только Supreme Admin может назначать superadmin"
            )
            return
        
        if not context.args or len(context.args) < 1:
            await update.message.reply_text(
                "❌ Использование: /make_superadmin <user_id>"
            )
            return
        
        try:
            target_user_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text("❌ user_id должен быть числом")
            return
        
        success = await self.admin_repo.promote_user(
            target_user_id, 'superadmin', user.id
        )
        
        if success:
            await update.message.reply_text(
                "✅ Роль пользователя обновлена."
            )
        else:
            await update.message.reply_text("❌ Ошибка при повышении")
    
    @log_async_exceptions
    async def admins_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Команда /admins
        Показывает список всех администраторов.
        """
        user = update.effective_user
        
        # Проверка доступа
        is_admin = await self.permissions.is_admin(user.id, user.username)
        if not is_admin:
            await update.message.reply_text("❌ Недостаточно прав")
            return
        
        admins = await self.admin_repo.get_admins()
        
        if not admins:
            await update.message.reply_text("👑 Нет администраторов в системе")
            return
        
        message = "👑 <b>Список администраторов:</b>\n\n"
        
        for admin in admins:
            role_name = admin.get('role') or role_name_from_id(admin.get('role_id'))
            role_emoji = "⭐" if role_name == 'superadmin' else "👤"
            message += (
                f"{role_emoji} <b>{admin['full_name']}</b>\n"
                f"   @{admin.get('username', 'нет')} | "
                f"Role: {role_name}\n\n"
            )
        
        await update.message.reply_text(message, parse_mode='HTML')


def register_admin_commands_handlers(
    application: Application,
    admin_repo: AdminRepository,
    permissions: PermissionsManager,
    notifications: NotificationService
):
    """Регистрирует быстрые команды админов."""
    handler = AdminCommandsHandler(admin_repo, permissions, notifications)
    
    application.add_handler(CommandHandler("approve", handler.approve_command))
    application.add_handler(CommandHandler("make_admin", handler.make_admin_command))
    application.add_handler(CommandHandler("make_superadmin", handler.make_superadmin_command))
    application.add_handler(CommandHandler("admins", handler.admins_command))
    
    logger.info("Admin commands handlers registered")
