# Файл: app/telegram/handlers/start.py

"""
Обновленный /start handler с динамическим контентом по ролям.

Короткие сообщения, role-based клавиатуры, БЕЗ списков команд.
"""

from telegram import Update
from telegram.ext import ContextTypes, CommandHandler

from app.db.manager import DatabaseManager
from app.db.repositories.users import UserRepository
from app.db.repositories.roles import RolesRepository
from app.core.roles import role_name_from_id, role_display_name_from_name
from app.telegram.utils.keyboard_builder import KeyboardBuilder
from app.telegram.middlewares.permissions import PermissionsManager
from app.logging_config import get_watchdog_logger
from app.utils.error_handlers import log_async_exceptions

logger = get_watchdog_logger(__name__)
DB_ERROR_MESSAGE = "Ошибка доступа к базе. Проверьте конфигурацию/схему БД."


class StartHandler:
    """Handler для команды /start с role-based UI."""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.user_repo = UserRepository(db_manager)
        self.roles_repo = RolesRepository(db_manager)
        self.keyboard_builder = KeyboardBuilder(self.roles_repo)
        self.permissions = PermissionsManager(db_manager)
    
    @log_async_exceptions
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Команда /start - приветствие с role-based клавиатурой.
        """
        user_id = update.effective_user.id
        username = update.effective_user.username
        user_name = update.effective_user.full_name
        
        logger.info(f"[START] Command from {user_id} ({username})")
        
        # Проверить Supreme/Dev Admin
        is_supreme = self.permissions.is_supreme_admin(user_id, username)
        is_dev = self.permissions.is_dev_admin(user_id, username)
        
        user_ctx = context.user_data.get("user_ctx")
        if not user_ctx:
            try:
                user_ctx = await self.user_repo.get_user_context_by_telegram_id(user_id)
                if user_ctx:
                    context.user_data["user_ctx"] = user_ctx
            except Exception:
                logger.exception(
                    "[START] Ошибка чтения пользователя",
                    extra={"user_id": user_id, "username": username},
                )
                await update.message.reply_text(DB_ERROR_MESSAGE)
                return
        
        if not user_ctx:
            await update.message.reply_text(
                f"👋 Добро пожаловать, {user_name}!\n\n"
                "Вы не зарегистрированы в системе.\n"
                "Используйте /register для регистрации."
            )
            return
        
        status = (user_ctx.get('status') or '').lower()
        
        if status == 'pending':
            await update.message.reply_text(
                f"👋 Здравствуйте, {user_name}!\n\n"
                "⏳ Ваша заявка ожидает одобрения администратором.\n\n"
                "Вы получите уведомление когда доступ будет предоставлен."
            )
            return
        
        if status == 'blocked':
            await update.message.reply_text(
                "❌ Ваш доступ к боту заблокирован.\n\n"
                "Для разъяснений обратитесь к администратору."
            )
            return
        
        # Approved пользователь
        role_id = int(user_ctx.get('role_id') or 1)
        role_slug = (user_ctx.get('role_name') or role_name_from_id(role_id)).lower()
        try:
            role_name = role_display_name_from_name(role_slug)
            perms = {
                'can_view_own_stats': bool(user_ctx.get('can_view_own_stats')),
                'can_view_all_stats': bool(user_ctx.get('can_view_all_stats')),
                'can_manage_users': bool(user_ctx.get('can_manage_users')),
                'can_debug': bool(user_ctx.get('can_debug')),
            }
            keyboard = await self.keyboard_builder.build_main_keyboard(
                role_id, is_supreme, is_dev, perms_override=perms
            )
        except Exception:
            logger.exception(
                "[START] Ошибка получения данных роли",
                extra={"user_id": user_id, "role_id": role_id},
            )
            await update.message.reply_text(DB_ERROR_MESSAGE)
            return
        
        # Сообщение в зависимости от роли
        if is_supreme or is_dev:
            message = (
                f"👋 Добро пожаловать, **{user_name}**!\n\n"
                f"🔱 Вы авторизованы как **{'Founder' if is_supreme else 'Developer'}**.\n\n"
                "Доступен **полный контроль** всех функций системы.\n\n"
                "⚠️ Опасные операции требуют подтверждения."
            )
        elif role_slug in ('founder', 'developer', 'superadmin'):
            message = (
                f"👋 Добро пожаловать, **{user_name}**!\n\n"
                f"👑 Вы авторизованы как **{role_name}**.\n\n"
                "Доступны:\n"
                "• 📊 Отчёты по всем операторам\n"
                "• 🔍 Поиск звонков\n"
                "• 👥 Управление пользователями и ролями\n"
                "• ⚙️ Системные функции\n\n"
                "Используйте кнопки ниже для навигации."
            )
        elif perms.get('can_manage_users'):  # Админские роли
            message = (
                f"👋 Добро пожаловать, **{user_name}**!\n\n"
                f"🛡️ Вы авторизованы как **{role_name}**.\n\n"
                "Доступны:\n"
                "• 📊 Отчёты и статистика\n"
                "• 🔍 Поиск звонков\n"
                "• 👥 Управление пользователями\n\n"
                "Для настройки доступов → «Пользователи и роли»."
            )
        elif perms.get('can_view_all_stats'):  # Руководство/маркетинг
            message = (
                f"👋 Добро пожаловать, **{user_name}**!\n\n"
                f"📊 Вы авторизованы как **{role_name}**.\n\n"
                "Доступны:\n"
                "• 📊 Отчёты по всем операторам\n"
                "• 🔍 Поиск звонков\n\n"
                "Начните с раздела «Отчёты» или «Поиск звонка»."
            )
        else:  # Оператор
            message = (
                f"👋 Добро пожаловать, **{user_name}**!\n\n"
                f"👤 Вы авторизованы как **{role_name}**.\n\n"
                "Доступны:\n"
                "• 📊 Моя статистика\n"
                "• 🔍 Мои звонки\n\n"
                "Используйте кнопки ниже."
            )
        
        await update.message.reply_text(
            message,
            parse_mode='Markdown',
            reply_markup=keyboard
        )
        
        logger.info(f"[START] Sent welcome for {user_id}, role={role_slug}")
    
    def get_handler(self):
        """Получить CommandHandler для регистрации."""
        return CommandHandler('start', self.start_command)
