# Файл: app/telegram/handlers/start.py

"""
Обновленный /start handler с динамическим контентом по ролям.

Короткие сообщения, role-based клавиатуры, БЕЗ списков команд.
"""

import html
from typing import Dict
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler

from app.db.manager import DatabaseManager
from app.db.repositories.users import UserRepository
from app.db.repositories.roles import RolesRepository
from app.core.roles import role_name_from_id, role_display_name_from_name
from app.telegram.keyboards.reply_main import ReplyMainKeyboardBuilder
from app.telegram.keyboards.exceptions import KeyboardPermissionsError
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
        self.keyboard_builder = ReplyMainKeyboardBuilder(self.roles_repo)
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
        
        try:
            user_ctx = await self.user_repo.get_user_context_by_telegram_id(user_id)
        except Exception:
            logger.exception(
                "[START] Ошибка чтения пользователя",
                extra={"user_id": user_id, "username": username},
            )
            await update.message.reply_text(DB_ERROR_MESSAGE)
            return
        
        effective_message = update.effective_message
        safe_user_name = html.escape(user_name or "пользователь")

        if not user_ctx:
            if effective_message:
                await effective_message.reply_text(
                    f"👋 Добро пожаловать, <b>{safe_user_name}</b>!\n\n"
                    "Вы не зарегистрированы в системе.\n"
                    "Используйте /register для регистрации.",
                    parse_mode="HTML",
                )
            return
        
        status = (user_ctx.get('status') or '').lower()
        
        if status == 'pending':
            if effective_message:
                await effective_message.reply_text(
                    f"👋 Здравствуйте, <b>{safe_user_name}</b>!\n\n"
                    "⏳ Ваша заявка ожидает одобрения администратором.\n\n"
                    "Вы получите уведомление когда доступ будет предоставлен.",
                    parse_mode="HTML",
                )
            return
        
        if status == 'blocked':
            if effective_message:
                await effective_message.reply_text(
                    "❌ Ваш доступ к боту заблокирован.\n\n"
                    "Для разъяснений обратитесь к администратору.",
                    parse_mode="HTML",
                )
            return
        
        # Approved пользователь
        role_id = int(user_ctx.get('role_id') or 1)
        role_slug = (user_ctx.get('role_name') or role_name_from_id(role_id)).lower()
        perms: Dict[str, bool] = {}
        try:
            role_display = role_display_name_from_name(role_slug)
            perms = self._build_effective_permissions(user_ctx, is_supreme, is_dev)
            keyboard = await self.keyboard_builder.build_main_keyboard(
                role_id, perms_override=perms
            )
        except KeyboardPermissionsError as exc:
            if effective_message:
                await effective_message.reply_text(
                    "❌ Временная ошибка доступа. Попробуйте позже.",
                    parse_mode="HTML",
                    reply_markup=exc.fallback_keyboard,
                )
            return
        except Exception:
            logger.exception(
                "[START] Ошибка получения данных роли",
                extra={"user_id": user_id, "role_id": role_id},
            )
            if effective_message:
                await effective_message.reply_text(DB_ERROR_MESSAGE, parse_mode="HTML")
            return
        
        safe_role_name = html.escape(role_display)
        message = self._build_role_message(safe_user_name, safe_role_name, perms)

        if effective_message:
            await effective_message.reply_text(
                message,
                parse_mode='HTML',
                reply_markup=keyboard
            )
        
        logger.info(f"[START] Sent welcome for {user_id}, role={role_slug}")
    
    def _build_effective_permissions(
        self,
        user_ctx: Dict[str, bool],
        is_supreme: bool,
        is_dev: bool,
    ) -> Dict[str, bool]:
        perms = {
            'can_view_own_stats': bool(user_ctx.get('can_view_own_stats')),
            'can_view_all_stats': bool(user_ctx.get('can_view_all_stats')),
            'can_manage_users': bool(user_ctx.get('can_manage_users')),
            'can_debug': bool(user_ctx.get('can_debug')),
        }
        if is_supreme or is_dev:
            perms.update({
                'can_view_own_stats': True,
                'can_view_all_stats': True,
                'can_manage_users': True,
                'can_debug': True,
            })
        return perms
    
    @staticmethod
    def _build_role_message(
        safe_user_name: str,
        safe_role_name: str,
        perms: Dict[str, bool],
    ) -> str:
        if perms.get('can_debug'):
            return (
                f"👋 Добро пожаловать, <b>{safe_user_name}</b>!\n\n"
                f"🔧 Вы авторизованы как <b>{safe_role_name}</b>.\n\n"
                "Доступны:\n"
                "• ⚙️ Системные функции\n"
                "• 👥 Управление пользователями\n"
                "• 📊 Отчёты по всем операторам\n"
                "• 🔍 Поиск звонков\n\n"
                "⚠️ Опасные операции требуют подтверждения."
            )
        if perms.get('can_manage_users'):
            return (
                f"👋 Добро пожаловать, <b>{safe_user_name}</b>!\n\n"
                f"🛡️ Вы авторизованы как <b>{safe_role_name}</b>.\n\n"
                "Доступны:\n"
                "• 📊 Отчёты и статистика\n"
                "• 🔍 Поиск звонков\n"
                "• 👥 Управление пользователями\n\n"
                "Для настройки доступов откройте «Пользователи и роли»."
            )
        if perms.get('can_view_all_stats'):
            return (
                f"👋 Добро пожаловать, <b>{safe_user_name}</b>!\n\n"
                f"📊 Вы авторизованы как <b>{safe_role_name}</b>.\n\n"
                "Доступны:\n"
                "• 📊 Отчёты по всем операторам\n"
                "• 🔍 Поиск звонков\n\n"
                "Начните с раздела «Отчёты» или «Поиск звонка»."
            )
        return (
            f"👋 Добро пожаловать, <b>{safe_user_name}</b>!\n\n"
            f"👤 Вы авторизованы как <b>{safe_role_name}</b>.\n\n"
            "Доступны:\n"
            "• 📊 Моя статистика\n"
            "• 🔍 Мои звонки\n\n"
            "Используйте кнопки ниже."
        )
    
    def get_handler(self):
        """Получить CommandHandler для регистрации."""
        return CommandHandler('start', self.start_command)
