# Файл: app/telegram/handlers/admin_settings.py

"""
Раздел админ-панели «Настройки».

Позволяет проверять ключевые переменные окружения,
перезапускать воркеры и очищать устаревшие данные кеша.
"""

from __future__ import annotations

import html
from typing import Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, ContextTypes

from app.config import OPENAI_API_KEY, TELEGRAM_TOKEN, DB_CONFIG, SENTRY_DSN
from app.db.repositories.admin import AdminRepository
from app.logging_config import get_watchdog_logger
from app.telegram.middlewares.permissions import PermissionsManager
from app.telegram.utils.messages import safe_edit_message, MAX_MESSAGE_CHUNK
from app.telegram.utils.logging import describe_user
from app.telegram.utils.callback_data import AdminCB
from app.utils.error_handlers import log_async_exceptions
from app.telegram.utils.admin_registry import register_admin_callback_handler
from app.workers.task_worker import start_workers, stop_workers

logger = get_watchdog_logger(__name__)

DEFAULT_CACHE_TTL_DAYS = 30


class AdminSettingsHandler:
    """Хендлер для раздела ⚙️ Настройки."""

    def __init__(self, admin_repo: AdminRepository, permissions: PermissionsManager):
        self.admin_repo = admin_repo
        self.permissions = permissions

    async def _ensure_access(self, user_id: int, username: Optional[str]) -> bool:
        """
        Доступ к настройкам разрешаем только владельцам продукта (founder/developer)
        и bootstrap-админам.
        Остальным показываем предупреждение и не выполняем действие.
        """
        return await self.permissions.has_top_privileges(user_id, username)

    @log_async_exceptions
    async def show_settings_menu(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        query = update.callback_query
        if not query:
            return
        await query.answer()

        user = update.effective_user
        if not await self._ensure_access(user.id, user.username):
            await query.answer("Доступ запрещён", show_alert=True)
            logger.warning("User %s tried to open settings", describe_user(user))
            return

        message = (
            "⚙️ <b>Настройки</b>\n\n"
            "На данный момент нет доступных операций."
        )
        keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data=AdminCB.create(AdminCB.BACK))]]

        await safe_edit_message(
            query,
            text=message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="HTML",
        )

    @log_async_exceptions
    async def handle_settings_action(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        query = update.callback_query
        if not query:
            return
        await query.answer()

        user = update.effective_user
        if not await self._ensure_access(user.id, user.username):
            await query.answer("Доступ запрещён", show_alert=True)
            logger.warning(
                "User %s tried to execute settings action %s",
                describe_user(user),
                query.data,
            )
            return

        action, args = AdminCB.parse(query.data or "")
        if action != AdminCB.SETTINGS:
            return
        sub_action = args[0] if args else "menu"
        logger.info(
            "Admin %s triggered settings action %s",
            describe_user(user),
            sub_action,
        )

        if sub_action == "menu":
            await self.show_settings_menu(update, context)
        else:
            await self.show_settings_menu(update, context)


def register_admin_settings_handlers(
    application: Application,
    admin_repo: AdminRepository,
    permissions: PermissionsManager,
) -> None:
    handler = AdminSettingsHandler(admin_repo, permissions)
    register_admin_callback_handler(application, AdminCB.SETTINGS, handler.handle_settings_action)
    logger.info("Admin settings handlers registered")
