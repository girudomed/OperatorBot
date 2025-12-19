# Файл: app/telegram/handlers/admin_settings.py

"""
Раздел админ-панели «Настройки».

Позволяет просматривать логи, проверять ключевые переменные окружения,
перезапускать воркеры и очищать устаревшие данные кеша.
"""

from __future__ import annotations

import html
from io import BytesIO
from pathlib import Path
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

LOG_FILES = [
    Path("logs/operabot.log"),
    Path("logs/errors.log"),
    Path("logs/logs.log"),
]
MAX_LOG_LINES = 40
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
            "Сервисные операции для администраторов:\n"
            "• просматривайте последние логи;\n"
            "• очищайте устаревший кеш дашбордов."
        )
        keyboard = [
            [
                InlineKeyboardButton(
                    "📄 Логи", callback_data=AdminCB.create(AdminCB.SETTINGS, "logs")
                ),
            ],
            [
                InlineKeyboardButton(
                    "🧹 Очистить кеш",
                    callback_data=AdminCB.create(AdminCB.SETTINGS, "cleanup"),
                ),
            ],
            [InlineKeyboardButton("◀️ Назад", callback_data=AdminCB.create(AdminCB.BACK))],
        ]

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
        elif sub_action == "logs":
            await self._send_logs(query)
        elif sub_action == "cleanup":
            await self._cleanup_cache(query)
        else:
            await query.answer("Неизвестная команда", show_alert=True)

    async def _send_logs(self, query) -> None:
        log_text = None
        log_path = None
        for candidate in LOG_FILES:
            if candidate.exists():
                try:
                    lines = candidate.read_text(encoding="utf-8", errors="ignore").splitlines()
                except Exception as exc:
                    logger.warning("Не удалось прочитать лог %s: %s", candidate, exc)
                    continue
                log_path = candidate
                tail = lines[-MAX_LOG_LINES:] if len(lines) > MAX_LOG_LINES else lines
                log_text = "\n".join(tail)
                break

        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "🔄 Обновить",
                        callback_data=AdminCB.create(AdminCB.SETTINGS, "logs"),
                    ),
                    InlineKeyboardButton(
                        "◀️ Назад", callback_data=AdminCB.create(AdminCB.SETTINGS)
                    ),
                ]
            ]
        )
        if not log_text:
            await safe_edit_message(
                query,
                text="📄 Логи недоступны (файлы не найдены).",
                reply_markup=keyboard,
                parse_mode="HTML",
            )
            return

        await self._send_logs_file(query, log_text, log_path)
        await safe_edit_message(
            query,
            text="📄 Файл с логами отправлен отдельным сообщением.",
            reply_markup=keyboard,
            parse_mode="HTML",
        )


    async def _cleanup_cache(self, query) -> None:
        try:
            delete_query = """
                DELETE FROM operator_dashboards
                WHERE cached_at < DATE_SUB(NOW(), INTERVAL %s DAY)
            """
            await self.admin_repo.db.execute_with_retry(
                delete_query,
                params=(DEFAULT_CACHE_TTL_DAYS,),
                commit=True,
            )
            text = (
                f"🧹 Удалены записи из operator_dashboards старше {DEFAULT_CACHE_TTL_DAYS} дней."
            )
            logger.info("Old dashboard cache entries removed via admin settings")
        except Exception as exc:
            logger.exception("Не удалось очистить кеш дашбордов: %s", exc)
            text = f"⚠️ Ошибка при очистке кеша: {exc}"

        await safe_edit_message(
            query,
            text=text,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("◀️ Назад", callback_data=AdminCB.create(AdminCB.SETTINGS))]]
            ),
        )

    async def _send_logs_file(self, query, log_text: str, log_path: Optional[Path]) -> None:
        message = getattr(query, "message", None)
        if not message:
            logger.warning("Нет message для отправки логов файлом")
            return
        buffer = BytesIO()
        buffer.write(log_text.encode("utf-8"))
        buffer.seek(0)
        filename = (log_path.name if isinstance(log_path, Path) else "logs.txt") or "logs.txt"
        caption = f"📄 Логи ({filename})"
        await message.reply_document(
            document=buffer,
            filename=filename,
            caption=caption,
        )


def register_admin_settings_handlers(
    application: Application,
    admin_repo: AdminRepository,
    permissions: PermissionsManager,
) -> None:
    handler = AdminSettingsHandler(admin_repo, permissions)
    register_admin_callback_handler(application, AdminCB.SETTINGS, handler.handle_settings_action)
    logger.info("Admin settings handlers registered")
