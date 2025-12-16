# Файл: app/telegram/handlers/system_menu.py

"""
Обработчики кнопок главного меню: «⚙️ Система» и «ℹ️ Помощь».

Позволяет запускать базовые диагностические действия прямо из Telegram.
"""

from __future__ import annotations

from collections import deque
from functools import partial
from pathlib import Path
from typing import Optional

from telegram import Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from app.db.manager import DatabaseManager
from app.db.repositories.roles import RolesRepository
from app.db.utils_schema import clear_schema_cache
from app.logging_config import get_watchdog_logger
from app.services.admin_logger import AdminActionLogger
from app.services.call_analytics_sync import CallAnalyticsSyncService
from app.telegram.handlers.auth import help_command
from app.telegram.middlewares.permissions import PermissionsManager
from app.telegram.utils.keyboard_builder import KeyboardBuilder

logger = get_watchdog_logger(__name__)


class SystemMenuHandler:
    """Отвечает за вывод системного меню и обработку его действий."""

    def __init__(
        self,
        db_manager: DatabaseManager,
        permissions: PermissionsManager,
    ):
        self.db_manager = db_manager
        self.permissions = permissions
        self.roles_repo = RolesRepository(db_manager)
        self.keyboard_builder = KeyboardBuilder(self.roles_repo)
        self.analytics_service = CallAnalyticsSyncService(db_manager)
        self.action_logger = AdminActionLogger(db_manager)

    async def handle_system_command(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Открывает системное меню по команде или кнопке."""
        message = update.effective_message
        user = update.effective_user
        if not message or not user:
            return

        if not await self._can_use_system(user.id, user.username):
            await message.reply_text(
                "❌ У вас нет доступа к системным действиям. Обратитесь к разработчику."
            )
            return

        include_cache_reset = self.permissions.is_dev_admin(user.id, user.username)

        await message.reply_text(
            "⚙️ <b>Системные функции</b>\nВыберите действие:",
            parse_mode="HTML",
            reply_markup=self.keyboard_builder.build_system_menu(include_cache_reset),
        )

    async def handle_system_callback(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Обработка callback-кнопок системного меню."""
        query = update.callback_query
        user = update.effective_user
        if not query or not user:
            return

        await query.answer()

        if not await self._can_use_system(user.id, user.username):
            await query.answer("Недостаточно прав", show_alert=True)
            return

        action = query.data.replace("system_", "", 1)
        include_cache_reset = self.permissions.is_dev_admin(user.id, user.username)

        try:
            if action == "status":
                text = await self._collect_status()
            elif action == "errors":
                text = await self._collect_recent_errors()
            elif action == "check":
                text = await self._run_integrity_checks()
            elif action == "sync":
                text = await self._run_sync()
            elif action == "clear_cache":
                if not include_cache_reset:
                    text = "❌ Доступ к очистке кеша разрешён только Dev Admin."
                else:
                    text = await self._clear_caches()
            else:
                text = "Неизвестное действие."
            await self._log_system_action(user.id, action, text)
        except Exception as exc:
            logger.exception("system_%s failed for user %s", action, user.id)
            text = f"❌ Ошибка при выполнении действия: {exc}"

        try:
            await query.edit_message_text(
                text=text,
                parse_mode="HTML",
                reply_markup=self.keyboard_builder.build_system_menu(include_cache_reset),
            )
        except Exception:
            logger.debug("Не удалось обновить сообщение системного меню", exc_info=True)

    async def _can_use_system(self, user_id: int, username: Optional[str]) -> bool:
        """Проверяет, доступно ли системное меню пользователю."""
        if self.permissions.is_supreme_admin(user_id, username):
            return True
        if self.permissions.is_dev_admin(user_id, username):
            return True
        role = await self.permissions.get_effective_role(user_id, username)
        return await self.permissions.check_permission(role, "debug")

    async def _collect_status(self) -> str:
        lines = ["⚙️ <b>Состояние системы</b>"]
        try:
            row = await self.db_manager.execute_with_retry(
                "SELECT VERSION() as ver", fetchone=True
            )
            version = row.get("ver") if row else "—"
            lines.append(f"✅ Подключение к БД активно (MySQL {version})")
        except Exception as exc:
            lines.append(f"❌ БД недоступна: {exc}")

        pool = getattr(self.db_manager, "pool", None)
        if pool:
            maxsize = getattr(pool, "maxsize", "?")
            minsize = getattr(pool, "minsize", "?")
            lines.append(f"ℹ️ Пул соединений: min={minsize}, max={maxsize}")
        else:
            lines.append("ℹ️ Пул соединений ещё не инициализирован.")

        return "\n".join(lines)

    async def _collect_recent_errors(self) -> str:
        log_path = Path("logs/app.log")
        if not log_path.exists():
            return "ℹ️ Лог-файл logs/app.log не найден."

        recent_errors = deque(maxlen=8)
        try:
            with log_path.open("r", encoding="utf-8", errors="ignore") as log_file:
                for line in log_file:
                    normalized = line.strip()
                    if not normalized:
                        continue
                    if "error" in normalized.lower():
                        recent_errors.append(normalized)
        except Exception as exc:
            logger.exception("Не удалось прочитать лог ошибок", exc_info=True)
            return f"❌ Не удалось прочитать logs/app.log: {exc}"

        if not recent_errors:
            return "✅ В логе нет ошибок за последнюю сессию."

        snippet = "\n".join(recent_errors)
        return f"❌ <b>Последние ошибки</b>:\n{snippet}"

    async def _run_integrity_checks(self) -> str:
        lines = ["🔌 <b>Проверка ключевых таблиц</b>"]
        tables = [
            "UsersTelegaBot",
            "roles_reference",
            "call_history",
            "call_scores",
        ]
        for table in tables:
            try:
                await self.db_manager.execute_with_retry(
                    f"SELECT 1 FROM {table} LIMIT 1", fetchone=True
                )
                lines.append(f"✅ {table}")
            except Exception as exc:
                lines.append(f"❌ {table}: {exc}")
        return "\n".join(lines)

    async def _run_sync(self) -> str:
        result = await self.analytics_service.sync_new()
        inserted = result.get("inserted", 0)
        errors = result.get("errors", 0)
        duration = float(result.get("duration") or 0.0)
        return (
            "🔄 <b>Синхронизация call_analytics</b>\n"
            f"Добавлено записей: {inserted}\n"
            f"Ошибок: {errors}\n"
            f"Длительность: {duration:.2f} c"
        )

    async def _clear_caches(self) -> str:
        self.roles_repo.clear_cache()
        self.permissions.clear_cache()
        clear_schema_cache()
        return "🗑️ Кэши ролей и схемы очищены."

    async def _log_system_action(self, user_id: int, action: str, text: str) -> None:
        try:
            await self.action_logger.log_action(
                actor_telegram_id=user_id,
                action="system_action",
                payload={"action": action, "result": text[:2000]},
            )
        except Exception:
            logger.debug("Не удалось записать system_action в лог", exc_info=True)


def register_system_handlers(
    application: Application,
    db_manager: DatabaseManager,
    permissions_manager: PermissionsManager,
) -> None:
    """Регистрирует обработчики для системного меню и кнопки помощи."""
    handler = SystemMenuHandler(db_manager, permissions_manager)
    application.add_handler(CommandHandler("system", handler.handle_system_command))
    application.add_handler(
        MessageHandler(
            filters.Regex(r"^⚙️ Система$"), handler.handle_system_command
        )
    )
    application.add_handler(
        CallbackQueryHandler(handler.handle_system_callback, pattern=r"^system_")
    )
    # Кнопка «ℹ️ Помощь» работает как /help
    application.add_handler(
        MessageHandler(
            filters.Regex(r"^ℹ️ Помощь$"),
            partial(help_command, permissions=permissions_manager),
        )
    )
