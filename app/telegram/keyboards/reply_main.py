"""Reply клавиатура главного меню."""

from __future__ import annotations

from typing import Dict, Optional

from telegram import KeyboardButton, ReplyKeyboardMarkup

from app.db.repositories.roles import RolesRepository
from app.logging_config import get_watchdog_logger

from .exceptions import KeyboardPermissionsError

logger = get_watchdog_logger(__name__)


class ReplyMainKeyboardBuilder:
    """Строит главную клавиатуру на основе прав роли."""

    def __init__(self, roles_repo: RolesRepository):
        self.roles_repo = roles_repo

    async def build_main_keyboard(
        self,
        role_id: int,
        perms_override: Optional[Dict[str, bool]] = None,
    ) -> ReplyKeyboardMarkup:
        logger.debug("[KEYBOARD] Building main keyboard for role_id=%s", role_id)
        perms = await self._resolve_permissions(role_id, perms_override)

        keyboard = []

        if perms.get("can_view_own_stats"):
            keyboard.append([KeyboardButton("📊 Моя статистика")])

        if perms.get("can_view_all_stats"):
            keyboard.append(
                [
                    KeyboardButton("📊 Отчёты"),
                    KeyboardButton("🔍 Поиск звонка"),
                ]
            )

        if perms.get("can_manage_users"):
            keyboard.append([KeyboardButton("👥 Пользователи и роли")])

        if perms.get("can_manage_users"):
            keyboard.append([KeyboardButton("👑 Админ-панель")])

        if perms.get("can_debug"):
            keyboard.append([KeyboardButton("⚙️ Система")])

        keyboard.append([KeyboardButton("ℹ️ Помощь")])
        keyboard.append([KeyboardButton("📘 Мануал")])

        reply_keyboard = ReplyKeyboardMarkup(
            keyboard,
            resize_keyboard=True,
            one_time_keyboard=False,
        )
        logger.info(
            "Reply keyboard built for role_id=%s: %s",
            role_id,
            _keyboard_texts(keyboard),
        )
        return reply_keyboard

    async def _resolve_permissions(
        self, role_id: int, perms_override: Optional[Dict[str, bool]]
    ) -> Dict[str, bool]:
        if perms_override is not None:
            return perms_override
        try:
            return await self.roles_repo.get_user_permissions(role_id)
        except Exception as exc:  # pragma: no cover - защитный путь
            logger.exception("Не удалось получить права роли %s", role_id)
            raise KeyboardPermissionsError(self._minimal_keyboard()) from exc

    @staticmethod
    def _minimal_keyboard() -> ReplyKeyboardMarkup:
        layout = [[KeyboardButton("ℹ️ Помощь")]]
        return ReplyKeyboardMarkup(layout, resize_keyboard=True, one_time_keyboard=False)


def _keyboard_texts(keyboard_layout):
    return [[btn.text for btn in row] for row in keyboard_layout]
