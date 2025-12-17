"""Экраны для повышения пользователей."""

from typing import List

from telegram import InlineKeyboardButton

from app.telegram.ui.admin.screens import Screen
from app.telegram.utils.callback_data import AdminCB


ROLE_TITLES = {
    "admin": "администратора",
    "superadmin": "супер-админа",
}


def render_promotion_menu_screen() -> Screen:
    text = (
        "⬆️ <b>Повышения</b>\n"
        "Выберите цель: кого назначаем и на какую роль."
    )
    keyboard = [
        [
            InlineKeyboardButton(
                "👑 Назначить админа",
                callback_data=AdminCB.create(AdminCB.PROMOTION, AdminCB.LIST, "admin"),
            )
        ],
        [
            InlineKeyboardButton(
                "⭐ Назначить супер-админа",
                callback_data=AdminCB.create(AdminCB.PROMOTION, AdminCB.LIST, "superadmin"),
            )
        ],
        [
            InlineKeyboardButton("◀️ Назад", callback_data=AdminCB.create(AdminCB.BACK))
        ],
    ]
    return Screen(text=text, keyboard=keyboard)


def render_promotion_list_screen(users: List[dict], role_slug: str) -> Screen:
    role_label = ROLE_TITLES.get(role_slug, role_slug)
    text = (
        f"⬆️ <b>Кандидаты на роль {role_label}</b>\n"
        "Выберите пользователя, чтобы подтвердить повышение."
    )
    keyboard: list[list[InlineKeyboardButton]] = []
    for user in users:
        keyboard.append(
            [
                InlineKeyboardButton(
                    _user_label(user),
                    callback_data=AdminCB.create(
                        AdminCB.PROMOTION,
                        AdminCB.DETAILS,
                        role_slug,
                        user.get("id"),
                    ),
                )
            ]
        )
    keyboard.append(
        [
            InlineKeyboardButton(
                "🔄 Обновить",
                callback_data=AdminCB.create(AdminCB.PROMOTION, AdminCB.LIST, role_slug),
            )
        ]
    )
    keyboard.append(
        [
            InlineKeyboardButton(
                "◀️ Назад", callback_data=AdminCB.create(AdminCB.PROMOTION, "menu")
            )
        ]
    )
    return Screen(text=text, keyboard=keyboard)


def render_empty_promotion_screen(role_slug: str) -> Screen:
    role_label = ROLE_TITLES.get(role_slug, role_slug)
    text = f"✅ Нет кандидатов для назначения на роль {role_label}."
    keyboard = [
        [
            InlineKeyboardButton(
                "🔄 Обновить",
                callback_data=AdminCB.create(AdminCB.PROMOTION, AdminCB.LIST, role_slug),
            )
        ],
        [InlineKeyboardButton("◀️ Назад", callback_data=AdminCB.create(AdminCB.PROMOTION, "menu"))],
    ]
    return Screen(text=text, keyboard=keyboard)


def render_promotion_detail_screen(user: dict, role_slug: str) -> Screen:
    role_label = ROLE_TITLES.get(role_slug, role_slug)
    text = (
        f"⬆️ <b>Подтверждение повышения до {role_label}</b>\n"
        f"Пользователь: <b>{user.get('full_name') or user.get('username') or user.get('id')}</b>\n"
        f"Username: @{user.get('username') or '—'}\n"
        f"Роль сейчас: {user.get('role', {}).get('name') or '—'}\n"
        "\nПодтвердить повышение?"
    )
    telegram_id = user.get("telegram_id") or user.get("user_id") or 0
    keyboard = [
        [
            InlineKeyboardButton(
                "✅ Повысить",
                callback_data=AdminCB.create(
                    AdminCB.PROMOTION,
                    AdminCB.APPROVE,
                    role_slug,
                    telegram_id,
                    user.get("id"),
                ),
            )
        ],
        [
            InlineKeyboardButton(
                "◀️ Назад",
                callback_data=AdminCB.create(AdminCB.PROMOTION, AdminCB.LIST, role_slug),
            )
        ],
    ]
    return Screen(text=text, keyboard=keyboard)


def _user_label(user: dict) -> str:
    base = user.get("full_name") or user.get("username") or f"#{user.get('id')}"
    ext = f" @{user['username']}" if user.get("username") else ""
    return f"{base}{ext}"[:50]

