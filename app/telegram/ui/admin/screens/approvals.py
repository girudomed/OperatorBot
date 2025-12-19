"""Экраны для обработки заявок (approve/decline)."""

from typing import List

from telegram import InlineKeyboardButton

from app.telegram.ui.admin.screens import Screen
from app.telegram.utils.callback_data import AdminCB


def render_approvals_list_screen(
    users: List[dict],
    page: int,
    total_pages: int,
) -> Screen:
    text = (
        "⏳ <b>Заявки на утверждение</b>\n"
        f"Страница {page + 1} из {max(total_pages, 1)}.\n\n"
        "Выберите пользователя, чтобы открыть карточку."
    )
    keyboard: list[list[InlineKeyboardButton]] = []
    for user in users:
        label = _user_label(user)
        keyboard.append(
            [
                InlineKeyboardButton(
                    label,
                    callback_data=AdminCB.create(
                        AdminCB.APPROVALS,
                        AdminCB.DETAILS,
                        page,
                        user.get("id"),
                    ),
                )
            ]
        )
    nav_row: list[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(
            InlineKeyboardButton(
                "⬅️",
                callback_data=AdminCB.create(AdminCB.APPROVALS, AdminCB.LIST, page - 1),
            )
        )
    if page + 1 < total_pages:
        nav_row.append(
            InlineKeyboardButton(
                "➡️",
                callback_data=AdminCB.create(AdminCB.APPROVALS, AdminCB.LIST, page + 1),
            )
        )
    if nav_row:
        keyboard.append(nav_row)
    keyboard.append(
        [
            InlineKeyboardButton(
                "🔄 Обновить", callback_data=AdminCB.create(AdminCB.APPROVALS, AdminCB.LIST, page)
            )
        ]
    )
    keyboard.append(
        [
            InlineKeyboardButton(
                "◀️ Назад", callback_data=AdminCB.create(AdminCB.APPROVALS, AdminCB.BACK)
            )
        ]
    )
    return Screen(text=text, keyboard=keyboard)


def render_empty_approvals_screen() -> Screen:
    text = (
        "✅ <b>Заявок нет</b>\n"
        "Все pending-пользователи уже обработаны."
    )
    keyboard = [
        [InlineKeyboardButton("🔄 Проверить снова", callback_data=AdminCB.create(AdminCB.APPROVALS, AdminCB.LIST, 0))],
        [InlineKeyboardButton("◀️ Назад", callback_data=AdminCB.create(AdminCB.APPROVALS, AdminCB.BACK))],
    ]
    return Screen(text=text, keyboard=keyboard)


def render_approval_detail_screen(user: dict, page: int) -> Screen:
    text = (
        "👤 <b>Карточка пользователя</b>\n"
        f"ID: <b>{user.get('id')}</b>\n"
        f"Имя: {user.get('full_name') or '—'}\n"
        f"Username: @{user.get('username') or '—'}\n"
        f"Telegram ID: {user.get('telegram_id') or '—'}\n"
        f"Статус: <b>{user.get('status')}</b>\n"
        f"Роль: {user.get('role', {}).get('name') or user.get('role_id') or '—'}\n"
        "\nДоступные действия:"
    )
    telegram_id = user.get("telegram_id") or user.get("user_id") or 0
    keyboard = [
        [
            InlineKeyboardButton(
                "✅ Утвердить",
                callback_data=AdminCB.create(
                    AdminCB.APPROVALS,
                    AdminCB.APPROVE,
                    user.get("id"),
                    telegram_id,
                    page,
                ),
            ),
            InlineKeyboardButton(
                "🗑️ Отклонить",
                callback_data=AdminCB.create(
                    AdminCB.APPROVALS,
                    AdminCB.DECLINE,
                    telegram_id,
                    page,
                ),
            ),
        ],
        [
            InlineKeyboardButton(
                "◀️ К списку",
                callback_data=AdminCB.create(AdminCB.APPROVALS, AdminCB.LIST, page),
            )
        ],
    ]
    return Screen(text=text, keyboard=keyboard)


def _user_label(user: dict) -> str:
    base = user.get("full_name") or user.get("username") or f"#{user.get('id')}"
    ext = f" · {user.get('extension')}" if user.get("extension") else ""
    return f"{base}{ext}"
