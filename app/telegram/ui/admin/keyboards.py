"""Клавиатуры экранов админ-панели."""

from typing import List

from telegram import InlineKeyboardButton

from app.telegram.utils.callback_data import AdminCB


InlineKeyboard = List[List[InlineKeyboardButton]]


def dashboard_keyboard() -> InlineKeyboard:
    return [
        [
            InlineKeyboardButton("🔄 Обновить", callback_data=AdminCB.create(AdminCB.DASHBOARD)),
        ],
        [
            InlineKeyboardButton("🚨 Алерты", callback_data=AdminCB.create(AdminCB.ALERTS)),
            InlineKeyboardButton("⬇️ Экспорт", callback_data=AdminCB.create(AdminCB.EXPORT)),
        ],
        [
            InlineKeyboardButton(
                "📝 Еженедельный отчёт",
                callback_data=AdminCB.create(AdminCB.COMMAND, "weekly_quality"),
            ),
        ],
        [
            InlineKeyboardButton("◀️ Назад", callback_data=AdminCB.create(AdminCB.BACK)),
            InlineKeyboardButton("🏠 В админ-панель", callback_data=AdminCB.create(AdminCB.DASHBOARD)),
        ],
    ]


def alerts_keyboard() -> InlineKeyboard:
    return [
        [
            InlineKeyboardButton(
                "🔍 Поиск звонков",
                callback_data=AdminCB.create(AdminCB.CALL_LOOKUP, "intro"),
            ),
            InlineKeyboardButton(
                "👥 Пользователи",
                callback_data=AdminCB.create(AdminCB.USERS, AdminCB.LIST, AdminCB.STATUS_PENDING),
            ),
        ],
        [
            InlineKeyboardButton("◀️ Назад", callback_data=AdminCB.create(AdminCB.BACK)),
            InlineKeyboardButton("🏠 В дашборд", callback_data=AdminCB.create(AdminCB.DASHBOARD)),
        ],
    ]


def export_keyboard() -> InlineKeyboard:
    return [
        [
            InlineKeyboardButton(
                "📝 Weekly CSV",
                callback_data=AdminCB.create(AdminCB.COMMAND, "weekly_quality"),
            ),
        ],
        [
            InlineKeyboardButton("◀️ Назад", callback_data=AdminCB.create(AdminCB.BACK)),
            InlineKeyboardButton("🏠 В дашборд", callback_data=AdminCB.create(AdminCB.DASHBOARD)),
        ],
    ]


def dangerous_ops_keyboard() -> InlineKeyboard:
    return [
        [
            InlineKeyboardButton("◀️ Назад", callback_data=AdminCB.create(AdminCB.BACK)),
            InlineKeyboardButton("🏠 В дашборд", callback_data=AdminCB.create(AdminCB.DASHBOARD)),
        ],
    ]


def critical_confirm_keyboard(action: str) -> InlineKeyboard:
    return [
        [
            InlineKeyboardButton(
                "✅ Подтвердить запуск",
                callback_data=AdminCB.create(AdminCB.COMMAND, action),
            )
        ],
        [
            InlineKeyboardButton("◀️ Назад", callback_data=AdminCB.create(AdminCB.SYSTEM)),
            InlineKeyboardButton("🏠 В дашборд", callback_data=AdminCB.create(AdminCB.DASHBOARD)),
        ],
    ]


def main_menu_keyboard(
    *,
    allow_commands: bool,
    allow_yandex_tools: bool,
) -> InlineKeyboard:
    keyboard: InlineKeyboard = [
        [
            InlineKeyboardButton(
                "👥 Пользователи",
                callback_data=AdminCB.create(AdminCB.USERS, AdminCB.LIST, AdminCB.STATUS_PENDING),
            ),
        ],
        [
            InlineKeyboardButton(
                "🧠 AI-отчёт", callback_data=AdminCB.create(AdminCB.REPORTS, "period_menu")
            ),
            InlineKeyboardButton(
                "🧠 LM Метрики", callback_data=AdminCB.create(AdminCB.LM_MENU)
            ),
        ],
        [
            InlineKeyboardButton(
                "🔍 Поиск звонков",
                callback_data=AdminCB.create(AdminCB.CALL_LOOKUP, "intro"),
            ),
            InlineKeyboardButton(
                "📈 Live-Dashboard",
                callback_data=AdminCB.create(AdminCB.STATS),
            ),
        ],
        [
            InlineKeyboardButton(
                "ℹ️ Помощь",
                callback_data=AdminCB.create(AdminCB.HELP_SCREEN),
            ),
            InlineKeyboardButton(
                "📘 Мануал",
                callback_data=AdminCB.create(AdminCB.MANUAL),
            ),
        ],
        [
            InlineKeyboardButton(
                "⚙️ Система",
                callback_data=AdminCB.create(AdminCB.SYSTEM),
            )
        ],
    ]
    # Всегда добавляем явную кнопку "◀️ Назад" в админских inline-меню
    # чтобы гарантировать единообразную навигацию назад через AdminCB.BACK.
    keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data=AdminCB.create(AdminCB.BACK))])
    return keyboard


def dashboard_error_keyboard() -> InlineKeyboard:
    return [
        [InlineKeyboardButton("◀️ Назад", callback_data=AdminCB.create(AdminCB.BACK))],
        [InlineKeyboardButton("🏠 В админ-панель", callback_data=AdminCB.create(AdminCB.DASHBOARD))],
    ]


def back_only_keyboard() -> InlineKeyboard:
    return [[InlineKeyboardButton("◀️ Назад", callback_data=AdminCB.create(AdminCB.BACK))]]
