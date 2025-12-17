"""Клавиатуры экранов админ-панели."""

from typing import List

from telegram import InlineKeyboardButton

from app.telegram.utils.callback_data import AdminCB


InlineKeyboard = List[List[InlineKeyboardButton]]


def dashboard_keyboard() -> InlineKeyboard:
    return [
        [
            InlineKeyboardButton("🔄 Обновить", callback_data=AdminCB.create(AdminCB.DASHBOARD)),
            InlineKeyboardButton(
                "📊 Детали", callback_data=AdminCB.create(AdminCB.DASHBOARD_DETAILS)
            ),
        ],
        [
            InlineKeyboardButton("🚨 Алерты", callback_data=AdminCB.create(AdminCB.ALERTS)),
            InlineKeyboardButton("⬇️ Экспорт", callback_data=AdminCB.create(AdminCB.EXPORT)),
        ],
        [
            InlineKeyboardButton(
                "👥 Пользователи",
                callback_data=AdminCB.create(AdminCB.USERS, AdminCB.LIST, AdminCB.STATUS_PENDING),
            ),
            InlineKeyboardButton(
                "⚠️ Опасные операции", callback_data=AdminCB.create(AdminCB.CRITICAL)
            ),
        ],
        [
            InlineKeyboardButton("◀️ Назад", callback_data=AdminCB.create(AdminCB.BACK)),
            InlineKeyboardButton("🏠 В админ-панель", callback_data=AdminCB.create(AdminCB.DASHBOARD)),
        ],
    ]


def dashboard_details_keyboard() -> InlineKeyboard:
    return [
        [
            InlineKeyboardButton(
                "🔄 Обновить детали",
                callback_data=AdminCB.create(AdminCB.DASHBOARD_DETAILS),
            ),
            InlineKeyboardButton("🏠 В дашборд", callback_data=AdminCB.create(AdminCB.DASHBOARD)),
        ],
        [InlineKeyboardButton("◀️ Назад", callback_data=AdminCB.create(AdminCB.BACK))],
    ]


def alerts_keyboard() -> InlineKeyboard:
    return [
        [
            InlineKeyboardButton(
                "📂 Расшифровки", callback_data=AdminCB.create(AdminCB.LOOKUP)
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
            InlineKeyboardButton(
                "🧠 AI-отчёт", callback_data=AdminCB.create(AdminCB.COMMAND, "report")
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
            InlineKeyboardButton(
                "📝 Weekly отчёт",
                callback_data=AdminCB.create(AdminCB.CRITICAL, "weekly_quality"),
            )
        ],
        [
            InlineKeyboardButton(
                "🧠 AI-отчёт", callback_data=AdminCB.create(AdminCB.CRITICAL, "report")
            )
        ],
        [
            InlineKeyboardButton(
                "📢 Техработы",
                callback_data=AdminCB.create(AdminCB.CRITICAL, "maintenance_alert"),
            )
        ],
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
            InlineKeyboardButton("◀️ Назад", callback_data=AdminCB.create(AdminCB.CRITICAL)),
            InlineKeyboardButton("🏠 В дашборд", callback_data=AdminCB.create(AdminCB.DASHBOARD)),
        ],
    ]


def main_menu_keyboard(allow_commands: bool) -> InlineKeyboard:
    keyboard: InlineKeyboard = [
        [
            InlineKeyboardButton("📊 Дашборд", callback_data=AdminCB.create(AdminCB.DASHBOARD)),
            InlineKeyboardButton(
                "👥 Пользователи",
                callback_data=AdminCB.create(AdminCB.USERS, AdminCB.LIST, AdminCB.STATUS_PENDING),
            ),
        ],
        [
            InlineKeyboardButton(
                "👑 Админы", callback_data=AdminCB.create(AdminCB.ADMINS, AdminCB.LIST)
            ),
            InlineKeyboardButton("⚙️ Настройки", callback_data=AdminCB.create(AdminCB.SETTINGS)),
        ],
        [
            InlineKeyboardButton("🧠 LM Метрики", callback_data=AdminCB.create(AdminCB.LM_MENU)),
            InlineKeyboardButton("📂 Расшифровки", callback_data=AdminCB.create(AdminCB.LOOKUP)),
        ],
    ]
    if allow_commands:
        keyboard.append(
            [InlineKeyboardButton("📑 Команды", callback_data=AdminCB.create(AdminCB.COMMANDS))]
        )
    return keyboard


def dashboard_error_keyboard() -> InlineKeyboard:
    return [
        [InlineKeyboardButton("◀️ Назад", callback_data=AdminCB.create(AdminCB.BACK))],
        [InlineKeyboardButton("🏠 В админ-панель", callback_data=AdminCB.create(AdminCB.DASHBOARD))],
    ]


def back_only_keyboard() -> InlineKeyboard:
    return [[InlineKeyboardButton("◀️ Назад", callback_data=AdminCB.create(AdminCB.BACK))]]
