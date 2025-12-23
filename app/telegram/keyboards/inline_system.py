"""Inline клавиатуры системного меню."""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from app.telegram.utils.callback_data import AdminCB


def build_system_menu(
    include_cache_reset: bool = False,
    back_callback: str = "system_back",
) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("🔍 Состояние бота", callback_data="system_status")],
        [InlineKeyboardButton("❌ Последние ошибки", callback_data="system_errors")],
        [InlineKeyboardButton("📄 Логи", callback_data="system_logs")],
        [InlineKeyboardButton("🔌 Проверка БД", callback_data="system_check")],
    ]
    if include_cache_reset:
        keyboard.append(
            [InlineKeyboardButton("🗑️ Очистить кеш", callback_data="system_clear_cache")]
        )
    keyboard.append(
        [
            InlineKeyboardButton(
                "📢 Техработы",
                callback_data=AdminCB.create(AdminCB.CRITICAL, "maintenance_alert"),
            ),
        ]
    )
    keyboard.append(
        [
            InlineKeyboardButton("◀️ Назад", callback_data=back_callback)
        ]
    )
    return InlineKeyboardMarkup(keyboard)
    return InlineKeyboardMarkup(keyboard)


def build_back_button(callback_data: str = "system_back") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("◀️ Назад", callback_data=callback_data)],
        ]
    )
