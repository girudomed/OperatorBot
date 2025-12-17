"""Inline клавиатуры системного меню."""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup


def build_system_menu(include_cache_reset: bool = False) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("🔍 Состояние бота", callback_data="system_status")],
        [InlineKeyboardButton("❌ Последние ошибки", callback_data="system_errors")],
        [InlineKeyboardButton("🔌 Проверка БД/Mango", callback_data="system_check")],
        [InlineKeyboardButton("🔄 Синхронизация аналитики", callback_data="system_sync")],
    ]
    if include_cache_reset:
        keyboard.append(
            [InlineKeyboardButton("🗑️ Очистить кеш", callback_data="system_clear_cache")]
        )
    keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="main_menu")])
    return InlineKeyboardMarkup(keyboard)


def build_back_button(callback_data: str = "main_menu") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("◀️ Назад", callback_data=callback_data)],
        ]
    )
