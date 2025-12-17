"""Inline клавиатуры для отчётов."""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup


def build_reports_menu(can_view_all: bool) -> InlineKeyboardMarkup:
    keyboard = []
    if can_view_all:
        keyboard.extend(
            [
                [InlineKeyboardButton("📅 Еженедельный отчёт", callback_data="reports_weekly")],
                [InlineKeyboardButton("📆 Отчёт за период", callback_data="reports_period")],
                [InlineKeyboardButton("👤 Отчёт по оператору", callback_data="reports_operator")],
                [InlineKeyboardButton("📊 Сводка по всем", callback_data="reports_all")],
            ]
        )
    else:
        keyboard.extend(
            [
                [InlineKeyboardButton("📅 Мой отчёт за неделю", callback_data="reports_my_week")],
                [InlineKeyboardButton("📆 Мой отчёт за период", callback_data="reports_my_period")],
            ]
        )

    keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="main_menu")])
    return InlineKeyboardMarkup(keyboard)


def build_call_lookup_menu() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("📞 По номеру телефона", callback_data="lookup_phone")],
        [InlineKeyboardButton("📅 По дате/интервалу", callback_data="lookup_date")],
        [InlineKeyboardButton("👤 По оператору", callback_data="lookup_operator")],
        [InlineKeyboardButton("🕐 Последние 10 звонков", callback_data="lookup_recent")],
        [InlineKeyboardButton("◀️ Назад", callback_data="main_menu")],
    ]
    return InlineKeyboardMarkup(keyboard)
