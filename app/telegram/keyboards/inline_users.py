"""Inline клавиатуры для управления пользователями."""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup


def build_users_management_menu(pending_count: int = 0) -> InlineKeyboardMarkup:
    pending_text = (
        f"⏳ Ожидают одобрения ({pending_count})"
        if pending_count > 0
        else "⏳ Ожидают одобрения"
    )
    keyboard = [
        [InlineKeyboardButton(pending_text, callback_data="users_pending")],
        [InlineKeyboardButton("📋 Список пользователей", callback_data="users_list")],
        [InlineKeyboardButton("👑 Список админов", callback_data="users_admins")],
        [InlineKeyboardButton("🔄 Изменить роль", callback_data="users_change_role")],
        [InlineKeyboardButton("◀️ Назад", callback_data="main_menu")],
    ]
    return InlineKeyboardMarkup(keyboard)


def build_approval_buttons(user_id: int) -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton("✅ Одобрить", callback_data=f"approve_{user_id}"),
            InlineKeyboardButton("❌ Отклонить", callback_data=f"decline_{user_id}"),
        ]
    ]
    return InlineKeyboardMarkup(keyboard)


def build_confirmation_buttons(action: str, target_id: int) -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton(
                "✅ Да, подтверждаю", callback_data=f"confirm_{action}_{target_id}"
            ),
            InlineKeyboardButton("❌ Отмена", callback_data="cancel"),
        ]
    ]
    return InlineKeyboardMarkup(keyboard)
