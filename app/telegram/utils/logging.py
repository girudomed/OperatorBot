"""
Утилиты для человекочитаемого отображения пользователей в логах.
"""

from typing import Optional

from telegram import User


def describe_user(user: Optional[User]) -> str:
    """
    Возвращает краткое описание пользователя для логов.
    """
    if not user:
        return "user=<unknown>"
    username = f"@{user.username}" if user.username else "no_username"
    full_name = user.full_name or "no_name"
    return f"user_id={user.id} {username} {full_name}"
