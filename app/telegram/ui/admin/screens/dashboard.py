"""Экран дашборда админки."""

from typing import Dict

from app.telegram.ui.admin import keyboards
from app.telegram.ui.admin.screens import Screen


def render_dashboard_screen(counters: Dict[str, int], updated_at: str) -> Screen:
    total_users = counters.get("total_users", 0)
    pending = counters.get("pending_users", 0)
    approved = counters.get("approved_users", 0)
    blocked = counters.get("blocked_users", 0)
    admins = counters.get("admins", 0)
    regular_users = counters.get("non_admin_approved", max(0, approved - admins))

    text = (
        f"📊 <b>Live dashboard</b>\n"
        f"Обновлено (МСК): <b>{updated_at}</b>\n\n"
        f"👥 Всего пользователей: <b>{total_users}</b>\n"
        f"⏳ Pending: <b>{pending}</b>\n"
        f"✅ Approved: <b>{approved}</b>\n"
        f"🚫 Заблокировано: <b>{blocked}</b>\n"
        f"👑 Администраторов: <b>{admins}</b>\n"
        f"👥 Пользователей (без админов): <b>{regular_users}</b>\n\n"
        "Для расширенной аналитики откройте «📈 Статистика системы»."
    )
    return Screen(text=text, keyboard=keyboards.dashboard_keyboard())
