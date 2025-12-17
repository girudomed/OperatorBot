"""Экран дашборда админки."""

from typing import Dict

from app.telegram.ui.admin import keyboards
from app.telegram.ui.admin.screens import Screen
from app.telegram.ui.admin.constants import ROLE_DISPLAY_ORDER, ROLE_EMOJI
from app.core.roles import role_display_name_from_name


def render_dashboard_screen(counters: Dict[str, int], updated_at: str) -> Screen:
    total_users = counters.get("total_users", 0)
    pending = counters.get("pending_users", 0)
    approved = counters.get("approved_users", 0)
    blocked = counters.get("blocked_users", 0)
    admins = counters.get("admins", 0)
    regular_users = counters.get(
        "non_admin_approved", max(0, approved - admins)
    )

    text = (
        f"📊 <b>Live dashboard</b>\n"
        f"Обновлено: <b>{updated_at}</b>\n\n"
        f"👥 Всего пользователей: <b>{total_users}</b>\n"
        f"⏳ Pending: <b>{pending}</b>\n"
        f"✅ Approved: <b>{approved}</b>\n"
        f"🚫 Заблокировано: <b>{blocked}</b>\n"
        f"👑 Администраторов: <b>{admins}</b>\n"
        f"👥 Пользователей (без админов): <b>{regular_users}</b>\n\n"
        "Короткий обзор. Детали — отдельным экраном."
    )
    return Screen(text=text, keyboard=keyboards.dashboard_keyboard())


def render_dashboard_details_screen(counters: Dict[str, int], updated_at: str) -> Screen:
    per_status = (
        f"⏳ Pending: <b>{counters.get('pending_users', 0)}</b>\n"
        f"✅ Approved: <b>{counters.get('approved_users', 0)}</b>\n"
        f"🚫 Заблокировано: <b>{counters.get('blocked_users', 0)}</b>\n"
    )
    roles_summary = _build_roles_summary(counters)
    text = (
        f"📊 <b>Детали дашборда</b>\n"
        f"Обновлено: <b>{updated_at}</b>\n\n"
        f"Статусы:\n{per_status}\n"
        f"Роли (approved):\n{roles_summary}\n\n"
        "Роли и статусы вынесены сюда, чтобы не перегружать основной экран."
    )
    return Screen(text=text, keyboard=keyboards.dashboard_details_keyboard())


def _build_roles_summary(counters) -> str:
    breakdown = counters.get("roles_breakdown") or {}
    lines = []
    for role in ROLE_DISPLAY_ORDER:
        stats = breakdown.get(role, {})
        emoji = ROLE_EMOJI.get(role, "•")
        display_name = role_display_name_from_name(role) or stats.get("display") or role.title()
        approved = int(stats.get("approved") or 0)
        lines.append(f"{emoji} {display_name}: <b>{approved}</b>")
    for role, stats in breakdown.items():
        if role in ROLE_DISPLAY_ORDER:
            continue
        display_name = role_display_name_from_name(role) or stats.get("display") or role.title()
        emoji = ROLE_EMOJI.get(role, "•")
        approved = int(stats.get("approved") or 0)
        lines.append(f"{emoji} {display_name}: <b>{approved}</b>")
    return "\n".join(lines) if lines else "—"
