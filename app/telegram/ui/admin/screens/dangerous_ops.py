"""Экран опасных операций."""

from app.telegram.ui.admin import keyboards
from app.telegram.ui.admin.screens import Screen


def render_dangerous_ops_screen() -> Screen:
    text = (
        "⚠️ <b>Опасные операции</b>\n"
        "Здесь действия, которые грузят базу или запускают массовые рассылки."
        " Каждое требует явного подтверждения."
    )
    return Screen(text=text, keyboard=keyboards.dangerous_ops_keyboard())


def render_critical_confirmation(action_key: str, description: str) -> Screen:
    text = (
        f"⚠️ <b>Подтверждение</b>\n"
        f"Операция: <b>{action_key}</b>\n"
        f"{description}\n\n"
        "После подтверждения действие запускается сразу и идёт в лог Watchdog."
    )
    return Screen(text=text, keyboard=keyboards.critical_confirm_keyboard(action_key))
