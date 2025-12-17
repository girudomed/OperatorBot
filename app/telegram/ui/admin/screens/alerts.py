"""Экран алертов."""

from app.telegram.ui.admin import keyboards
from app.telegram.ui.admin.screens import Screen


def render_alerts_screen() -> Screen:
    text = (
        "🚨 <b>Алерты</b>\n"
        "Этот экран собирает срочные события: превышения SLA, падения интеграций, ошибки ETL.\n\n"
        "По каждой группе будет отдельное уведомление. Жмите на конкретные алерты, чтобы перейти к деталям."
    )
    return Screen(text=text, keyboard=keyboards.alerts_keyboard())

