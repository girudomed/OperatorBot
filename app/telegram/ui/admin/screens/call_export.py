"""Экран выбора диапазона для выгрузки звонков."""

from app.telegram.ui.admin import keyboards
from app.telegram.ui.admin.screens import Screen


def render_call_export_screen() -> Screen:
    text = (
        "⬇️ <b>Выгрузка звонков</b>\n"
        "Выберите, за какой период собрать .xlsx с расшифровками.\n"
        "Файл придёт отдельным сообщением."
    )
    return Screen(text=text, keyboard=keyboards.call_export_keyboard())

