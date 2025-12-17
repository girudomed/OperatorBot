"""Экран экспорта."""

from app.telegram.ui.admin import keyboards
from app.telegram.ui.admin.screens import Screen


def render_export_screen() -> Screen:
    text = (
        "⬇️ <b>Экспорт</b>\n"
        "Тут собираем подготовленные выгрузки: CSV по пользователям, отчёты с метриками, свежие LM-статы.\n\n"
        "После запуска экспорта бот пришлёт файл отдельным сообщением."
    )
    return Screen(text=text, keyboard=keyboards.export_keyboard())

