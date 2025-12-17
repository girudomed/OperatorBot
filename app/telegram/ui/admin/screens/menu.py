"""Главное меню админ-панели."""

from app.telegram.ui.admin import keyboards
from app.telegram.ui.admin.screens import Screen


def render_main_menu_screen(allow_commands: bool) -> Screen:
    text = (
        "🏠 <b>Админ-панель</b>\n"
        "Дашборд — главный экран. Остальные разделы вынесены отдельными экранами.\n\n"
        "Выберите следующий шаг:"
    )
    return Screen(text=text, keyboard=keyboards.main_menu_keyboard(allow_commands))

