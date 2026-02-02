"""Главное меню админ-панели."""

from app.telegram.ui.admin import keyboards
from app.telegram.ui.admin.screens import Screen


def render_main_menu_screen(
    allow_commands: bool,
    allow_yandex_tools: bool,
    allow_video_upload: bool,
    allow_video_delete: bool,
    video_status: str | None = None,
) -> Screen:
    text = (
        "🏠 <b>Админ-панель</b>\n"
        "Дашборд — главный экран. Остальные разделы вынесены отдельными экранами.\n\n"
        "Выберите следующий шаг:"
    )
    if video_status:
        text += f"\n\n🎬 Видео обучения: {video_status}"
    return Screen(
        text=text,
        keyboard=keyboards.main_menu_keyboard(
            allow_commands=allow_commands,
            allow_yandex_tools=allow_yandex_tools,
            allow_video_upload=allow_video_upload,
            allow_video_delete=allow_video_delete,
        ),
    )
