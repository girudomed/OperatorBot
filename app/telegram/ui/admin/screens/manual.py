"""Экран обучения (manual) для админ-панели."""

from app.telegram.ui.admin import keyboards
from app.telegram.ui.admin.screens import Screen
from app.telegram.handlers.manual import MANUAL_URL


def render_manual_screen(
    *,
    allow_video_upload: bool,
    allow_video_delete: bool,
    video_status: str | None,
) -> Screen:
    text = (
        "📘 <b>Обучение</b>\n\n"
        f"Материал: {MANUAL_URL}\n"
    )
    return Screen(
        text=text,
        keyboard=keyboards.manual_keyboard(
            allow_video_upload=allow_video_upload,
            allow_video_delete=allow_video_delete,
        ),
    )
