# Файл: app/telegram/utils/messages.py

"""
Утилиты для безопасной работы с callback-сообщениями.
"""

from typing import Any

from telegram.error import BadRequest, TelegramError

from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)


async def safe_edit_message(query, **kwargs: Any) -> None:
    """
    Пытается отредактировать сообщение callback-а.
    Если Telegram возвращает ошибку (сообщение удалено/устарело),
    отправляет новое сообщение с теми же параметрами.
    """
    message = getattr(query, "message", None)
    try:
        await query.edit_message_text(**kwargs)
    except (BadRequest, TelegramError) as exc:
        logger.warning(
            "Не удалось отредактировать сообщение callback (%s), отправляем новое.",
            exc,
        )
        if message:
            await message.reply_text(**kwargs)
