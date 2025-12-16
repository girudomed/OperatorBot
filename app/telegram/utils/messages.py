# Файл: app/telegram/utils/messages.py

"""
Утилиты для безопасной работы с callback-сообщениями.
"""

from typing import Any, Iterable

from telegram.error import BadRequest, TelegramError

from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)
MAX_MESSAGE_CHUNK = 3500


def _chunk_text(text: str, chunk_size: int = MAX_MESSAGE_CHUNK) -> Iterable[str]:
    for start in range(0, len(text), chunk_size):
        yield text[start:start + chunk_size]


async def safe_edit_message(query, **kwargs: Any) -> None:
    """
    Пытается отредактировать сообщение callback-а.
    Если Telegram возвращает ошибку (сообщение удалено/устарело),
    отправляет новое сообщение с теми же параметрами.
    """
    message = getattr(query, "message", None)
    text = kwargs.get("text")
    if text and len(text) > MAX_MESSAGE_CHUNK and message:
        logger.info(
            "Сообщение длиной %s превышает лимит, отправляем чанками",
            len(text),
        )
        reply_markup = kwargs.get("reply_markup")
        for index, chunk in enumerate(_chunk_text(text)):
            chunk_kwargs = dict(kwargs)
            chunk_kwargs["text"] = chunk
            chunk_kwargs.pop("parse_mode", None)
            if index > 0:
                chunk_kwargs.pop("reply_markup", None)
            try:
                await message.reply_text(**chunk_kwargs)
            except (BadRequest, TelegramError) as exc:
                logger.error("Не удалось отправить часть сообщения: %s", exc)
                break
        return
    try:
        await query.edit_message_text(**kwargs)
    except (BadRequest, TelegramError) as exc:
        logger.warning(
            "Не удалось отредактировать сообщение callback (%s), отправляем новое.",
            exc,
        )
        if message:
            await message.reply_text(**kwargs)
