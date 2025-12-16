# Файл: app/telegram/utils/messages.py

"""Утилиты для безопасной работы с callback-сообщениями."""

from io import BytesIO
from typing import Any

from telegram.error import BadRequest, TelegramError

from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)
MAX_MESSAGE_CHUNK = 3500


async def _send_text_document(message, text: str, *, filename: str = "message.txt") -> None:
    """Отправляет длинный текст как txt-документ."""
    buffer = BytesIO(text.encode("utf-8"))
    buffer.name = filename
    try:
        await message.reply_document(
            document=buffer,
            caption="Полный текст во вложении.",
        )
    except (BadRequest, TelegramError) as exc:
        logger.error("Не удалось отправить документ с текстом: %s", exc)


async def _send_chunked_message(message, **kwargs: Any) -> None:
    """Отправляет длинное сообщение кусками, чтобы избежать Message_too_long."""
    text = kwargs.get("text") or ""
    reply_markup = kwargs.get("reply_markup")
    parse_mode = kwargs.get("parse_mode")
    if not text:
        await message.reply_text(**kwargs)
        return

    chunks = list(_chunk_text(text)) or [""]
    for index, chunk in enumerate(chunks):
        chunk_kwargs = dict(kwargs)
        chunk_kwargs["text"] = chunk
        if index > 0:
            chunk_kwargs.pop("reply_markup", None)
            chunk_kwargs.pop("parse_mode", None)
        try:
            await message.reply_text(**chunk_kwargs)
        except (BadRequest, TelegramError) as exc:
            logger.error("Не удалось отправить часть сообщения: %s", exc)
            break


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
            "Сообщение длиной %s превышает лимит, отправляем документом",
            len(text),
        )
        await _send_text_document(message, text)
        return
    try:
        await query.edit_message_text(**kwargs)
    except (BadRequest, TelegramError) as exc:
        logger.warning(
            "Не удалось отредактировать сообщение callback (%s), отправляем новое.",
            exc,
        )
        if message:
            text = kwargs.get("text") or ""
            if len(text) > MAX_MESSAGE_CHUNK:
                await _send_text_document(message, text)
            else:
                await message.reply_text(**kwargs)
