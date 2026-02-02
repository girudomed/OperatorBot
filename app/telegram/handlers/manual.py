# Файл: app/telegram/handlers/manual.py

"""Кнопка «Обучение» и одноимённая команда."""

import json
from datetime import datetime
from pathlib import Path

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

from app.config import DEV_ADMIN_ID
from app.logging_config import get_watchdog_logger
from app.telegram.utils.logging import describe_user
from app.telegram.utils.state import MANUAL_VIDEO_KEY

MANUAL_URL = "https://docs.google.com/document/d/1g2cpa4Pzv6NhZ7hL6bLvo26TF0--KxWlqVnoxvDvpss/edit?usp=sharing"
MANUAL_TEXT = "Обучение:\n" + MANUAL_URL
MANUAL_VIDEO_PATH = Path(__file__).resolve().parents[3] / "config" / "manual_video.json"
logger = get_watchdog_logger(__name__)


def _load_video_file_id() -> str | None:
    try:
        if not MANUAL_VIDEO_PATH.exists():
            return None
        payload = json.loads(MANUAL_VIDEO_PATH.read_text(encoding="utf-8"))
        return payload.get("file_id")
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        logger.warning("Не удалось прочитать manual video file id: %s", exc)
        return None
    except Exception:
        logger.exception("Не удалось прочитать manual video file id")
        raise


def _save_video_file_id(file_id: str) -> None:
    try:
        MANUAL_VIDEO_PATH.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "file_id": file_id,
            "updated_at": datetime.utcnow().isoformat(),
        }
        MANUAL_VIDEO_PATH.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    except (OSError, TypeError, ValueError) as exc:
        logger.warning("Не удалось сохранить manual video file id: %s", exc)
        raise
    except Exception:
        logger.exception("Не удалось сохранить manual video file id")
        raise


def _delete_video_file_id() -> bool:
    try:
        if MANUAL_VIDEO_PATH.exists():
            MANUAL_VIDEO_PATH.unlink()
        return True
    except OSError as exc:
        logger.warning("Не удалось удалить manual video file id: %s", exc)
        return False
    except Exception:
        logger.exception("Не удалось удалить manual video file id")
        raise


async def _send_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user = update.effective_user
    if not message:
        return
    logger.info("Manual запрошен пользователем %s", describe_user(user))
    await message.reply_text(MANUAL_TEXT)
    try:
        video_id = _load_video_file_id()
    except Exception:
        video_id = None
    if video_id:
        thread_id = getattr(message, "message_thread_id", None)
        await context.bot.send_video(
            chat_id=message.chat_id,
            video=video_id,
            message_thread_id=thread_id,
        )


async def _start_video_upload(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or user.id != DEV_ADMIN_ID:
        target = update.effective_message
        if target:
            await target.reply_text("Недостаточно прав.")
        elif update.callback_query:
            await update.callback_query.answer("Недостаточно прав.", show_alert=True)
        return
    context.user_data[MANUAL_VIDEO_KEY] = True
    target = update.effective_message
    if target:
        await target.reply_text("Отправьте видео для обучения (одним сообщением).")
    elif update.callback_query:
        await update.callback_query.answer("Отправьте видео для обучения.", show_alert=True)


async def _handle_video_upload(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    message = update.effective_message
    if not message or not user:
        return
    if not context.user_data.get(MANUAL_VIDEO_KEY):
        return
    context.user_data.pop(MANUAL_VIDEO_KEY, None)
    if user.id != DEV_ADMIN_ID:
        await message.reply_text("Недостаточно прав.")
        return
    if not message.video:
        await message.reply_text("Нужно прислать видео.")
        return
    file_id = message.video.file_id
    try:
        _save_video_file_id(file_id)
    except Exception:
        await message.reply_text("Не удалось сохранить видео.")
        return
    await message.reply_text("Видео сохранено. Теперь будет отправляться вместе с обучением.")


async def _delete_video(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    message = update.effective_message
    if not user or user.id != DEV_ADMIN_ID:
        if message:
            await message.reply_text("Недостаточно прав.")
        elif update.callback_query:
            await update.callback_query.answer("Недостаточно прав.", show_alert=True)
        return
    try:
        ok = _delete_video_file_id()
    except Exception:
        ok = False
    if message:
        await message.reply_text("Видео удалено." if ok else "Не удалось удалить видео.")
    elif update.callback_query:
        await update.callback_query.answer("Видео удалено." if ok else "Не удалось удалить видео.", show_alert=True)


def register_manual_handlers(application: Application) -> None:
    application.add_handler(CommandHandler("manual", _send_manual))
    manual_button_handler = MessageHandler(
        filters.Regex(r"(?i)^\s*(?:📘\s*)?(?:мануал|обучение)\s*$"),
        _send_manual,
    )
    manual_button_handler.block = False
    application.add_handler(manual_button_handler, group=0)
    video_handler = MessageHandler(filters.VIDEO, _handle_video_upload)
    video_handler.block = False
    application.add_handler(video_handler, group=0)
    application.bot_data["manual_text_handler"] = _send_manual
    application.bot_data["manual_video_upload_handler"] = _start_video_upload
    application.bot_data["manual_video_delete_handler"] = _delete_video
    application.bot_data["manual_video_has_file"] = lambda: bool(_load_video_file_id())
