# Файл: app/telegram/handlers/manual.py

"""Кнопка «Мануал» и одноимённая команда."""

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

from app.config import MANUAL_URL
from app.logging_config import get_watchdog_logger
from app.telegram.utils.logging import describe_user

MANUAL_TEXT = (
    "По ссылке вы можете найти мануал по использованию бота"
)
logger = get_watchdog_logger(__name__)


async def _send_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user = update.effective_user
    if not message:
        return
    if not MANUAL_URL:
        logger.error("MANUAL_URL не сконфигурирован для manual handler")
        await message.reply_text("Ссылка на мануал временно недоступна.")
        return
    logger.info("Manual запрошен пользователем %s", describe_user(user))
    markup = InlineKeyboardMarkup(
        [[InlineKeyboardButton("📘 Открыть мануал", url=MANUAL_URL)]]
    )
    await message.reply_text(MANUAL_TEXT, reply_markup=markup)


def register_manual_handlers(application: Application) -> None:
    application.add_handler(CommandHandler("manual", _send_manual))
    application.add_handler(
        MessageHandler(
            filters.Regex(r"(?i)^\s*(?:📘\s*)?мануал\s*$"),
            _send_manual,
            block=False,
        ),
        group=0,
    )
    application.bot_data["manual_text_handler"] = _send_manual
