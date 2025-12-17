# Файл: app/telegram/handlers/manual.py

"""Кнопка «Мануал» и одноимённая команда."""

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

from app.config import MANUAL_URL
MANUAL_TEXT = (
    "По ссылке вы можете найти мануал по использованию бота"
)


async def _send_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if not message:
        return
    markup = InlineKeyboardMarkup(
        [[InlineKeyboardButton("📘 Открыть мануал", url=MANUAL_URL)]]
    )
    await message.reply_text(MANUAL_TEXT, reply_markup=markup)


def register_manual_handlers(application: Application) -> None:
    application.add_handler(CommandHandler("manual", _send_manual))
    application.add_handler(
        MessageHandler(
            filters.Regex(r"(?i)мануал") ,
            _send_manual,
            block=False,
        )
    )
