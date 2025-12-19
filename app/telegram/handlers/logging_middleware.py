from __future__ import annotations

from telegram import Update
from telegram.ext import Application, CallbackQueryHandler, ContextTypes, MessageHandler, filters

from app.logging_config import get_watchdog_logger
from app.telegram.utils.logging import describe_user

logger = get_watchdog_logger(__name__)


async def _log_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user = update.effective_user
    if not message or not message.text:
        return
    logger.info(
        "[INPUT_TEXT] user=%s chat_id=%s text=%r",
        describe_user(user),
        message.chat_id if message else None,
        message.text,
    )


async def _log_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if not query:
        return
    logger.info(
        "[INPUT_CALLBACK] user=%s message_id=%s data=%r",
        describe_user(user),
        query.message.message_id if query.message else None,
        query.data,
    )


def register_logging_handlers(application: Application) -> None:
    """
    Регистрирует лёгкие лог-фильтры, которые фиксируют текстовые кнопки
    и callback-и до того, как те попадут в основной роутинг.
    """
    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            _log_text,
            block=False,
        ),
        group=-1,
    )
    application.add_handler(
        CallbackQueryHandler(
            _log_callback,
            block=False,
        ),
        group=-1,
    )
