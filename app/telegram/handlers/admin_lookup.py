# Файл: app/telegram/handlers/admin_lookup.py

"""
Раздел админ-панели для работы с расшифровками.
"""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, ContextTypes

from app.telegram.middlewares.permissions import PermissionsManager
from app.telegram.handlers.call_lookup import CALL_LOOKUP_CALLBACK_PREFIX
from app.utils.error_handlers import log_async_exceptions
from app.logging_config import get_watchdog_logger
from app.telegram.utils.logging import describe_user
from app.telegram.utils.messages import safe_edit_message

logger = get_watchdog_logger(__name__)


class AdminLookupHandler:
    """Подсказки и быстрые действия для раздела Расшифровок."""

    def __init__(self, permissions: PermissionsManager):
        self.permissions = permissions

    @log_async_exceptions
    async def show_lookup_entry(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        query = update.callback_query
        if not query:
            return

        await query.answer()

        try:
            user = update.effective_user
            if not await self.permissions.can_access_call_lookup(user.id, user.username):
                await query.answer("Нет доступа к расшифровкам", show_alert=True)
                logger.warning(
                    "Пользователь %s попытался открыть раздел расшифровок без прав",
                    describe_user(user),
                )
                return

            logger.info(
                "Админ %s открыл подсказку раздела расшифровок",
                describe_user(user),
            )
            message = (
                "📂 <b>Расшифровки</b>\n\n"
                "Выберите период, после чего бот попросит ввести номер телефона "
                "и автоматически выполнит поиск. Никаких команд вручную вводить не нужно."
            )

            keyboard = [
                [
                    InlineKeyboardButton(
                        "Daily",
                        callback_data=f"{CALL_LOOKUP_CALLBACK_PREFIX}:ask:daily",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "Weekly",
                        callback_data=f"{CALL_LOOKUP_CALLBACK_PREFIX}:ask:weekly",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "Monthly",
                        callback_data=f"{CALL_LOOKUP_CALLBACK_PREFIX}:ask:monthly",
                    )
                ],
                [InlineKeyboardButton("◀️ Назад", callback_data="admin:back")],
            ]

            await safe_edit_message(
                query,
                text=message,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML",
            )
        except Exception as exc:
            logger.exception("Не удалось открыть подсказку расшифровок: %s", exc)
            await safe_edit_message(
                query,
                text="⚠️ Не удалось открыть раздел «Расшифровки». Попробуйте снова чуть позже.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("◀️ Назад", callback_data="admin:back")]]
                ),
            )


def register_admin_lookup_handlers(
    application: Application,
    permissions: PermissionsManager,
):
    handler = AdminLookupHandler(permissions)
    application.add_handler(
        CallbackQueryHandler(handler.show_lookup_entry, pattern=r"^admin:lookup")
    )
    logger.info("Admin lookup handlers registered")
