"""
Раздел админ-панели для работы с расшифровками.
"""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, ContextTypes

from app.telegram.middlewares.permissions import PermissionsManager
from app.utils.error_handlers import log_async_exceptions
from app.logging_config import get_watchdog_logger
from app.telegram.utils.logging import describe_user

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
            "Введите команду <code>/call_lookup &lt;номер&gt; [период]</code>, "
            "например: <code>/call_lookup +7 999 1234567 weekly</code>.\n\n"
            "Ниже — быстрые кнопки для вставки команды в чат."
        )

        keyboard = [
            [
                InlineKeyboardButton(
                    "🔍 Вставить команду",
                    switch_inline_query_current_chat="/call_lookup ",
                )
            ],
            [
                InlineKeyboardButton(
                    "Daily",
                    switch_inline_query_current_chat="/call_lookup  daily",
                ),
                InlineKeyboardButton(
                    "Weekly",
                    switch_inline_query_current_chat="/call_lookup  weekly",
                ),
                InlineKeyboardButton(
                    "Monthly",
                    switch_inline_query_current_chat="/call_lookup  monthly",
                ),
            ],
            [InlineKeyboardButton("◀️ Назад", callback_data="admin:back")],
        ]

        await query.edit_message_text(
            text=message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="HTML",
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
