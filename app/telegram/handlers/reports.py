"""
Telegram хендлер генерации отчетов.
"""

from telegram import Update
from telegram.ext import Application, CallbackContext, CommandHandler

from app.services.reports import ReportService
from app.telegram.middlewares.permissions import PermissionsManager
from app.db.manager import DatabaseManager
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)

REPORT_COMMAND = "report"
REPORT_PERMISSION = "view_reports"


def register_report_handlers(
    application: Application,
    report_service: ReportService,
    permissions_manager: PermissionsManager,
    db_manager: DatabaseManager
) -> None:
    handler = _ReportHandler(report_service, permissions_manager, db_manager)
    application.add_handler(CommandHandler(REPORT_COMMAND, handler.handle_command))


class _ReportHandler:
    def __init__(
        self,
        report_service: ReportService,
        permissions_manager: PermissionsManager,
        db_manager: DatabaseManager
    ):
        self.report_service = report_service
        self.permissions_manager = permissions_manager
        self.db_manager = db_manager

    async def handle_command(self, update: Update, context: CallbackContext) -> None:
        message = update.effective_message
        user = update.effective_user
        if not message or not user:
            return

        # Проверка прав (можно расширить позже)
        # role = await self.permissions_manager.get_user_role(user.id)
        # if not role:
        #     await message.reply_text("У вас нет прав на выполнение этой команды.")
        #     return

        args = context.args or []
        period = args[0] if args else "daily"
        date_range = args[1] if len(args) > 1 else None

        await message.reply_text(f"Генерация отчета за период: {period}...")

        try:
            report = await self.report_service.generate_report(
                user_id=user.id,
                period=period,
                date_range=date_range
            )
            
            if report:
                # Разбиваем длинные сообщения
                if len(report) > 4096:
                    for x in range(0, len(report), 4096):
                        await message.reply_text(report[x:x+4096])
                else:
                    await message.reply_text(report)
            else:
                await message.reply_text("Отчет не был сгенерирован (возможно, нет данных).")

        except Exception as e:
            logger.error(f"Ошибка при генерации отчета: {e}", exc_info=True)
            await message.reply_text("Произошла ошибка при генерации отчета.")
