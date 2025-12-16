# Файл: app/telegram/handlers/weekly_quality.py

"""
Telegram хендлер еженедельных отчетов качества.
"""

from datetime import datetime
from typing import Optional, Tuple

from telegram import Update
from telegram.ext import Application, CallbackContext, CommandHandler

from app.telegram.middlewares.permissions import PermissionsManager
from app.services.weekly_quality import WeeklyQualityService
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)

WEEKLY_QUALITY_COMMAND = "weekly_quality"
WEEKLY_QUALITY_PERMISSION = "weekly_quality"


def register_weekly_quality_handlers(
    application: Application,
    service: WeeklyQualityService,
    permissions_manager: PermissionsManager,
) -> None:
    handler = _WeeklyQualityHandler(service, permissions_manager)
    application.add_handler(CommandHandler(WEEKLY_QUALITY_COMMAND, handler.handle_command))


class _WeeklyQualityHandler:
    def __init__(
        self,
        service: WeeklyQualityService,
        permissions_manager: PermissionsManager,
    ) -> None:
        self.service = service
        self.permissions_manager = permissions_manager

    async def handle_command(self, update: Update, context: CallbackContext) -> None:
        message = update.effective_message
        user = update.effective_user
        if not message or not user:
            return

        if not await self._is_allowed(user.id, user.username):
            await message.reply_text(
                "Команда доступна только старшим администраторам. "
                "Обратитесь к администратору для получения доступа."
            )
            return

        try:
            period, custom_range = self._parse_args(context.args or [])
            report_text = await self.service.get_text_report(
                period=period,
                start_date=custom_range[0] if custom_range else None,
                end_date=custom_range[1] if custom_range else None,
            )
        except ValueError as exc:
            await message.reply_text(f"Ошибка: {exc}")
            return
        except Exception as exc:
            logger.exception("Ошибка при генерации weekly_quality: %s", exc)
            await message.reply_text(
                "Не удалось получить отчёт качества. Попробуйте позже."
            )
            return

        await message.reply_text(report_text)

    async def _is_allowed(self, user_id: int, username: Optional[str] = None) -> bool:
        if self.permissions_manager.is_supreme_admin(user_id, username) or self.permissions_manager.is_dev_admin(user_id, username):
            return True
        
        status = await self.permissions_manager.get_user_status(user_id)
        if status != 'approved':
            return False
        
        role = await self.permissions_manager.get_effective_role(user_id, username)
        return await self.permissions_manager.check_permission(
            role, WEEKLY_QUALITY_PERMISSION
        )

    def _parse_args(self, args: list[str]) -> Tuple[str, Optional[Tuple[datetime, datetime]]]:
        if not args:
            return "weekly", None

        period = args[0].lower()
        if period != "custom":
            return period, None

        if len(args) < 3:
            raise ValueError(
                "Для периода custom необходимо указать даты: "
                "/weekly_quality custom 2024-01-01 2024-01-07"
            )

        start = self._parse_date(args[1])
        end = self._parse_date(args[2])
        if start > end:
            raise ValueError("Начальная дата должна быть раньше конечной.")
        return "custom", (start, end)

    @staticmethod
    def _parse_date(value: str) -> datetime:
        for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        raise ValueError(
            f"Некорректная дата: {value}. Допустимые форматы: YYYY-MM-DD, DD.MM.YYYY, DD/MM/YYYY."
        )
