# Файл: app/telegram/handlers/reports.py

"""
Telegram хендлер генерации отчетов.
"""

from math import ceil
from typing import Dict, Any, Optional, List

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CallbackContext, CommandHandler, CallbackQueryHandler

from app.services.reports import ReportService
from app.telegram.middlewares.permissions import PermissionsManager
from app.db.manager import DatabaseManager
from app.db.repositories.operators import OperatorRepository
from app.logging_config import get_watchdog_logger
from app.telegram.utils.logging import describe_user
from app.telegram.utils.messages import safe_edit_message

logger = get_watchdog_logger(__name__)

REPORT_COMMAND = "report"
REPORT_CALLBACK_PREFIX = "reports"
REPORT_PERMISSION = "report"
OPERATORS_PAGE_SIZE = 8


def register_report_handlers(
    application: Application,
    report_service: ReportService,
    permissions_manager: PermissionsManager,
    db_manager: DatabaseManager
) -> None:
    handler = _ReportHandler(report_service, permissions_manager, db_manager)
    application.add_handler(CommandHandler(REPORT_COMMAND, handler.handle_command))
    application.add_handler(
        CallbackQueryHandler(
            handler.handle_callback,
            pattern=rf"^{REPORT_CALLBACK_PREFIX}:"
        )
    )


class _ReportHandler:
    def __init__(
        self,
        report_service: ReportService,
        permissions_manager: PermissionsManager,
        db_manager: DatabaseManager
    ):
        self.report_service = report_service
        self.permissions_manager = permissions_manager
        self.operator_repo = OperatorRepository(db_manager)

    async def handle_command(self, update: Update, context: CallbackContext) -> None:
        message = update.effective_message
        user = update.effective_user
        if not message or not user:
            return

        args = context.args or []
        period = args[0] if args else "daily"
        date_range = args[1] if len(args) > 1 else None
        context.user_data["report_args"] = {
            "period": period,
            "date_range": date_range,
        }

        role = await self.permissions_manager.get_effective_role(user.id, user.username)
        if role in ("admin", "superadmin"):
            logger.info(
                "Админ %s запрашивает отчёт (period=%s, date_range=%s)",
                describe_user(user),
                period,
                date_range,
            )
            await self._show_operator_keyboard(message, page=0)
            return

        is_allowed = await self.permissions_manager.check_permission(role, REPORT_PERMISSION)
        if not is_allowed:
            await message.reply_text("У вас нет прав для генерации отчёта.")
            logger.warning(
                "Пользователь %s попытался вызвать /report без прав",
                describe_user(user),
            )
            return

        await self._send_report_for_user(
            bot=context.bot,
            chat_id=message.chat_id,
            target_user_id=user.id,
            header=f"Генерация отчёта ({period})...",
            period=period,
            date_range=date_range,
        )

    async def handle_callback(self, update: Update, context: CallbackContext) -> None:
        query = update.callback_query
        user = update.effective_user
        if not query or not user:
            return

        await query.answer()
        parts = query.data.split(":")
        if len(parts) < 2:
            return

        action = parts[1]
        if action == "page":
            page = int(parts[2]) if len(parts) > 2 else 0
            await self._show_operator_keyboard(query, page=page, edit=True)
            return

        if action == "select":
            if len(parts) < 3:
                await query.answer("Некорректные данные", show_alert=True)
                return
            try:
                target_user_id = int(parts[2])
            except ValueError:
                await query.answer("Некорректный оператор", show_alert=True)
                return

            args = context.user_data.get("report_args", {})
            period = args.get("period", "daily")
            date_range = args.get("date_range")

            logger.info(
                "Пользователь %s выбрал оператора %s для отчёта",
                describe_user(user),
                target_user_id,
            )
            await self._send_report_for_user(
                bot=context.bot,
                chat_id=query.message.chat_id if query.message else user.id,
                target_user_id=target_user_id,
                header="Генерация отчёта…",
                period=period,
                date_range=date_range,
            )

    async def _show_operator_keyboard(self, target, page: int = 0, edit: bool = False):
        operators = await self.operator_repo.get_approved_operators(include_pending=True)
        cleaned_operators: List[Dict[str, Any]] = []
        skipped_no_extension = 0
        for operator in operators:
            if not operator.get("extension"):
                skipped_no_extension += 1
                continue
            cleaned_operators.append(operator)
        operators = cleaned_operators
        if not operators:
            text = "Нет утверждённых операторов для отчётов."
            if edit and hasattr(target, "edit_message_text"):
                await safe_edit_message(target, text=text)
            else:
                await target.reply_text(text)
            return

        total = len(operators)
        total_pages = max(1, ceil(total / OPERATORS_PAGE_SIZE))
        page = max(0, min(page, total_pages - 1))
        start = page * OPERATORS_PAGE_SIZE
        end = start + OPERATORS_PAGE_SIZE
        page_items = operators[start:end]

        keyboard: List[List[InlineKeyboardButton]] = []
        for operator in page_items:
            target_user_id = operator.get("user_id")
            if not target_user_id:
                continue
            name = operator.get("full_name") or operator.get("username") or "Без имени"
            ext = operator.get("extension")
            status = operator.get("status")
            label = f"{name}" + (f" ({ext})" if ext else "")
            if status and status != "approved":
                label += f" [{status}]"
            keyboard.append([
                InlineKeyboardButton(
                    label[:64],
                    callback_data=f"{REPORT_CALLBACK_PREFIX}:select:{target_user_id}",
                )
            ])

        nav_row: List[InlineKeyboardButton] = []
        if page > 0:
            nav_row.append(
                InlineKeyboardButton(
                    "⬅️ Назад",
                    callback_data=f"{REPORT_CALLBACK_PREFIX}:page:{page-1}",
                )
            )
        if page < total_pages - 1:
            nav_row.append(
                InlineKeyboardButton(
                    "➡️ Далее",
                    callback_data=f"{REPORT_CALLBACK_PREFIX}:page:{page+1}",
                )
            )
        if nav_row:
            keyboard.append(nav_row)

        text_lines = [
            "Выберите оператора для генерации отчёта.",
            f"Показано {start + 1}-{min(end, total)} из {total}.",
        ]
        if skipped_no_extension:
            text_lines.append(
                f"Пропущено {skipped_no_extension} операторов без extension — добавьте его, чтобы видеть в списке."
            )
        text = "\n".join(text_lines)
        markup = InlineKeyboardMarkup(keyboard)

        if edit and hasattr(target, "edit_message_text"):
            await safe_edit_message(
                target,
                text=text,
                reply_markup=markup,
            )
        else:
            await target.reply_text(text, reply_markup=markup)

    async def _send_report_for_user(
        self,
        bot,
        chat_id: int,
        target_user_id: int,
        header: str,
        period: str,
        date_range: Optional[str],
    ):
        operator_info = await self.operator_repo.get_operator_info_by_user_id(target_user_id)
        if not operator_info:
            await bot.send_message(
                chat_id=chat_id,
                text=f"Оператор с ID {target_user_id} не найден в системе.",
            )
            return

        operator_name = operator_info.get("full_name") or operator_info.get("username") or f"оператор {target_user_id}"
        if not operator_info.get("extension"):
            await bot.send_message(
                chat_id=chat_id,
                text=f"Для {operator_name} не указан extension — отчёт недоступен.",
            )
            return

        status_message = await bot.send_message(chat_id=chat_id, text=header)
        try:
            report = await self.report_service.generate_report(
                user_id=target_user_id,
                period=period,
                date_range=date_range,
            )

            if not report:
                await bot.send_message(
                    chat_id=chat_id,
                    text=f"Отчёт для {operator_name} не был сгенерирован (нет данных).",
                )
                return

            chunks = [report[i:i + 4000] for i in range(0, len(report), 4000)]
            for chunk in chunks:
                await bot.send_message(chat_id=chat_id, text=chunk)
        except Exception as exc:
            logger.error("Ошибка при генерации отчёта: %s", exc, exc_info=True)
            await bot.send_message(chat_id=chat_id, text="Произошла ошибка при генерации отчёта.")
        finally:
            try:
                await status_message.delete()
            except Exception:
                pass
