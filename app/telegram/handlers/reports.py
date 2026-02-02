# Файл: app/telegram/handlers/reports.py

"""
Telegram хендлер генерации отчетов.
"""

from math import ceil
from typing import Dict, Any, Optional, List

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    CallbackContext,
    CommandHandler,
    MessageHandler,
    filters,
)

from app.services.reports import ReportService
from app.telegram.middlewares.permissions import PermissionsManager
from app.db.manager import DatabaseManager
from app.db.repositories.operators import OperatorRepository
from app.logging_config import get_watchdog_logger
from app.telegram.utils.logging import describe_user
from app.telegram.utils.messages import safe_edit_message
from app.telegram.utils.callback_data import AdminCB
from app.telegram.utils.admin_registry import register_admin_callback_handler
from app.utils.rate_limit import rate_limit_hit

logger = get_watchdog_logger(__name__)
DB_ERROR_MESSAGE = "Ошибка доступа к базе. Проверьте конфигурацию/схему БД."

REPORT_COMMAND = "report"
REPORT_PERMISSION = "report"
OPERATORS_PAGE_SIZE = 8
REPORT_PERIOD_CHOICES = [
    ("daily", "день"),
    ("weekly", "неделя"),
    ("biweekly", "две недели"),
    ("monthly", "месяц"),
    ("half_year", "полгода"),
    ("yearly", "год"),
]


def register_report_handlers(
    application: Application,
    report_service: ReportService,
    permissions_manager: PermissionsManager,
    db_manager: DatabaseManager
) -> None:
    handler = _ReportHandler(report_service, permissions_manager, db_manager)
    application.add_handler(CommandHandler(REPORT_COMMAND, handler.handle_command))
    register_admin_callback_handler(application, AdminCB.REPORTS, handler.handle_callback)
    application.bot_data["report_handler"] = handler
    application.add_handler(
        MessageHandler(
            filters.Regex(r"(?i)^\s*(?:📊\s*)?(?:ai\s+)?отч[её]ты\s*$"),
            handler.handle_reports_button,
        ),
        group=0,
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
        self._busy_key = "reports_busy"

    async def handle_command(self, update: Update, context: CallbackContext) -> None:
        args = context.args or []
        period = args[0] if args else "daily"
        date_range = args[1] if len(args) > 1 else None
        if self._rate_limited(update, context, "report_command"):
            return
        await self._start_reports_flow(update, context, period, date_range)

    async def handle_reports_button(self, update: Update, context: CallbackContext) -> None:
        if self._rate_limited(update, context, "report_button"):
            return
        logger.info(
            "[REPORTS] Пользователь %s нажал кнопку «Отчет-Операторы»",
            describe_user(update.effective_user),
        )
        await self._start_reports_flow(update, context, period="monthly", date_range=None)

    async def start_report_flow(
        self,
        update: Update,
        context: CallbackContext,
        period: str = "daily",
        date_range: Optional[str] = None,
    ) -> None:
        await self._start_reports_flow(update, context, period, date_range)

    async def _start_reports_flow(
        self,
        update: Update,
        context: CallbackContext,
        period: str,
        date_range: Optional[str],
    ) -> None:
        message = update.effective_message
        user = update.effective_user
        if not message or not user:
            return

        if not self._acquire_busy(context):
            await self._notify_busy(update)
            return

        context.user_data["report_args"] = {
            "period": period,
            "date_range": date_range,
        }

        try:
            can_manage = await self.permissions_manager.can_manage_users(user.id, user.username)
        except Exception:
            logger.exception(
                "report: не удалось определить права пользователя",
                extra={"user_id": user.id, "username": user.username},
            )
            await message.reply_text(DB_ERROR_MESSAGE)
            self._release_busy(context)
            return

        if can_manage:
            logger.info(
                "Админ %s запрашивает отчёт (period=%s, date_range=%s)",
                describe_user(user),
                period,
                date_range,
            )
            await self._render_period_menu(message, period, edit=False)
            self._release_busy(context)
            return

        is_allowed = await self.permissions_manager.check_permission(
            await self.permissions_manager.get_effective_role(user.id, user.username),
            REPORT_PERMISSION,
        )
        if not is_allowed:
            await message.reply_text("У вас нет прав для генерации отчёта.")
            logger.warning(
                "Пользователь %s попытался вызвать /report без прав",
                describe_user(user),
            )
            self._release_busy(context)
            return

        try:
            await self._send_report_for_user(
                bot=context.bot,
                chat_id=message.chat_id,
                target_user_id=user.id,
                header=f"Генерация отчёта ({period})...",
                period=period,
                date_range=date_range,
            )
        finally:
            self._release_busy(context)

    async def handle_callback(self, update: Update, context: CallbackContext) -> None:
        query = update.callback_query
        user = update.effective_user
        if not query or not user:
            return

        logger.info(
            "[REPORTS] Callback от %s: data=%s",
            describe_user(user),
            query.data,
        )

        try:
            await query.answer()
        except BadRequest as exc:
            logger.debug("report: callback уже устарел: %s", exc)
        except Exception:
            logger.exception("report: ошибка ответа на callback")
            raise
        
        # Parse AdminCB: adm:rep:sub_action:args...
        try:
            action_type, args = AdminCB.parse(query.data)
        except (ValueError, TypeError) as exc:
            logger.warning("report: некорректные данные callback '%s': %s", query.data, exc)
            return
        except Exception:
            logger.exception("report: непредвиденная ошибка разбора callback '%s'", query.data)
            raise
        if action_type != AdminCB.REPORTS or not args:
            return

        sub_action = args[0]
        params = args[1:]
        if sub_action == "select" and self._rate_limited(update, context, f"report_callback:{sub_action}"):
            return
        logger.info(
            "[REPORTS] Действие=%s params=%s user=%s",
            sub_action,
            params,
            describe_user(user),
        )
        args_store = context.user_data.setdefault(
            "report_args",
            {"period": "monthly", "date_range": None},
        )

        if sub_action == "period_menu":
            current_period = args_store.get("period", "monthly")
            logger.info(
                "[REPORTS] Открыт выбор периода (current=%s) user=%s",
                current_period,
                describe_user(user),
            )
            await self._render_period_menu(query, current_period, edit=True)
            return

        if sub_action == "period":
            period = params[0] if params else "monthly"
            args_store["period"] = period
            logger.info(
                "[REPORTS] Пользователь %s выбрал период %s",
                describe_user(user),
                period,
            )
            await self._show_operator_keyboard(query, context, page=0, edit=True)
            return

        if sub_action == "page":
            page = self._safe_int(params[0] if params else None, default=0)
            logger.info(
                "[REPORTS] Переключение страницы операторов: page=%s user=%s",
                page,
                describe_user(user),
            )
            await self._show_operator_keyboard(query, context, page=page, edit=True)
            return

        if sub_action == "select":
            try:
                await query.answer("Готовлю отчёт…", show_alert=False)
            except BadRequest:
                pass
            if len(params) < 2:
                try:
                    await query.answer("Некорректные данные", show_alert=True)
                except BadRequest:
                    pass
                return
            try:
                target_user_id = self._safe_int(params[0], default=None)
                extension = params[1]
            except (ValueError, IndexError) as exc:
                logger.warning("Некорректный target_id в report callback '%s': %s", params, exc)
                await query.answer("Некорректный оператор", show_alert=True)
                return
            if not target_user_id:
                await query.answer("Некорректный оператор", show_alert=True)
                return

            args = context.user_data.get("report_args", {})
            period = args.get("period", "daily")
            date_range = args.get("date_range")

            logger.info(
                "Пользователь %s выбрал оператора %s для отчёта (period=%s)",
                describe_user(user),
                target_user_id,
                period,
            )
            if not self._acquire_busy(context, query):
                return
            async def _run_report_task():
                try:
                    await self._send_report_for_user(
                        bot=context.bot,
                        chat_id=query.message.chat_id if query.message else user.id,
                        message_thread_id=query.message.message_thread_id if query.message else None,
                        target_user_id=target_user_id,
                        header="Генерация отчёта…",
                        period=period,
                        date_range=date_range,
                    )
                finally:
                    self._release_busy(context)

            context.application.create_task(_run_report_task())
            return

    def _rate_limited(self, update: Update, context: CallbackContext, key: str) -> bool:
        user = update.effective_user
        if not user:
            return False
        if rate_limit_hit(
            context.application.bot_data,
            user.id,
            f"reports:{key}",
            cooldown_seconds=3.0,
        ):
            cb = update.callback_query
            if cb:
                context.application.create_task(cb.answer("Подождите пару секунд, отчёт ещё готовится.", show_alert=True))
            elif update.message:
                context.application.create_task(update.message.reply_text("⚠️ Отчёт уже считается. Дождитесь результата."))
            return True
        return False

    def _is_busy(self, context: CallbackContext) -> bool:
        return bool(context.user_data.get(self._busy_key))

    def _acquire_busy(self, context: CallbackContext, query=None) -> bool:
        if self._is_busy(context):
            if query:
                try:
                    context.application.create_task(
                        query.answer("Отчёт ещё рассчитывается. Подождите.", show_alert=True)
                    )
                except Exception:
                    logger.exception("report: не удалось отправить busy-ответ")
            return False
        context.user_data[self._busy_key] = True
        return True

    def _release_busy(self, context: CallbackContext) -> None:
        context.user_data.pop(self._busy_key, None)

    async def _notify_busy(self, update: Update) -> None:
        if update.callback_query:
            try:
                await update.callback_query.answer("Отчёт ещё рассчитывается. Подождите.", show_alert=True)
            except BadRequest as exc:
                logger.debug("report: callback уже устарел: %s", exc)
            except Exception:
                logger.exception("report: не удалось отправить busy-ответ")
                raise
        elif update.message:
            await update.message.reply_text("⚠️ Предыдущий отчёт ещё не готов. Дождитесь завершения.")

    async def _show_operator_keyboard(self, target, context: CallbackContext, page: int = 0, edit: bool = False):
        try:
            operators = await self.operator_repo.get_approved_operators(
                include_pending=True
            )
        except Exception:
            logger.exception("report: не удалось загрузить операторов для отчёта")
            if edit:
                await safe_edit_message(target, text=DB_ERROR_MESSAGE)
            else:
                await target.reply_text(DB_ERROR_MESSAGE)
            return
        cleaned_operators: List[Dict[str, Any]] = []
        skipped_no_extension = 0
        for operator in operators:
            if not operator.get("extension"):
                skipped_no_extension += 1
                continue
            cleaned_operators.append(operator)
        operators = cleaned_operators
        logger.info(
            "[REPORTS] Список операторов: total=%s skipped_no_extension=%s",
            len(operators),
            skipped_no_extension,
        )
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

        args = context.user_data.get("report_args") or {}
        current_period = args.get("period", "monthly")

        keyboard: List[List[InlineKeyboardButton]] = []
        for operator in page_items:
            target_user_id = operator.get("user_id")
            extension = operator.get("extension")
            if not target_user_id or not extension:
                continue
            name = (
                operator.get("full_name")
                or operator.get("name")
                or operator.get("username")
                or f"ext {extension}"
            )
            status = operator.get("status")
            label = f"{name} ({extension})"
            if status and status != "approved":
                label += f" [{status}]"
            keyboard.append([
                InlineKeyboardButton(
                    label[:64],
                    callback_data=AdminCB.create(AdminCB.REPORTS, "select", target_user_id, extension),
                )
            ])

        nav_row: List[InlineKeyboardButton] = []
        if page > 0:
            nav_row.append(
                InlineKeyboardButton(
                    "⬅️ Назад",
                    callback_data=AdminCB.create(AdminCB.REPORTS, "page", page-1),
                )
            )
        if page < total_pages - 1:
            nav_row.append(
                InlineKeyboardButton(
                    "➡️ Далее",
                    callback_data=AdminCB.create(AdminCB.REPORTS, "page", page+1),
                )
            )
        if nav_row:
            keyboard.append(nav_row)

        text_lines = [
            "Выберите оператора для генерации отчёта.",
            f"Текущий период: {self._human_period_name(current_period)}.",
            f"Показано {start + 1}-{min(end, total)} из {total}.",
        ]
        if skipped_no_extension:
            text_lines.append(
                f"Пропущено {skipped_no_extension} операторов без extension — добавьте его, чтобы видеть в списке."
            )
        keyboard.append(
            [
                InlineKeyboardButton(
                    "📅 Сменить период",
                    callback_data=AdminCB.create(AdminCB.REPORTS, "period_menu"),
                )
            ]
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
        extension: Optional[str] = None,
        message_thread_id: Optional[int] = None,
    ):
        logger.info(
            "[REPORTS] Генерация отчёта start: target_user_id=%s period=%s date_range=%s",
            target_user_id,
            period,
            date_range,
        )
        try:
            operator_info = await self.operator_repo.get_operator_info_by_user_id(
                target_user_id
            )
        except Exception:
            logger.exception(
                "report: не удалось получить оператора",
                extra={"target_user_id": target_user_id, "chat_id": chat_id},
            )
            await bot.send_message(
                chat_id=chat_id,
                text=DB_ERROR_MESSAGE,
                message_thread_id=message_thread_id,
            )
            return
        if not operator_info:
            await bot.send_message(
                chat_id=chat_id,
                text=f"Оператор с ID {target_user_id} не найден в системе.",
                message_thread_id=message_thread_id,
            )
            return

        operator_extension = operator_info.get("extension") or extension
        operator_name = (
            operator_info.get("full_name")
            or operator_info.get("name")
            or operator_info.get("username")
            or operator_extension
            or f"оператор {target_user_id}"
        )
        if not operator_extension:
            await bot.send_message(
                chat_id=chat_id,
                text=f"Для {operator_name} не указан extension — отчёт недоступен.",
                message_thread_id=message_thread_id,
            )
            return

        try:
            status_message = await bot.send_message(
                chat_id=chat_id,
                text=header,
                message_thread_id=message_thread_id,
            )
        except BadRequest as exc:
            logger.warning("report: не удалось отправить статусное сообщение: %s", exc)
            return
        except Exception:
            logger.exception("report: непредвиденная ошибка при отправке статуса")
            raise
        try:
            report = await self.report_service.generate_report(
                user_id=target_user_id,
                period=period,
                date_range=date_range,
                extension=operator_extension,
            )

            if not report:
                await bot.send_message(
                    chat_id=chat_id,
                    text=f"Отчёт для {operator_name} не был сгенерирован (нет данных).",
                    message_thread_id=message_thread_id,
                )
                logger.info(
                    "[REPORTS] Отчёт пустой: target_user_id=%s period=%s",
                    target_user_id,
                    period,
                )
                return

            chunks = [report[i:i + 4000] for i in range(0, len(report), 4000)]
            for chunk in chunks:
                try:
                    await bot.send_message(
                        chat_id=chat_id,
                        text=chunk,
                        message_thread_id=message_thread_id,
                    )
                except BadRequest as exc:
                    logger.warning("report: не удалось отправить часть отчёта: %s", exc)
                    return
                except Exception:
                    logger.exception("report: непредвиденная ошибка при отправке отчёта")
                    raise
        except Exception:
            logger.exception(
                "report: генерация отчёта завершилась с ошибкой",
                extra={"target_user_id": target_user_id, "period": period},
            )
            await bot.send_message(
                chat_id=chat_id,
                text=DB_ERROR_MESSAGE,
                message_thread_id=message_thread_id,
            )
        finally:
            try:
                await status_message.delete()
            except BadRequest as exc:
                logger.debug("Не удалось удалить статусное сообщение отчёта: %s", exc)
            except Exception:
                logger.exception("report: непредвиденная ошибка при удалении статуса")
                raise
            logger.info(
                "[REPORTS] Генерация отчёта finish: target_user_id=%s period=%s",
                target_user_id,
                period,
            )

    @staticmethod
    def _safe_int(value: Optional[str], default: Optional[int] = 0) -> Optional[int]:
        if value is None:
            return default
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def _human_period_name(self, period: str) -> str:
        mapping = {slug: label for slug, label in REPORT_PERIOD_CHOICES}
        return mapping.get(period, period)

    def _period_keyboard(self, selected: str) -> InlineKeyboardMarkup:
        rows: List[List[InlineKeyboardButton]] = []
        for i in range(0, len(REPORT_PERIOD_CHOICES), 2):
            chunk = REPORT_PERIOD_CHOICES[i:i+2]
            row: List[InlineKeyboardButton] = []
            for slug, label in chunk:
                prefix = "✅ " if slug == selected else ""
                row.append(
                    InlineKeyboardButton(
                        f"{prefix}{label.title()}",
                        callback_data=AdminCB.create(AdminCB.REPORTS, "period", slug),
                    )
                )
            rows.append(row)
        rows.append([InlineKeyboardButton("◀️ Назад", callback_data=AdminCB.create(AdminCB.BACK))])
        return InlineKeyboardMarkup(rows)

    async def _render_period_menu(
        self,
        target,
        period: str,
        edit: bool,
    ) -> None:
        text = (
            "📅 <b>Выбор периода отчёта</b>\n\n"
            "Выберите временной диапазон, за который нужно построить отчёт."
        )
        markup = self._period_keyboard(period)
        if edit and hasattr(target, "edit_message_text"):
            await safe_edit_message(target, text=text, reply_markup=markup, parse_mode="HTML")
        else:
            await target.reply_text(text, reply_markup=markup, parse_mode="HTML")
