# Файл: app/telegram/handlers/call_lookup.py

"""
Telegram хендлер поиска звонков.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
import uuid
from typing import Any, Dict, List, Optional, Tuple

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Message, Update
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    CallbackContext,
    CallbackQueryHandler,
    CommandHandler,
    MessageHandler,
    filters,
)

from app.services.call_lookup import CallLookupService
from app.telegram.middlewares.permissions import PermissionsManager
from app.telegram.utils.messages import safe_edit_message
from app.logging_config import get_watchdog_logger
from app.telegram.utils.logging import describe_user
from app.utils.error_handlers import log_async_exceptions

CALL_LOOKUP_COMMAND = "call_lookup"
CALL_LOOKUP_PERMISSION = "call_lookup"
CALL_LOOKUP_CALLBACK_PREFIX = "cl"
PERIOD_CHOICES = {
    "daily",
    "weekly",
    "biweekly",
    "monthly",
    "half_year",
    "yearly",
    "custom",
}

logger = get_watchdog_logger(__name__)


def register_call_lookup_handlers(
    application: Application,
    service: CallLookupService,
    permissions_manager: PermissionsManager,
) -> None:
    """
    Регистрирует обработчики команды /call_lookup и её callback-кнопок.
    """
    handler = _CallLookupHandlers(service, permissions_manager)
    application.add_handler(
        CommandHandler(CALL_LOOKUP_COMMAND, handler.handle_command)
    )
    application.add_handler(
        MessageHandler(
            filters.Regex(r"^@\S+\s+/call_lookup"),
            handler.handle_mention_command,
        )
    )
    application.add_handler(
        MessageHandler(
            filters.Regex(r"^🔍 Поиск звонка$"),
            handler.handle_menu_button,
        )
    )
    application.add_handler(
        CallbackQueryHandler(
            handler.handle_callback,
            pattern=rf"^{CALL_LOOKUP_CALLBACK_PREFIX}:",
        )
    )
    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            handler.handle_phone_input,
            block=False,
        )
    )


@dataclass
class _LookupRequest:
    phone: str
    period: str
    offset: int
    limit: int


class _CallLookupHandlers:
    def __init__(
        self,
        service: CallLookupService,
        permissions_manager: PermissionsManager,
    ):
        self.service = service
        self.permissions_manager = permissions_manager
        self._error_reply = (
            "Не удалось выполнить поиск, ошибка конфигурации БД."
        )
        self._pending_key = "call_lookup_pending"
        self._last_request_key = "call_lookup_last_request"

    @staticmethod
    def _generate_error_code() -> str:
        return f"ERR-{uuid.uuid4().hex[:8].upper()}"

    def _format_error_text(self, code: str, base: Optional[str] = None) -> str:
        text = base or self._error_reply
        return f"{text}\nКод ошибки: {code}"

    def _limit_callback_data(self, data: str, fallback: str) -> str:
        try:
            if len(data.encode("utf-8")) <= 64:
                return data
        except Exception as exc:
            logger.debug("Не удалось оценить размер callback_data '%s': %s", data, exc, exc_info=True)
        logger.warning(
            "callback_data too long (%s bytes), fallback=%s",
            len(data.encode("utf-8")) if isinstance(data, str) else "?",
            fallback,
        )
        return fallback

    def _remember_request(
        self, context: CallbackContext, request: _LookupRequest
    ) -> None:
        context.user_data[self._last_request_key] = {
            "phone": request.phone,
            "period": request.period,
            "limit": request.limit,
        }

    def _restore_request(
        self,
        context: CallbackContext,
        *,
        offset: int = 0,
    ) -> Optional[_LookupRequest]:
        payload = context.user_data.get(self._last_request_key)
        if not isinstance(payload, dict):
            return None
        phone = payload.get("phone")
        period = payload.get("period")
        limit = payload.get("limit") or self.service.DEFAULT_LIMIT
        if not phone or not period:
            return None
        return _LookupRequest(
            phone=str(phone),
            period=str(period),
            offset=max(0, int(offset)),
            limit=int(limit),
        )

    async def _safe_reply_text(
        self,
        message: Optional[Message],
        text: str,
        *,
        parse_mode: Optional[str] = None,
        reply_markup: Optional[InlineKeyboardMarkup] = None,
    ) -> None:
        if not message:
            return
        try:
            await message.reply_text(
                text,
                parse_mode=parse_mode,
                reply_markup=reply_markup,
            )
        except BadRequest as exc:
            logger.warning("Не удалось отправить сообщение: %s", exc, exc_info=True)

    async def _safe_send_message(
        self,
        context: CallbackContext,
        chat_id: int,
        text: str,
        *,
        parse_mode: Optional[str] = None,
        reply_markup: Optional[InlineKeyboardMarkup] = None,
    ) -> None:
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=parse_mode,
                reply_markup=reply_markup,
            )
        except BadRequest as exc:
            logger.warning("Не удалось отправить сообщение: %s", exc, exc_info=True)

    @log_async_exceptions
    async def handle_command(self, update: Update, context: CallbackContext) -> None:
        message = update.effective_message
        user = update.effective_user
        if not message or not user:
            return

        context.user_data.pop(self._pending_key, None)

        if not await self._is_allowed(user.id, user.username):
            logger.warning(
                "Отказ в /call_lookup для %s",
                describe_user(user),
            )
            await self._safe_reply_text(
                message,
                "Команда доступна только старшим администраторам. "
                "Обратитесь к администратору для получения доступа.",
            )
            return

        args = context.args or []
        if not args:
            await self._send_usage_hint(message)
            return

        try:
            phone, period = self._parse_command_args(args)
        except ValueError as parse_error:
            logger.warning(
                "Некорректные аргументы /call_lookup от %s: %s",
                describe_user(user),
                parse_error,
                exc_info=True,
            )
            await self._safe_reply_text(message, str(parse_error))
            return

        logger.info(
            "Пользователь %s выполняет /call_lookup (phone=%s, period=%s)",
            describe_user(user),
            phone,
            period,
        )
        try:
            response = await self.service.lookup_calls(
                phone=phone,
                period=period,
                offset=0,
                requesting_user_id=user.id,
            )
        except ValueError as exc:
            logger.warning(
                "Ошибка валидации /call_lookup (%s): %s",
                describe_user(user),
                exc,
                exc_info=True,
            )
            await self._safe_reply_text(message, f"Ошибка: {exc}")
            return
        except Exception:
            code = self._generate_error_code()
            logger.exception(
                "Не удалось выполнить /call_lookup для %s (code=%s)",
                describe_user(user),
                code,
            )
            await self._safe_reply_text(
                message,
                self._format_error_text(code),
            )
            return

        request = _LookupRequest(
            phone=response["normalized_phone"],
            period=period,
            offset=0,
            limit=response["limit"],
        )
        text, markup = self._build_result_message(
            response=response,
            period=period,
            request=request,
        )

        await self._safe_reply_text(message, text, reply_markup=markup)
        self._remember_request(context, request)
        logger.info(
            "Пользователь %s получил %s звонков по запросу /call_lookup",
            describe_user(user),
            response.get("count"),
        )

    @log_async_exceptions
    async def handle_mention_command(
        self, update: Update, context: CallbackContext
    ) -> None:
        """Обрабатывает сообщения вида '@bot /call_lookup ...'."""
        message = update.effective_message
        if not message or not message.text:
            return

        tokens = message.text.strip().split()
        command_index = next(
            (i for i, token in enumerate(tokens) if token.startswith("/call_lookup")),
            None,
        )
        if command_index is None:
            return

        context.args = tokens[command_index + 1 :]
        await self.handle_command(update, context)

    @log_async_exceptions
    async def handle_menu_button(self, update: Update, context: CallbackContext) -> None:
        """Реакция на кнопку главного меню «Поиск звонка»."""
        message = update.effective_message
        user = update.effective_user
        if not message or not user:
            return
        logger.info(
            "[CALL_LOOKUP] Пользователь %s нажал кнопку «🔍 Поиск звонка»",
            describe_user(user),
        )

        if not await self._is_allowed(user.id, user.username):
            await self._safe_reply_text(
                message,
                "Команда доступна только старшим администраторам. "
                "Обратитесь к администратору для получения доступа.",
            )
            return

        await self._send_usage_hint(message)

    @log_async_exceptions
    async def handle_callback(self, update: Update, context: CallbackContext) -> None:
        query = update.callback_query
        user = update.effective_user
        if not query or not user:
            return

        parts = (query.data or "").split(":")
        if len(parts) < 2 or parts[0] != CALL_LOOKUP_CALLBACK_PREFIX:
            return

        await query.answer()

        action = parts[1]
        chat_id = query.message.chat_id if query.message else user.id

        if not await self._is_allowed(user.id, user.username):
            await safe_edit_message(query, text="Доступ запрещён.")
            logger.warning(
                "Call lookup callback отклонён для %s (action=%s)",
                describe_user(user),
                action,
            )
            return
        logger.info(
            "Call lookup callback получен: action=%s user=%s",
            action,
            describe_user(user),
        )

        if action == "ask":
            period = parts[2] if len(parts) > 2 else "monthly"
            context.user_data[self._pending_key] = {"period": period}
            await self._safe_send_message(
                context,
                chat_id,
                f"Введите номер телефона для поиска звонков ({period}).",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "⬅️ Назад",
                                callback_data=f"{CALL_LOOKUP_CALLBACK_PREFIX}:cancel",
                            )
                        ]
                    ]
                ),
            )
            logger.info(
                "Call lookup запрос номера (period=%s) пользователем %s",
                period,
                describe_user(user),
            )
        elif action == "p":
            if len(parts) < 3:
                await query.answer("Некорректные данные", show_alert=True)
                return
            try:
                offset_value = max(0, int(parts[2]))
            except ValueError as exc:
                logger.warning("Некорректный offset '%s' в callback %s: %s", parts[2] if len(parts) > 2 else "?", data, exc)
                await query.answer("Некорректный offset", show_alert=True)
                return
            restored = self._restore_request(context, offset=offset_value)
            if not restored:
                await query.answer("Запрос устарел, выполните поиск заново", show_alert=True)
                return
            request = restored
            logger.info(
                "Call lookup пагинация (%s) пользователем %s",
                request,
                describe_user(user),
            )
            try:
                response = await self.service.lookup_calls(
                    phone=request.phone,
                    period=request.period,
                    offset=request.offset,
                    limit=request.limit,
                    requesting_user_id=user.id,
                )
            except Exception:
                code = self._generate_error_code()
                logger.exception(
                    "Ошибка пагинации call_lookup для %s (code=%s)",
                    describe_user(user),
                    code,
                )
                await safe_edit_message(
                    query,
                    text=self._format_error_text(code),
                )
                return
            text, markup = self._build_result_message(
                response=response,
                period=request.period,
                request=request,
            )
            await self._edit_or_send(
                chat_id=query.message.chat_id if query.message else None,
                message=query.message,
                context=context,
                text=text,
                markup=markup,
            )
            self._remember_request(context, request)
        elif action == "t":
            if len(parts) < 3:
                await query.answer("Некорректные данные", show_alert=True)
                return
            try:
                history_id = int(parts[2])
            except ValueError as exc:
                logger.warning("Некорректный history_id '%s' (action=t): %s", parts[2] if len(parts) > 2 else "?", exc)
                await query.answer("Некорректный ID", show_alert=True)
                return
            try:
                details = await self.service.fetch_call_details(history_id)
            except Exception:
                code = self._generate_error_code()
                logger.exception(
                    "Ошибка загрузки расшифровки звонка %s от %s (code=%s)",
                    history_id,
                    describe_user(user),
                    code,
                )
                await self._safe_send_message(
                    context,
                    chat_id,
                    self._format_error_text(code),
                )
                return
            details_payload = details or {}
            transcript = details_payload.get("transcript")
            text = self._format_transcript_details(details_payload, transcript)
            await self._safe_send_message(
                context, chat_id, text, parse_mode="HTML"
            )
            logger.info(
                "Пользователь %s запросил расшифровку звонка %s",
                describe_user(user),
                history_id,
            )
        elif action == "r":
            if len(parts) < 3:
                await query.answer("Некорректные данные", show_alert=True)
                return
            try:
                history_id = int(parts[2])
            except ValueError as exc:
                logger.warning("Некорректный history_id '%s' (action=r): %s", parts[2] if len(parts) > 2 else "?", exc)
                await query.answer("Некорректный ID", show_alert=True)
                return
            try:
                details = await self.service.fetch_call_details(history_id)
            except Exception:
                code = self._generate_error_code()
                logger.exception(
                    "Ошибка загрузки записи звонка %s от %s (code=%s)",
                    history_id,
                    describe_user(user),
                    code,
                )
                await self._safe_send_message(
                    context,
                    chat_id,
                    self._format_error_text(code),
                )
                return
            details_payload = details or {}
            record_url = details_payload.get("record_url")
            if record_url:
                await self._safe_send_message(
                    context,
                    chat_id,
                    self._format_record_message(history_id, details_payload),
                    parse_mode="HTML",
                )
                logger.info(
                    "Пользователь %s запросил запись звонка %s",
                    describe_user(user),
                    history_id,
                )
            else:
                await self._safe_send_message(
                    context,
                    chat_id,
                    "Запись недоступна для этого звонка.",
                )
        elif action == "cancel":
            context.user_data.pop(self._pending_key, None)
            await self._safe_send_message(
                context,
                chat_id,
                "Режим поиска звонков закрыт.",
            )
    @log_async_exceptions
    async def handle_phone_input(
        self,
        update: Update,
        context: CallbackContext,
    ) -> None:
        message = update.effective_message
        user = update.effective_user
        if not message or not user:
            return

        pending = context.user_data.get(self._pending_key)
        if not pending:
            return

        phone_text = (message.text or "").strip()
        if not phone_text:
            await self._safe_reply_text(
                message,
                "Введите номер телефона цифрами.",
            )
            return

        if not re.search(r"\d", phone_text):
            return

        period = pending.get("period", "monthly")
        try:
            response = await self.service.lookup_calls(
                phone=phone_text,
                period=period,
                offset=0,
                limit=self.service.DEFAULT_LIMIT,
                requesting_user_id=user.id,
            )
        except ValueError as exc:
            logger.warning(
                "Call lookup ввёл некорректные данные %s: %s",
                describe_user(user),
                exc,
            )
            await self._safe_reply_text(message, str(exc))
            return
        except Exception:
            code = self._generate_error_code()
            logger.exception(
                "Call lookup (interactive) упал у %s (code=%s)",
                describe_user(user),
                code,
            )
            await self._safe_reply_text(
                message,
                self._format_error_text(code),
            )
            context.user_data.pop(self._pending_key, None)
            return

        request = _LookupRequest(
            phone=response["normalized_phone"],
            period=period,
            offset=0,
            limit=response["limit"],
        )
        text, markup = self._build_result_message(
            response=response,
            period=period,
            request=request,
        )
        await self._safe_send_message(
            context,
            message.chat_id,
            text,
            reply_markup=markup,
        )
        self._remember_request(context, request)
        context.user_data.pop(self._pending_key, None)
        await self._safe_reply_text(message, "Режим поиска звонков завершён.")

    async def _is_allowed(self, user_id: int, username: Optional[str] = None) -> bool:
        # Supremes/devs всегда имеют доступ
        if self.permissions_manager.is_supreme_admin(user_id, username) or self.permissions_manager.is_dev_admin(user_id, username):
            return True
        
        status = await self.permissions_manager.get_user_status(user_id)
        if status != 'approved':
            return False
        
        role = await self.permissions_manager.get_effective_role(user_id, username)
        return await self.permissions_manager.check_permission(
            role, CALL_LOOKUP_PERMISSION
        )

    def _build_result_message(
        self,
        *,
        response: Dict[str, Any],
        period: str,
        request: _LookupRequest,
    ) -> Tuple[str, Optional[InlineKeyboardMarkup]]:
        normalized_phone = response["normalized_phone"]
        items: List[Dict[str, Any]] = response.get("items", [])
        lines = [
            f"Поиск по номеру: {normalized_phone}",
            f"Период: {period}",
        ]

        if not items:
            lines.append("По заданным параметрам ничего не найдено.")
            return "\n".join(lines), None

        for idx, item in enumerate(items, start=request.offset + 1):
            timestamp = self._format_datetime(item.get("call_time"))
            duration = self._format_duration(item.get("talk_duration"))
            info = f"{item.get('caller_info') or '-'} → {item.get('called_info') or '-'}"
            patient = item.get("caller_number") or "—"
            piece = (
                f"{idx}. {timestamp} | {info}\n"
                f"   Пациент: {patient}\n"
                f"   ID: {item.get('history_id')} | Длительность: {duration} | "
                f"Оценка: {item.get('score') if item.get('score') is not None else '—'}"
            )
            lines.append(piece)

        keyboard: List[List[InlineKeyboardButton]] = []
        for item in items:
            history_id = item.get("history_id")
            if not history_id:
                continue
            row = [
                InlineKeyboardButton(
                    "Расшифровка",
                    callback_data=self._limit_callback_data(
                        f"{CALL_LOOKUP_CALLBACK_PREFIX}:t:{history_id}",
                        f"{CALL_LOOKUP_CALLBACK_PREFIX}:t:{history_id}",
                    ),
                )
            ]
            if item.get("record_url"):
                row.append(
                    InlineKeyboardButton(
                        "Запись",
                        callback_data=self._limit_callback_data(
                            f"{CALL_LOOKUP_CALLBACK_PREFIX}:r:{history_id}",
                            f"{CALL_LOOKUP_CALLBACK_PREFIX}:r:{history_id}",
                        ),
                    )
                )
            keyboard.append(row)

        pagination_row: List[InlineKeyboardButton] = []
        prev_offset = max(0, request.offset - request.limit)
        if request.offset > 0:
            pagination_row.append(
                InlineKeyboardButton(
                    "⬅️ Назад",
                    callback_data=self._limit_callback_data(
                        self._encode_page_callback(offset=prev_offset),
                        f"{CALL_LOOKUP_CALLBACK_PREFIX}:p:{prev_offset}",
                    ),
                )
            )
        if response["count"] >= request.limit:
            pagination_row.append(
                InlineKeyboardButton(
                    "➡️ Далее",
                    callback_data=self._limit_callback_data(
                        self._encode_page_callback(
                            offset=request.offset + request.limit,
                        ),
                        f"{CALL_LOOKUP_CALLBACK_PREFIX}:p:{request.offset + request.limit}",
                    ),
                )
            )
        if pagination_row:
            keyboard.append(pagination_row)

        markup = InlineKeyboardMarkup(keyboard) if keyboard else None
        return "\n".join(lines), markup

    def _parse_command_args(self, args: List[str]) -> Tuple[str, str]:
        tokens = [token for token in args if token.strip()]
        if not tokens:
            raise ValueError("Добавьте номер телефона.")

        period: Optional[str] = None
        phone_tokens: List[str] = []

        for token in tokens:
            if token.startswith("@"):  # игнорируем упоминания бота
                continue
            lowered = token.lower()
            if lowered in PERIOD_CHOICES and period is None:
                period = lowered
                continue
            phone_tokens.append(token)

        if not phone_tokens:
            raise ValueError("Добавьте номер телефона в команду.")

        phone = "".join(phone_tokens)
        if not phone.strip():
            raise ValueError("Добавьте номер телефона в команду.")

        return phone, (period or "monthly")

    async def _send_usage_hint(self, message: Message) -> None:
        text = (
            "📂 <b>Расшифровки</b>\n\n"
            "Выберите период, после чего введите номер телефона — бот покажет расшифровки "
            "по нужному пациенту. Если нужно выйти из режима, нажмите кнопку «Назад»."
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
            [
                InlineKeyboardButton(
                    "◀️ Назад",
                    callback_data=f"{CALL_LOOKUP_CALLBACK_PREFIX}:cancel",
                )
            ],
        ]
        await self._safe_reply_text(
            message,
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="HTML",
        )

    async def _edit_or_send(
        self,
        *,
        chat_id: Optional[int],
        message: Optional[Message],
        context: CallbackContext,
        text: str,
        markup: Optional[InlineKeyboardMarkup],
    ) -> None:
        if message:
            try:
                await message.edit_text(text, reply_markup=markup)
            except BadRequest as exc:
                logger.warning(
                    "Не удалось обновить сообщение поиска звонков: %s",
                    exc,
                    exc_info=True,
                )
                if chat_id is not None:
                    await self._safe_send_message(
                        context,
                        chat_id,
                        text,
                        reply_markup=markup,
                    )
        elif chat_id is not None:
            await self._safe_send_message(
                context,
                chat_id,
                text,
                reply_markup=markup,
            )

    def _encode_page_callback(
        self,
        *,
        offset: int,
    ) -> str:
        safe_offset = max(0, int(offset))
        return f"{CALL_LOOKUP_CALLBACK_PREFIX}:p:{safe_offset}"

    @staticmethod
    def _format_datetime(value: Any) -> str:
        if isinstance(value, datetime):
            return value.strftime("%d.%m %H:%M")
        if isinstance(value, str):
            return value
        return "-"

    @staticmethod
    def _format_duration(value: Any) -> str:
        if not value:
            return "—"
        seconds = int(value)
        minutes, secs = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        if hours:
            return f"{hours}ч {minutes:02d}м"
        if minutes:
            return f"{minutes}м {secs:02d}с"
        return f"{secs}с"

    def _format_transcript_details(
        self,
        details: Dict[str, Any],
        transcript: Optional[str],
    ) -> str:
        if not details:
            return "ℹ️ Расшифровка недоступна."

        patient = details.get("caller_number") or "-"
        call_time = self._format_datetime(details.get("call_time"))
        record_url = details.get("record_url")
        recording_id = details.get("recording_id") or "—"
        lm_metrics = details.get("lm_metrics") or []
        transcript_text = transcript or "Расшифровка отсутствует."

        metrics_lines = self._format_metrics(lm_metrics)

        message_lines = [
            f"ℹ️ <b>Звонок #{details.get('history_id')}</b>",
            f"Пациент: {patient}",
            f"Время: {call_time}",
            f"recording_id: {recording_id}",
            "",
            f"<b>Расшифровка:</b>\n{transcript_text}",
        ]

        if metrics_lines:
            message_lines.append("")
            message_lines.append("<b>Метрики:</b>")
            message_lines.extend(metrics_lines)

        if record_url:
            message_lines.append("")
            message_lines.append(f"🎧 <a href=\"{record_url}\">Слушать запись</a>")

        return "\n".join(message_lines)

    def _format_metrics(self, metrics: List[Dict[str, Any]]) -> List[str]:
        lines: List[str] = []
        for metric in metrics:
            code = metric.get("metric_code")
            value = metric.get("value_numeric")
            label = metric.get("value_label")
            if isinstance(value, (int, float)):
                formatted_value = f"{value:.2f}"
            else:
                formatted_value = value if value is not None else label or "-"
            lines.append(f"• {code}: {formatted_value}")
        return lines

    def _format_record_message(
        self,
        history_id: int,
        details: Dict[str, Any],
    ) -> str:
        record_url = details.get("record_url") if details else None
        recording_id = details.get("recording_id") if details else None
        parts = [f"🎧 Запись звонка #{history_id}"]
        if recording_id:
            parts.append(f"recording_id: {recording_id}")
        if record_url:
            parts.append(record_url)
        return "\n".join(parts)
