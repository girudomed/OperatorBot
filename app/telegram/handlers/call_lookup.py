"""
Telegram хендлер поиска звонков.
"""

import base64
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Message, Update
from telegram.ext import (
    Application,
    CallbackContext,
    CallbackQueryHandler,
    CommandHandler,
)

from app.services.call_lookup import CallLookupService
from app.telegram.middlewares.permissions import PermissionsManager

CALL_LOOKUP_COMMAND = "call_lookup"
CALL_LOOKUP_PERMISSION = "call_lookup"
CALL_LOOKUP_CALLBACK_PREFIX = "calllookup"


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
        CallbackQueryHandler(
            handler.handle_callback,
            pattern=rf"^{CALL_LOOKUP_CALLBACK_PREFIX}:",
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

        args = context.args or []
        if not args:
            await message.reply_text(
                "Использование: /call_lookup <номер> [период]\n"
                "Пример: /call_lookup +7 999 123 45 67 weekly"
            )
            return

        phone = "".join(args[0:2]) if args[0] == "+7" and len(args) > 1 else args[0]
        period = args[1] if len(args) > 1 else "monthly"

        try:
            response = await self.service.lookup_calls(
                phone=phone,
                period=period,
                offset=0,
                requesting_user_id=user.id,
            )
        except ValueError as exc:
            await message.reply_text(f"Ошибка: {exc}")
            return
        except Exception:
            await message.reply_text(
                "Не удалось выполнить поиск. Попробуйте позже."
            )
            raise

        text, markup = self._build_result_message(
            response=response,
            period=period,
            request=_LookupRequest(
                phone=response["normalized_phone"],
                period=period,
                offset=0,
                limit=response["limit"],
            ),
        )

        await message.reply_text(text, reply_markup=markup)

    async def handle_callback(self, update: Update, context: CallbackContext) -> None:
        query = update.callback_query
        user = update.effective_user
        if not query or not user:
            return

        await query.answer()

        parts = query.data.split(":", 2)
        if len(parts) < 3:
            return

        action = parts[1]
        payload = self._decode_payload(parts[2])

        if not await self._is_allowed(user.id, user.username):
            await query.edit_message_text("Доступ запрещён.")
            return

        if action == "page":
            request = _LookupRequest(
                phone=payload["phone"],
                period=payload["period"],
                offset=max(0, int(payload.get("offset", 0))),
                limit=int(payload.get("limit", 5)),
            )
            response = await self.service.lookup_calls(
                phone=request.phone,
                period=request.period,
                offset=request.offset,
                limit=request.limit,
                requesting_user_id=user.id,
            )
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
        elif action == "transcript":
            history_id = int(payload["history_id"])
            details = await self.service.fetch_call_details(history_id)
            transcript = details.get("transcript") if details else None
            if not transcript:
                transcript = "Расшифровка отсутствует."
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=f"ℹ️ Расшифровка звонка #{history_id}:\n{transcript}",
            )
        elif action == "record":
            history_id = int(payload["history_id"])
            details = await self.service.fetch_call_details(history_id)
            record_url = details.get("record_url") if details else None
            if record_url:
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text=f"🎧 Запись звонка #{history_id}: {record_url}",
                )
            else:
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text="Запись недоступна для этого звонка.",
                )

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
            piece = (
                f"{idx}. {timestamp} | {info}\n"
                f"   ID: {item.get('history_id')} | Длительность: {duration} | "
                f"Оценка: {item.get('score') if item.get('score') is not None else '—'}"
            )
            lines.append(piece)

        keyboard: List[List[InlineKeyboardButton]] = []
        for item in items:
            payload = self._encode_payload({"history_id": item.get("history_id")})
            row = [
                InlineKeyboardButton(
                    "Расшифровка",
                    callback_data=f"{CALL_LOOKUP_CALLBACK_PREFIX}:transcript:{payload}",
                )
            ]
            if item.get("record_url"):
                row.append(
                    InlineKeyboardButton(
                        "Запись",callback_data=f"{CALL_LOOKUP_CALLBACK_PREFIX}:record:{payload}",
                    )
                )
            keyboard.append(row)

        pagination_row: List[InlineKeyboardButton] = []
        prev_offset = max(0, request.offset - request.limit)
        if request.offset > 0:
            pagination_row.append(
                InlineKeyboardButton(
                    "⬅️ Назад",
                    callback_data=self._encode_page_callback(
                        phone=normalized_phone,
                        period=period,
                        offset=prev_offset,
                        limit=request.limit,
                    ),
                )
            )
        if response["count"] >= request.limit:
            pagination_row.append(
                InlineKeyboardButton(
                    "➡️ Далее",
                    callback_data=self._encode_page_callback(
                        phone=normalized_phone,
                        period=period,
                        offset=request.offset + request.limit,
                        limit=request.limit,
                    ),
                )
            )
        if pagination_row:
            keyboard.append(pagination_row)

        markup = InlineKeyboardMarkup(keyboard) if keyboard else None
        return "\n".join(lines), markup

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
            await message.edit_text(text, reply_markup=markup)
        elif chat_id is not None:
            await context.bot.send_message(
                chat_id=chat_id, text=text, reply_markup=markup
            )

    def _encode_page_callback(
        self,
        *,
        phone: str,
        period: str,
        offset: int,
        limit: int,
    ) -> str:
        payload = {
            "phone": phone,
            "period": period,
            "offset": offset,
            "limit": limit,
        }
        return f"{CALL_LOOKUP_CALLBACK_PREFIX}:page:{self._encode_payload(payload)}"

    @staticmethod
    def _encode_payload(data: Dict[str, Any]) -> str:
        raw = json.dumps(data, separators=(",", ":")).encode("utf-8")
        token = base64.urlsafe_b64encode(raw).decode("utf-8")
        return token.rstrip("=")

    @staticmethod
    def _decode_payload(token: str) -> Dict[str, Any]:
        padding = "=" * (-len(token) % 4)
        data = base64.urlsafe_b64decode(token + padding)
        return json.loads(data.decode("utf-8"))

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
