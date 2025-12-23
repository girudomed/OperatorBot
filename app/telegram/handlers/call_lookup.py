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
import os
from io import BytesIO
import asyncio
from contextlib import asynccontextmanager

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Message, Update, User
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    CallbackContext,
    CommandHandler,
    MessageHandler,
    filters,
)

from app.services.call_lookup import CallLookupService
from app.services.yandex import YandexDiskCache, YandexDiskClient, YandexDiskRecording
from app.telegram.middlewares.permissions import PermissionsManager
from app.telegram.utils.messages import safe_edit_message
from app.telegram.utils.callback_data import AdminCB
from app.telegram.utils.callback_lm import LMCB
from app.logging_config import get_watchdog_logger
from app.telegram.utils.state import reset_feature_states
from app.utils.error_handlers import log_async_exceptions
from app.telegram.utils.logging import describe_user
from app.telegram.utils.admin_registry import register_admin_callback_handler

CLOCK_EMOJI = "🕒"
PHONE_EMOJI = "📱"
TRANSCRIPT_PREVIEW_LIMIT = 2500
ANALYSIS_CHUNK_LIMIT = 3500
CALL_LOOKUP_COMMAND = "call_lookup"
CALL_LOOKUP_PERMISSION = "call_lookup"
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
    yandex_disk_client: Optional[YandexDiskClient] = None,
    yandex_disk_cache: Optional["YandexDiskCache"] = None,
) -> None:
    """
    Регистрирует обработчики команды /call_lookup и её callback-кнопок.
    """
    handler = _CallLookupHandlers(service, permissions_manager, yandex_disk_client, yandex_disk_cache)
    application.bot_data["call_lookup_handler"] = handler
    register_admin_callback_handler(application, AdminCB.CALL_LOOKUP, handler.handle_callback)
    
    application.add_handler(
        CommandHandler(CALL_LOOKUP_COMMAND, handler.handle_command)
    )
    application.add_handler(
        CommandHandler("reindex", handler.handle_reindex)
    )
    def _safe_add_handler(handler_obj):
        try:
            application.add_handler(handler_obj, group=0)
        except TypeError as exc:
            if "unexpected keyword argument 'group'" in str(exc):
                logger.warning(
                    "[CALL_LOOKUP] PTB version does not support grouped handlers. "
                    "Falling back to default group. Details: %s",
                    exc,
                )
                application.add_handler(handler_obj)
            else:
                raise

    _safe_add_handler(
        MessageHandler(
            filters.Regex(r"^@\S+\s+/call_lookup"),
            handler.handle_mention_command,
        )
    )
    _safe_add_handler(
        MessageHandler(
            filters.Regex(r"(?i).*поиск\s+звонк(?:а|ов).*"),
            handler.handle_menu_button,
        )
    )
    _safe_add_handler(
        MessageHandler(
            filters.Regex(r"(?i).*расшифровк"),
            handler.handle_menu_button,
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
        yandex_disk_client: Optional[YandexDiskClient] = None,
        yandex_disk_cache: Optional[YandexDiskCache] = None,
    ):
        self.service = service
        self.permissions_manager = permissions_manager
        self.yandex_disk_client = yandex_disk_client
        self.yandex_disk_cache = yandex_disk_cache
        self._error_reply = (
            "Не удалось выполнить поиск, ошибка конфигурации БД."
        )
        self._pending_key = "call_lookup_pending"
        self._last_request_key = "call_lookup_last_request"
        self._busy_key = "call_lookup_busy"
        self._recordings_key = "call_lookup_recordings"
        self._download_locks: Dict[str, asyncio.Lock] = {}
        self._db_semaphore = asyncio.Semaphore(
            max(1, int(os.getenv("CALL_LOOKUP_DB_CONCURRENCY", "5") or 5))
        )
        self._yandex_semaphore = asyncio.Semaphore(
            max(1, int(os.getenv("CALL_LOOKUP_YANDEX_CONCURRENCY", "3") or 3))
        )
        self._call_details_key = "call_lookup_last_details"
        self._analysis_chunks_key = "call_lookup_analysis_chunks"

    async def _send_usage_hint(
        self,
        message: Message,
        context: CallbackContext,
        *,
        default_period: str = "monthly",
    ) -> None:
        """Показ подсказки по использованию поиска."""
        if not message:
            return
        await self._prompt_lookup_start(
            context,
            message.chat_id,
            message.from_user,
            default_period=default_period,
        )

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

    @staticmethod
    def _mask_phone(value: Optional[str]) -> str:
        digits = re.sub(r"\D", "", value or "")
        if not digits:
            return "—"
        masked = "*" * max(0, len(digits) - 4) + digits[-4:]
        if value and value.strip().startswith("+"):
            masked = "+" + masked.lstrip("*")
        return masked

    def _pending_storage_key(self, chat_id: int) -> str:
        return f"{self._pending_key}:{chat_id}"

    def _last_request_storage_key(self, chat_id: int) -> str:
        return f"{self._last_request_key}:{chat_id}"

    def _recordings_storage_key(self, chat_id: int) -> str:
        return f"{self._recordings_key}:{chat_id}"

    def _call_details_storage_key(self, chat_id: int) -> str:
        return f"{self._call_details_key}:{chat_id}"

    def _analysis_storage_key(self, chat_id: int) -> str:
        return f"{self._analysis_chunks_key}:{chat_id}"

    def _resolve_chat_id(self, update: Update, fallback_user: Optional[User]) -> int:
        if update.effective_chat:
            return update.effective_chat.id
        query = update.callback_query
        if query and query.message:
            return query.message.chat_id
        return fallback_user.id if fallback_user else 0

    def _remember_request(
        self, context: CallbackContext, chat_id: int, request: _LookupRequest
    ) -> None:
        context.chat_data[self._last_request_storage_key(chat_id)] = {
            "phone": request.phone,
            "period": request.period,
            "limit": request.limit,
        }

    def _remember_recordings(
        self,
        context: CallbackContext,
        chat_id: int,
        items: Optional[List[Dict[str, Any]]],
    ) -> None:
        mapping: Dict[str, str] = {}
        for item in items or []:
            history_id = item.get("history_id")
            recording_id = item.get("recording_id")
            if history_id and recording_id:
                mapping[str(history_id)] = str(recording_id)
        context.chat_data[self._recordings_storage_key(chat_id)] = mapping

    def _store_analysis_chunks(
        self,
        context: CallbackContext,
        chat_id: int,
        history_id: int,
        chunks: List[str],
    ) -> None:
        if not chunks:
            return
        storage = context.chat_data.setdefault(self._analysis_storage_key(chat_id), {})
        storage[str(history_id)] = list(chunks)

    def _pop_next_analysis_chunk(
        self,
        context: CallbackContext,
        chat_id: int,
        history_id: int,
    ) -> Tuple[Optional[str], bool]:
        storage = context.chat_data.get(self._analysis_storage_key(chat_id))
        if not storage:
            return None, False
        queue = storage.get(str(history_id))
        if not queue:
            return None, False
        next_chunk = queue.pop(0)
        has_more = bool(queue)
        if has_more:
            storage[str(history_id)] = queue
        else:
            storage.pop(str(history_id), None)
            if not storage:
                context.chat_data.pop(self._analysis_storage_key(chat_id), None)
        return next_chunk, has_more

    def _clear_analysis_chunks(
        self,
        context: CallbackContext,
        chat_id: int,
        history_id: Optional[int] = None,
    ) -> None:
        if history_id is None:
            context.chat_data.pop(self._analysis_storage_key(chat_id), None)
            return
        storage = context.chat_data.get(self._analysis_storage_key(chat_id))
        if not storage:
            return
        storage.pop(str(history_id), None)
        if not storage:
            context.chat_data.pop(self._analysis_storage_key(chat_id), None)
        logger.debug(
            "[CALL_LOOKUP] Обновлён кеш recording_id для chat_id=%s (%d элементов)",
            chat_id,
            len(mapping),
        )

    def _get_cached_recording_id(
        self,
        context: CallbackContext,
        chat_id: int,
        history_id: int,
    ) -> Optional[str]:
        storage = context.chat_data.get(self._recordings_storage_key(chat_id))
        if not isinstance(storage, dict):
            return None
        return storage.get(str(history_id))

    def _build_back_keyboard(
        self,
        context: CallbackContext,
        chat_id: int,
        user: Optional[User],
    ) -> InlineKeyboardMarkup:
        request = self._restore_request(context, chat_id)
        if request:
            return InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "⬅️ Назад",
                            callback_data=self._encode_page_callback(offset=request.offset),
                        )
                    ]
                ]
            )
        return self._lookup_menu_keyboard(
            user.id if user else None,
            user.username if user else None,
        )

    def _call_actions_keyboard(
        self,
        history_id: int,
        *,
        transcript_truncated: bool,
        origin: Optional[str] = None,
        origin_context: Optional[str] = None,
    ) -> InlineKeyboardMarkup:
        rows: List[List[InlineKeyboardButton]] = []
        action_row: List[InlineKeyboardButton] = []
        if transcript_truncated:
            action_row.append(
                InlineKeyboardButton(
                    "📄 Показать полностью",
                    callback_data=AdminCB.create(AdminCB.CALL, "full", history_id),
                )
            )
        if action_row:
            rows.append(action_row)
        
        rows.append([
            InlineKeyboardButton(
                "📊 LM Аналитика",
                callback_data=LMCB.create(LMCB.SUMMARY, history_id)
            )
        ])
        
        back_button = self._build_back_button(history_id, origin, origin_context)
        if back_button:
            rows.append([back_button])
        return InlineKeyboardMarkup(rows)

    def _build_back_button(
        self,
        history_id: int,
        origin: Optional[str],
        origin_context: Optional[str],
    ) -> Optional[InlineKeyboardButton]:
        if origin == "lm":
            target_context = origin_context if origin_context not in (None, "none") else None
            callback = LMCB.create(LMCB.ACTION_SUMMARY, history_id, target_context or "")
            return InlineKeyboardButton("⬅️ Назад в LM", callback_data=callback)
        return InlineKeyboardButton(
            "⬅️ Назад к списку",
            callback_data=AdminCB.create(AdminCB.CALL, "back", history_id),
        )

    def _call_card_keyboard(
        self,
        history_id: int,
        details: Dict[str, Any],
    ) -> InlineKeyboardMarkup:
        rows: List[List[InlineKeyboardButton]] = []
        action_row: List[InlineKeyboardButton] = []
        has_audio = bool(details.get("recording_id") or details.get("record_url"))
        has_transcript = bool(details.get("transcript"))
        if has_audio or has_transcript:
            action_row.append(
                InlineKeyboardButton(
                    "🎧 Аудио и текст",
                    callback_data=AdminCB.create(AdminCB.CALL, "bundle", history_id),
                )
            )
            rows.append(action_row)
        if details.get("operator_result"):
            rows.append(
                [
                    InlineKeyboardButton(
                        "🧠 Анализ работы",
                        callback_data=AdminCB.create(AdminCB.CALL, "analysis", history_id),
                    )
                ]
            )
        rows.append(
            [
                InlineKeyboardButton(
                    "⬅️ Назад к списку",
                    callback_data=AdminCB.create(AdminCB.CALL, "back", history_id),
                )
            ]
        )
        return InlineKeyboardMarkup(rows)

    async def _get_cached_path(self, recording_id: str) -> Optional[str]:
        if not self.yandex_disk_cache:
            return None
        return await self.yandex_disk_cache.get_path(recording_id)

    async def _cache_path(self, recording_id: str, path: Optional[str]) -> None:
        if not self.yandex_disk_cache or not path:
            return
        await self.yandex_disk_cache.save_path(recording_id, path)

    def _store_call_details(
        self,
        context: CallbackContext,
        chat_id: int,
        history_id: int,
        details: Dict[str, Any],
    ) -> None:
        context.chat_data[self._call_details_storage_key(chat_id)] = {
            "history_id": history_id,
            "details": details,
        }

    def _load_call_details(
        self,
        context: CallbackContext,
        chat_id: int,
        history_id: int,
    ) -> Optional[Dict[str, Any]]:
        payload = context.chat_data.get(self._call_details_storage_key(chat_id))
        if not isinstance(payload, dict):
            return None
        if payload.get("history_id") != history_id:
            return None
        details = payload.get("details")
        if isinstance(details, dict):
            return details
        return None

    @asynccontextmanager
    async def _lock_recording(self, recording_id: str):
        lock = self._download_locks.setdefault(recording_id, asyncio.Lock())
        await lock.acquire()
        try:
            yield
        finally:
            lock.release()
            if not lock.locked():
                self._download_locks.pop(recording_id, None)

    @asynccontextmanager
    async def _limit_db_load(self):
        await self._db_semaphore.acquire()
        try:
            yield
        finally:
            self._db_semaphore.release()

    @asynccontextmanager
    async def _limit_yandex_load(self):
        await self._yandex_semaphore.acquire()
        try:
            yield
        finally:
            self._yandex_semaphore.release()

    def _sync_recording_id_with_cache(
        self,
        context: CallbackContext,
        chat_id: int,
        history_id: int,
        details: Optional[Dict[str, Any]],
    ) -> Optional[str]:
        cached = self._get_cached_recording_id(context, chat_id, history_id)
        current = details.get("recording_id") if details else None
        if cached and current and cached != current:
            logger.warning(
                "[CALL_LOOKUP] recording_id mismatch для history_id=%s: db=%s, ui=%s — используем версию из списка.",
                history_id,
                current,
                cached,
            )
            current = cached
        elif not current and cached:
            current = cached
        if details is not None and current and not details.get("recording_id"):
            details["recording_id"] = current
        return current

    def _restore_request(
        self,
        context: CallbackContext,
        chat_id: int,
        *,
        offset: int = 0,
    ) -> Optional[_LookupRequest]:
        payload = context.chat_data.get(self._last_request_storage_key(chat_id))
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

    async def _safe_send_document(
        self,
        context: CallbackContext,
        chat_id: int,
        recording: YandexDiskRecording,
        *,
        caption: Optional[str] = None,
    ) -> Optional[Message]:
        file_obj = BytesIO(recording.content)
        file_obj.name = recording.filename
        try:
            return await context.bot.send_document(
                chat_id=chat_id,
                document=file_obj,
                filename=recording.filename,
                caption=caption,
            )
        except BadRequest as exc:
            logger.warning("Не удалось отправить запись: %s", exc, exc_info=True)
            return None

    async def _send_cached_file(
        self,
        context: CallbackContext,
        chat_id: int,
        recording_id: str,
    ) -> bool:
        if not self.yandex_disk_cache:
            return False
        file_id = await self.yandex_disk_cache.get_file_id(recording_id)
        if not file_id:
            return False
        try:
            await context.bot.send_document(chat_id=chat_id, document=file_id)
            logger.info(
                "[CALL_LOOKUP] Отправлена запись %s из Telegram-кэша.",
                recording_id,
            )
            return True
        except BadRequest as exc:
            logger.warning(
                "Не удалось отправить cached file_id для %s: %s",
                recording_id,
                exc,
            )
            await self.yandex_disk_cache.delete_file_id(recording_id)
            return False

    def _build_result_message(
        self,
        *,
        response: Dict[str, Any],
        period: str,
        request: _LookupRequest,
    ) -> Tuple[str, InlineKeyboardMarkup]:
        items: List[Dict[str, Any]] = response.get("items") or []
        offset = max(0, int(request.offset or 0))
        limit = max(1, int(request.limit or self.service.DEFAULT_LIMIT))
        phone_display = response.get("normalized_phone") or request.phone or "—"

        lines = [
            "📋 Результаты поиска звонков",
            f"Номер: {phone_display}",
            f"Период: {self._human_period_name(period)}",
        ]

        keyboard_rows: List[List[InlineKeyboardButton]] = []

        if not items:
            lines.append("")
            lines.append("Совпадений не найдено.")
            keyboard_rows.append(
                [
                    InlineKeyboardButton(
                        "📅 Выбрать другой период",
                        callback_data=AdminCB.create(AdminCB.CALL_LOOKUP, "intro", period),
                    )
                ]
            )
            return "\n".join(lines), InlineKeyboardMarkup(keyboard_rows)

        for idx, item in enumerate(items, start=1):
            history_id = item.get("history_id")
            call_time = self._format_datetime(item.get("call_time"))
            duration = self._format_duration(item.get("talk_duration"))
            caller = item.get("caller_info") or item.get("caller_number") or "—"
            called = item.get("called_info") or item.get("called_number") or "—"
            recording_id = item.get("recording_id")
            score = item.get("score")
            score_display = (
                f"{score:.1f}" if isinstance(score, (int, float)) else (score if score is not None else "—")
            )

            lines.append("")
            lines.append(f"{offset + idx}. #{history_id or '—'}")
            header_parts = [
                f"🕒 {call_time}",
                f"⏱ {duration}",
                f"⭐ {score_display}",
            ]
            lines.append(" | ".join(header_parts))
            lines.append(f"👤 Кто звонил: {caller}")
            lines.append(f"🏢 Кому звонили: {called}")
            if recording_id:
                lines.append(f"🎧 recording_id: {recording_id}")

            if history_id:
                keyboard_rows.append(
                    [
                        InlineKeyboardButton(
                            f"📝 #{history_id}",
                            callback_data=AdminCB.create(AdminCB.CALL, "open", history_id),
                        )
                    ]
                )

        nav_row: List[InlineKeyboardButton] = []
        if offset > 0:
            prev_offset = max(0, offset - limit)
            nav_row.append(
                InlineKeyboardButton(
                    "⬅️ Назад",
                    callback_data=self._encode_page_callback(offset=prev_offset),
                )
            )
        if len(items) >= limit:
            next_offset = offset + limit
            nav_row.append(
                InlineKeyboardButton(
                    "➡️ Далее",
                    callback_data=self._encode_page_callback(offset=next_offset),
                )
            )
        if nav_row:
            keyboard_rows.append(nav_row)

        keyboard_rows.append(
            [
                InlineKeyboardButton(
                    "📅 Сменить период",
                    callback_data=AdminCB.create(AdminCB.CALL_LOOKUP, "intro", period),
                )
            ]
        )
        keyboard_rows.append(
            [
                InlineKeyboardButton(
                    "⬅️ Назад",
                    callback_data=AdminCB.create(AdminCB.CALL_LOOKUP, "cancel"),
                )
            ]
        )

        return "\n".join(lines), InlineKeyboardMarkup(keyboard_rows)

    @log_async_exceptions
    async def handle_command(self, update: Update, context: CallbackContext) -> None:
        message = update.effective_message
        user = update.effective_user
        if not message or not user:
            return

        chat_id = message.chat_id
        reset_feature_states(context, chat_id)

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
            await self._send_usage_hint(message, context)
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

        if not await self._acquire_busy(context, notifier=message):
            return
        try:
            logger.info(
                "Пользователь %s выполняет /call_lookup (phone=%s, period=%s)",
                describe_user(user),
                self._mask_phone(phone),
                period,
            )
            try:
                async with self._limit_db_load():
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
            except Exception as exc:
                # Непредвиденная ошибка — логируем и пробрасываем выше.
                logger.exception(
                    "Unexpected error while executing /call_lookup for %s",
                    describe_user(user),
                    exc_info=True,
                )
                raise

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
            self._remember_request(context, message.chat_id, request)
            self._remember_recordings(context, message.chat_id, response.get("items"))
            logger.info(
                "Пользователь %s получил %s звонков по запросу /call_lookup",
                describe_user(user),
                response.get("count"),
            )
        finally:
            self._release_busy(context)

    @log_async_exceptions
    async def handle_reindex(self, update: Update, context: CallbackContext) -> None:
        message = update.effective_message
        user = update.effective_user
        if not message or not user:
            return
        if not await self._is_allowed(user.id, user.username):
            await self._safe_reply_text(message, "Команда доступна только администраторам.")
            return
        if not self.yandex_disk_cache or not self.yandex_disk_client:
            await self._safe_reply_text(message, "Индексация недоступна (не настроен Redis или Яндекс.Диск).")
            return
        await self._safe_reply_text(message, "Запускаю переиндексацию /mango_data ...")
        try:
            async with self._limit_yandex_load():
                updated = await self.yandex_disk_cache.refresh_index(self.yandex_disk_client)
        except Exception as exc:
            logger.exception("Ошибка индексации /mango_data: %s", exc)
            await self._safe_reply_text(message, f"Ошибка индексации: {exc}")
            return
        await self._safe_reply_text(
            message,
            f"Готово. Обновлено записей: {updated}",
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
        """Реакция на кнопку главного меню «Поиск звонков»."""
        message = update.effective_message
        user = update.effective_user
        if not message or not user:
            return
        logger.info(
            "[CALL_LOOKUP] Пользователь %s нажал кнопку «🔍 Поиск звонков»",
            describe_user(user),
        )

        if not await self._is_allowed(user.id, user.username):
            await self._safe_reply_text(
                message,
                "Команда доступна только старшим администраторам. "
                "Обратитесь к администратору для получения доступа.",
            )
            return

        await self._send_usage_hint(message, context)
    
    async def handle_callback(self, update: Update, context: CallbackContext) -> None:
        query = update.callback_query
        user = update.effective_user
        if not query or not user:
            return

        # Parse AdminCB: adm:cl:sub_action:args...
        action_type, args = AdminCB.parse(query.data)
        if action_type != AdminCB.CALL_LOOKUP or not args:
            return

        await query.answer()

        sub_action = args[0]
        params = args[1:]
        
        chat_id = query.message.chat_id if query.message else user.id

        if not await self._is_allowed(user.id, user.username):
            await safe_edit_message(query, text="Доступ запрещён.")
            logger.warning(
                "Call lookup callback отклонён для %s (sub=%s)",
                describe_user(user),
                sub_action,
            )
            return
        logger.info(
            "Call lookup callback получен: sub=%s user=%s",
            sub_action,
            describe_user(user),
        )

        if sub_action == "intro":
            period = params[0] if params else "monthly"
            reset_feature_states(context, chat_id)
            await self._prompt_lookup_start(
                context,
                chat_id,
                user,
                default_period=period,
            )
            return
        elif sub_action == "ask":
            period = params[0] if params else "monthly"
            context.chat_data[self._pending_storage_key(chat_id)] = {"period": period}
            await self._safe_send_message(
                context,
                chat_id,
                f"Введите номер телефона для поиска звонков ({self._human_period_name(period)}).",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "⬅️ Назад",
                                callback_data=AdminCB.create(AdminCB.CALL_LOOKUP, "cancel"),
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
        elif sub_action == "p":
            try:
                offset_value = max(0, int(params[0])) if params else 0
            except ValueError as exc:
                logger.warning("Некорректный offset в callback %s: %s", query.data, exc)
                await query.answer("Некорректный offset", show_alert=True)
                return
            restored = self._restore_request(context, chat_id, offset=offset_value)
            if not restored:
                await query.answer("Запрос устарел, выполните поиск заново", show_alert=True)
                return
            request = restored
            logger.info(
                "Call lookup пагинация (period=%s, offset=%s) пользователем %s",
                request.period,
                request.offset,
                describe_user(user),
            )
            if not await self._acquire_busy(context, notifier=query):
                return
            try:
                async with self._limit_db_load():
                    response = await self.service.lookup_calls(
                        phone=request.phone,
                        period=request.period,
                        offset=request.offset,
                        limit=request.limit,
                        requesting_user_id=user.id,
                    )
            except Exception as exc:
                # Непредвиденная ошибка — логируем и пробрасываем выше после освобождения busy-статуса.
                logger.exception(
                    "Unexpected error during call_lookup pagination for %s",
                    describe_user(user),
                    exc_info=True,
                )
                self._release_busy(context)
                raise
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
            self._remember_request(context, chat_id, request)
            self._remember_recordings(context, chat_id, response.get("items"))
            self._release_busy(context)
        elif sub_action == "t":
            try:
                history_id = int(params[0]) if params else 0
            except ValueError as exc:
                logger.warning("Некорректный history_id (action=t): %s", exc)
                await query.answer("Некорректный ID", show_alert=True)
                return
            if not await self._acquire_busy(context, notifier=query):
                return
            try:
                async with self._limit_db_load():
                    details = await self.service.fetch_call_details(history_id)
            except Exception as exc:
                # Непредвиденная ошибка при загрузке расшифровки — логируем и пробрасываем.
                logger.exception(
                    "Unexpected error loading transcript %s for %s",
                    history_id,
                    describe_user(user),
                    exc_info=True,
                )
                self._release_busy(context)
                raise
            details_payload = details or {}
            self._sync_recording_id_with_cache(context, chat_id, history_id, details_payload)
            transcript = details_payload.get("transcript")
            text = self._format_transcript_details(details_payload, transcript)
            await self._safe_send_message(
                context,
                chat_id,
                text,
                parse_mode="HTML",
                reply_markup=self._build_back_keyboard(context, chat_id, user),
            )
            logger.info(
                "Пользователь %s запросил расшифровку звонка %s",
                describe_user(user),
                history_id,
            )
            self._release_busy(context)
        elif sub_action == "r":
            try:
                history_id = int(params[0]) if params else 0
            except ValueError as exc:
                logger.warning("Некорректный history_id (action=r): %s", exc)
                await query.answer("Некорректный ID", show_alert=True)
                return
            if not await self._acquire_busy(context, notifier=query):
                return
            try:
                details = await self.service.fetch_call_details(history_id)
            except Exception as exc:
                # Непредвиденная ошибка при загрузке записи — логируем и пробрасываем.
                logger.exception(
                    "Unexpected error loading recording %s for %s",
                    history_id,
                    describe_user(user),
                    exc_info=True,
                )
                self._release_busy(context)
                raise
            details_payload = details or {}
            selected_recording_id = self._sync_recording_id_with_cache(
                context,
                chat_id,
                history_id,
                details_payload,
            )
            record_url = details_payload.get("record_url")
            recording_id = selected_recording_id
            downloaded_record: Optional[YandexDiskRecording] = None
            cache_served = False
            if recording_id:
                cache_served = await self._send_cached_file(context, chat_id, recording_id)
            if cache_served:
                self._release_busy(context)
                return
            if recording_id and self.yandex_disk_client:
                async with self._lock_recording(recording_id):
                    async with self._limit_yandex_load():
                        cache_served = await self._send_cached_file(context, chat_id, recording_id)
                        if cache_served:
                            self._release_busy(context)
                            return
                        cached_path = await self._get_cached_path(recording_id)
                        if cached_path:
                            downloaded_record = await self.yandex_disk_client.download_by_path(cached_path)
                            if not downloaded_record and self.yandex_disk_cache:
                                await self.yandex_disk_cache.delete_path(recording_id)
                        if not downloaded_record:
                            try:
                                downloaded_record = await self.yandex_disk_client.download_recording(
                                    recording_id,
                                    call_time=details_payload.get("call_time"),
                                    phone_candidates=[
                                        details_payload.get("caller_number"),
                                        details_payload.get("caller_info"),
                                        details_payload.get("called_number"),
                                        details_payload.get("called_info"),
                                    ],
                                )
                            except Exception as exc:
                                logger.exception(
                                    "Не удалось загрузить запись %s из Яндекс.Диска: %s",
                                    recording_id,
                                    exc,
                                    exc_info=True,
                                )
            if downloaded_record:
                caption = self._format_record_message(history_id, details_payload)
                message = await self._safe_send_document(
                    context,
                    chat_id,
                    downloaded_record,
                    caption=caption,
                )
                if (
                    message
                    and message.document
                    and recording_id
                    and self.yandex_disk_cache
                ):
                    await self.yandex_disk_cache.save_file_id(
                        recording_id,
                        message.document.file_id,
                    )
                if recording_id:
                    await self._cache_path(recording_id, downloaded_record.path)
                logger.info(
                    "Пользователь %s получил запись %s (filename=%s) из Яндекс.Диска",
                    describe_user(user),
                    recording_id,
                    downloaded_record.filename,
                )
            elif record_url:
                logger.warning(
                    "[CALL_LOOKUP] Запись %s не найдена на Диске, отправляем ссылку record_url.",
                    recording_id or "—",
                )
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
                logger.warning(
                    "[CALL_LOOKUP] Запись не найдена ни на Диске, ни в record_url (history_id=%s, recording_id=%s)",
                    history_id,
                    recording_id or "—",
                )
                await self._safe_send_message(
                    context,
                    chat_id,
                    "Запись недоступна или не найдена на Яндекс.Диске.",
                )
            self._release_busy(context)
        elif sub_action == "cancel":
            context.chat_data.pop(self._pending_storage_key(chat_id), None)
            context.chat_data.pop(self._recordings_storage_key(chat_id), None)
            context.chat_data.pop(self._call_details_storage_key(chat_id), None)
            self._clear_analysis_chunks(context, chat_id)
            await self._safe_send_message(
                context,
                chat_id,
                "🔙 Поиск звонков остановлен. Выберите период, чтобы начать заново, или вернитесь назад.",
                reply_markup=self._lookup_menu_keyboard(user.id, user.username),
            )

    @log_async_exceptions
    async def handle_call_callback(
        self,
        update: Update,
        context: CallbackContext,
        args: List[str],
    ) -> None:
        query = update.callback_query
        user = update.effective_user
        if not query or not user or not args:
            return
        sub_action = args[0]
        try:
            history_id = int(args[1]) if len(args) > 1 else 0
        except ValueError:
            history_id = 0
        origin = args[2] if len(args) > 2 else None
        origin_context = args[3] if len(args) > 3 else None
        if history_id <= 0:
            await query.answer("Некорректный ID", show_alert=True)
            return
        if sub_action == "open":
            if not await self._acquire_busy(context, notifier=query):
                return
            try:
                await self._handle_call_open(update, context, history_id, user)
            finally:
                self._release_busy(context)
        elif sub_action == "audio":
            if not await self._acquire_busy(context, notifier=query):
                return
            try:
                await self._handle_call_audio_retry(update, context, history_id, user)
            finally:
                self._release_busy(context)
        elif sub_action == "bundle":
            if not await self._acquire_busy(context, notifier=query):
                return
            try:
                await self._handle_call_bundle(update, context, history_id, user, origin=origin, origin_context=origin_context)
            finally:
                self._release_busy(context)
        elif sub_action in ("full", "full_transcript"):
            await self._handle_call_full_transcript(update, context, history_id, origin=origin, origin_context=origin_context)
        elif sub_action == "transcript":
            await self._handle_call_transcript_preview(update, context, history_id, user, origin=origin, origin_context=origin_context)
        elif sub_action == "analysis":
            await self._handle_call_analysis(update, context, history_id, user)
        elif sub_action == "analysis_more":
            await self._handle_call_analysis_more(update, context, history_id, user)
        elif sub_action == "back":
            await self._handle_call_back(update, context)

    async def _handle_call_open(
        self,
        update: Update,
        context: CallbackContext,
        history_id: int,
        user: User,
    ) -> None:
        chat_id = self._resolve_chat_id(update, user)
        try:
            async with self._limit_db_load():
                details = await self.service.fetch_call_details(history_id)
        except Exception as exc:
            logger.exception(
                "Unexpected error loading call %s for %s",
                history_id,
                describe_user(user),
                exc_info=True,
            )
            await self._safe_send_message(context, chat_id, "Ошибка доступа к базе. Попробуйте позже.")
            return
        if not details:
            await self._safe_send_message(context, chat_id, "Звонок не найден или нет доступа.")
            return
        logger.info(
            "[CALL_LOOKUP] open_call history_id=%s user=%s",
            history_id,
            describe_user(user),
        )
        details_payload = details or {}
        self._sync_recording_id_with_cache(
            context,
            chat_id,
            history_id,
            details_payload,
        )
        await self._send_call_card(context, chat_id, details_payload)
        self._store_call_details(context, chat_id, history_id, details_payload)
        context.user_data["lm:last_history_id"] = history_id
        self._clear_analysis_chunks(context, chat_id, history_id)

    async def _handle_call_audio_retry(
        self,
        update: Update,
        context: CallbackContext,
        history_id: int,
        user: User,
    ) -> None:
        chat_id = self._resolve_chat_id(update, user)
        details = await self._ensure_call_details(context, chat_id, history_id)
        if not details:
            await self._safe_send_message(context, chat_id, "Звонок не найден.")
            return
        await self._send_call_audio(context, chat_id, history_id, details, user)

    async def _handle_call_bundle(
        self,
        update: Update,
        context: CallbackContext,
        history_id: int,
        user: User,
        origin: Optional[str] = None,
        origin_context: Optional[str] = None,
    ) -> None:
        chat_id = self._resolve_chat_id(update, user)
        details = await self._ensure_call_details(context, chat_id, history_id)
        if not details:
            await self._safe_send_message(context, chat_id, "Звонок не найден.")
            return
        await self._send_call_transcript(
            context,
            chat_id,
            history_id,
            details,
            user,
            origin=origin,
            origin_context=origin_context,
        )
        await self._send_call_audio(context, chat_id, history_id, details, user)

    async def _handle_call_transcript_preview(
        self,
        update: Update,
        context: CallbackContext,
        history_id: int,
        user: User,
        origin: Optional[str] = None,
        origin_context: Optional[str] = None,
    ) -> None:
        chat_id = self._resolve_chat_id(update, user)
        details = await self._ensure_call_details(context, chat_id, history_id)
        if not details:
            await self._safe_send_message(context, chat_id, "Звонок не найден.")
            return
        await self._send_call_transcript(
            context,
            chat_id,
            history_id,
            details,
            user,
            origin=origin,
            origin_context=origin_context,
        )

    async def _handle_call_full_transcript(
        self,
        update: Update,
        context: CallbackContext,
        history_id: int,
        origin: Optional[str] = None,
        origin_context: Optional[str] = None,
    ) -> None:
        chat_id = self._resolve_chat_id(update, update.effective_user)
        details = await self._ensure_call_details(context, chat_id, history_id)
        transcript = details.get("transcript") if details else None
        if not transcript:
            await self._safe_send_message(context, chat_id, "Полная расшифровка отсутствует.")
            return
        await self._send_full_transcript(
            context,
            chat_id,
            history_id,
            transcript,
            origin=origin,
            origin_context=origin_context,
        )
        logger.info(
            "[CALL_LOOKUP] transcript_status=sent_full history_id=%s",
            history_id,
        )

    async def _handle_call_analysis(
        self,
        update: Update,
        context: CallbackContext,
        history_id: int,
        user: User,
    ) -> None:
        chat_id = self._resolve_chat_id(update, user)
        details = await self._ensure_call_details(context, chat_id, history_id)
        if not details:
            await self._safe_send_message(context, chat_id, "Звонок не найден.")
            return
        analysis = details.get("operator_result")
        lm_metrics = details.get("lm_metrics") or []
        if not analysis and not lm_metrics:
            await self._safe_send_message(context, chat_id, "Анализ недоступен для этого звонка.")
            return
        lines = ["🧠 <b>Анализ работы</b>"]
        if analysis:
            lines.append("")
            lines.append(analysis)
        metric_lines = self._format_metrics(lm_metrics)
        if metric_lines:
            lines.append("")
            lines.append("<b>Метрики:</b>")
            lines.extend(metric_lines)
        chunks = self._split_text_chunks("\n".join(lines), ANALYSIS_CHUNK_LIMIT)
        first_chunk = chunks[0]
        remainder = chunks[1:]
        reply_markup = None
        if remainder:
            self._store_analysis_chunks(context, chat_id, history_id, remainder)
            reply_markup = InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "📄 Показать больше",
                            callback_data=AdminCB.create(AdminCB.CALL, "analysis_more", history_id),
                        )
                    ]
                ]
            )
        else:
            self._clear_analysis_chunks(context, chat_id, history_id)
        await self._safe_send_message(
            context,
            chat_id,
            first_chunk,
            parse_mode="HTML",
            reply_markup=reply_markup,
        )
        logger.info(
            "[CALL_LOOKUP] analysis_sent history_id=%s user=%s chunks=%s",
            history_id,
            describe_user(user),
            1 + len(remainder),
        )

    async def _handle_call_analysis_more(
        self,
        update: Update,
        context: CallbackContext,
        history_id: int,
        user: User,
    ) -> None:
        chat_id = self._resolve_chat_id(update, user)
        chunk, has_more = self._pop_next_analysis_chunk(context, chat_id, history_id)
        if not chunk:
            await self._safe_send_message(context, chat_id, "Дополнительный текст анализа отсутствует.")
            return
        reply_markup = None
        if has_more:
            reply_markup = InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "📄 Показать больше",
                            callback_data=AdminCB.create(AdminCB.CALL, "analysis_more", history_id),
                        )
                    ]
                ]
            )
        await self._safe_send_message(
            context,
            chat_id,
            chunk,
            parse_mode="HTML",
            reply_markup=reply_markup,
        )
        if not has_more:
            self._clear_analysis_chunks(context, chat_id, history_id)

    async def _handle_call_back(
        self,
        update: Update,
        context: CallbackContext,
    ) -> None:
        chat_id = self._resolve_chat_id(update, update.effective_user)
        request = self._restore_request(context, chat_id)
        if not request:
            await self._safe_send_message(
                context,
                chat_id,
                "Запрос устарел, выполните поиск заново.",
            )
            return
        try:
            async with self._limit_db_load():
                response = await self.service.lookup_calls(
                    phone=request.phone,
                    period=request.period,
                    offset=request.offset,
                    limit=request.limit,
                    requesting_user_id=update.effective_user.id if update.effective_user else None,
                )
        except Exception as exc:
            logger.exception("Ошибка возврата к списку звонков: %s", exc, exc_info=True)
            await self._safe_send_message(
                context,
                chat_id,
                "Ошибка доступа к базе. Повторите позже.",
            )
            return
        text, markup = self._build_result_message(
            response=response,
            period=request.period,
            request=request,
        )
        await self._safe_send_message(
            context,
            chat_id,
            text,
            reply_markup=markup,
        )
        self._remember_request(context, chat_id, request)
        self._remember_recordings(context, chat_id, response.get("items"))

    async def _ensure_call_details(
        self,
        context: CallbackContext,
        chat_id: int,
        history_id: int,
    ) -> Optional[Dict[str, Any]]:
        cached = self._load_call_details(context, chat_id, history_id)
        if cached:
            return cached
        try:
            async with self._limit_db_load():
                details = await self.service.fetch_call_details(history_id)
        except Exception as exc:
            logger.exception("Ошибка загрузки деталей звонка %s: %s", history_id, exc, exc_info=True)
            return None
        if not details:
            return None
        self._store_call_details(context, chat_id, history_id, details)
        return details
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

        chat_id = message.chat_id
        pending = context.chat_data.get(self._pending_storage_key(chat_id))
        if not pending:
            logger.debug(
                "[CALL_LOOKUP] Игнорируем ввод номера %s — режим не активен (chat_id=%s)",
                describe_user(user),
                chat_id,
            )
            return

        phone_text = (message.text or "").strip()
        if not phone_text:
            await self._safe_reply_text(
                message,
                "Введите номер телефона цифрами.",
            )
            return
            
        # ... validation ...
        
        # NOTE: This method is now called via TextRouter, so we assume pending check is done?
        # TextRouter checks chat_data[call_lookup_pending]. So it IS pending.
        # But we double check just in case.

        if not re.search(r"\d", phone_text):
            return

        period = pending.get("period", "monthly")
        logger.info(
            "[CALL_LOOKUP] Пользователь %s ввёл номер %s (period=%s)",
            describe_user(user),
            phone_text,
            period,
        )
        if not await self._acquire_busy(context, notifier=message):
            return

        try:
            async with self._limit_db_load():
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
        except Exception as exc:
            # Непредвиденная ошибка — логируем, очищаем pending и пробрасываем.
            logger.exception(
                "Unexpected error in interactive call lookup for %s",
                describe_user(user),
                exc_info=True,
            )
            context.chat_data.pop(self._pending_storage_key(chat_id), None)
            raise
        finally:
            self._release_busy(context)

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
            chat_id,
            text,
            reply_markup=markup,
        )
        self._remember_request(context, chat_id, request)
        self._remember_recordings(context, chat_id, response.get("items"))
        context.chat_data.pop(self._pending_storage_key(chat_id), None)

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

    async def _acquire_busy(self, context: CallbackContext, notifier=None) -> bool:
        if context.user_data.get(self._busy_key):
            await self._notify_busy(notifier)
            return False
        context.user_data[self._busy_key] = True
        return True

    def _release_busy(self, context: CallbackContext) -> None:
        context.user_data.pop(self._busy_key, None)

    async def _notify_busy(self, target) -> None:
        if not target:
            return
        if hasattr(target, "answer"):
            await target.answer("Поиск уже выполняется. Подождите.", show_alert=True)
        elif hasattr(target, "reply_text"):
            await target.reply_text("⚠️ Поиск уже выполняется. Дождитесь завершения.")

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
        return f"{AdminCB.PREFIX}:{AdminCB.CALL_LOOKUP}:p:{safe_offset}"

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

    @staticmethod
    def _split_text_chunks(text: str, chunk_size: int) -> List[str]:
        if len(text) <= chunk_size:
            return [text]
        chunks: List[str] = []
        current: List[str] = []
        current_len = 0
        for line in text.splitlines():
            addition = len(line) + (1 if current else 0)
            if current and current_len + addition > chunk_size:
                chunks.append("\n".join(current))
                current = [line]
                current_len = len(line)
            else:
                current.append(line)
                current_len += addition
        if current:
            chunks.append("\n".join(current))
        return chunks or [text]

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
        patient = details.get("caller_number") or details.get("caller_info") or "—"
        call_time = self._format_datetime(details.get("call_time"))
        lines = [
            f"ℹ️ Звонок #{history_id}",
            f"Пациент: {patient}",
            f"Время: {call_time}",
        ]
        if recording_id:
            lines.append(f"recording_id: {recording_id}")
        if record_url:
            lines.append("")
            lines.append(record_url)
        return "\n".join(lines)

    def _format_call_card(self, details: Dict[str, Any]) -> str:
        history_id = details.get("history_id") or "—"
        call_time = self._format_datetime(details.get("call_time"))
        patient = details.get("caller_number") or details.get("caller_info") or "—"
        duration = self._format_duration(details.get("talk_duration"))
        score = details.get("score")
        caller_display = details.get("caller_info") or details.get("caller_number") or "—"
        called_display = details.get("called_info") or details.get("called_number") or "—"
        recording_id = details.get("recording_id")
        lines = [
            f"📞 Звонок #{history_id}",
            f"🕒 {call_time}",
            f"📱 {patient}",
            f"⏱ {duration}",
            f"👤 Кто звонил: {caller_display}",
            f"🏢 Кому звонили: {called_display}",
        ]
        if score is not None:
            lines.append(f"⭐ Score: {score}")
        if recording_id:
            lines.append(f"🎧 recording_id: {recording_id}")
        return "\n".join(lines)

    async def _send_call_card(
        self,
        context: CallbackContext,
        chat_id: int,
        details: Dict[str, Any],
    ) -> None:
        card_text = self._format_call_card(details)
        history_id = int(details.get("history_id") or details.get("id") or 0)
        reply_markup = self._call_card_keyboard(history_id, details)
        await self._safe_send_message(
            context,
            chat_id,
            card_text,
            parse_mode="HTML",
            reply_markup=reply_markup,
        )

    async def _send_call_audio(
        self,
        context: CallbackContext,
        chat_id: int,
        history_id: int,
        details: Dict[str, Any],
        user: User,
    ) -> Tuple[bool, bool]:
        recording_id = details.get("recording_id")
        record_url = details.get("record_url")
        has_audio_source = bool(recording_id or record_url)
        audio_sent = False
        reason = "no_source"
        if recording_id and self.yandex_disk_client:
            downloaded_record: Optional[YandexDiskRecording] = None
            async with self._lock_recording(recording_id):
                async with self._limit_yandex_load():
                    cached_path = await self._get_cached_path(recording_id)
                    if cached_path:
                        downloaded_record = await self.yandex_disk_client.download_by_path(cached_path)
                        if not downloaded_record and self.yandex_disk_cache:
                            await self.yandex_disk_cache.delete_path(recording_id)
                    if not downloaded_record:
                        try:
                            downloaded_record = await self.yandex_disk_client.download_recording(
                                recording_id,
                                call_time=details.get("call_time"),
                                phone_candidates=[
                                    details.get("caller_number"),
                                    details.get("caller_info"),
                                    details.get("called_number"),
                                    details.get("called_info"),
                                ],
                            )
                        except Exception as exc:
                            logger.exception(
                                "Не удалось загрузить запись %s из Яндекс.Диска: %s",
                                recording_id,
                                exc,
                                exc_info=True,
                            )
                            reason = "download_error"
            if downloaded_record:
                caption = self._format_record_message(history_id, details)
                message = await self._safe_send_document(
                    context,
                    chat_id,
                    downloaded_record,
                    caption=caption,
                )
                if message and message.document and self.yandex_disk_cache:
                    await self.yandex_disk_cache.save_file_id(
                        recording_id,
                        message.document.file_id,
                    )
                    await self._cache_path(recording_id, downloaded_record.path)
                audio_sent = message is not None
                reason = "sent"
        if not audio_sent:
            if record_url:
                text = f"🎧 Аудио доступно по ссылке: {record_url}"
                reason = "fallback_url"
            elif has_audio_source:
                text = "🎧 Аудио недоступно: ошибка скачивания."
                reason = "download_failed"
            else:
                text = "🎧 Аудио недоступно для этого звонка."
            await self._safe_send_message(context, chat_id, text)
        logger.info(
            "[CALL_LOOKUP] audio_status=%s reason=%s history_id=%s user=%s",
            "sent" if audio_sent else "missing",
            reason,
            history_id,
            describe_user(user),
        )
        return audio_sent, has_audio_source

    async def _send_call_transcript(
        self,
        context: CallbackContext,
        chat_id: int,
        history_id: int,
        details: Dict[str, Any],
        user: User,
        *,
        origin: Optional[str] = None,
        origin_context: Optional[str] = None,
    ) -> Tuple[str, bool]:
        transcript = details.get("transcript")
        truncated = False
        status = "missing"
        if transcript:
            truncated = len(transcript) > TRANSCRIPT_PREVIEW_LIMIT
            preview = transcript[:TRANSCRIPT_PREVIEW_LIMIT]
            if truncated:
                preview = preview.rstrip()
            text_lines = ["📝 Расшифровка:", preview]
            if truncated:
                text_lines.append("")
                text_lines.append("<i>Текст сокращён. Нажмите «📄 Показать полностью».</i>")
            text = "\n".join(line for line in text_lines if line is not None)
            status = "sent_preview" if truncated else "sent_full"
        else:
            text = "📝 Расшифровка отсутствует."
        reply_markup = self._call_actions_keyboard(
            history_id,
            transcript_truncated=truncated,
            origin=origin,
            origin_context=origin_context,
        )
        await self._safe_send_message(
            context,
            chat_id,
            text,
            reply_markup=reply_markup,
        )
        logger.info(
            "[CALL_LOOKUP] transcript_status=%s history_id=%s user=%s",
            status,
            history_id,
            describe_user(user),
        )
        return status, truncated

    async def _send_full_transcript(
        self,
        context: CallbackContext,
        chat_id: int,
        history_id: int,
        transcript: str,
        *,
        origin: Optional[str] = None,
        origin_context: Optional[str] = None,
    ) -> None:
        chunks = self._split_text(transcript, limit=3800)
        back_button = self._build_back_button(history_id, origin, origin_context)
        for index, chunk in enumerate(chunks):
            reply_markup = None
            if index + 1 < len(chunks):
                reply_markup = InlineKeyboardMarkup(
                    [
                        [
                            back_button
                        ]
                    ]
                )
            else:
                reply_markup = InlineKeyboardMarkup(
                    [
                        [
                            back_button
                        ]
                    ]
                )
            await self._safe_send_message(
                context,
                chat_id,
                f"📝 Полный текст:\n{chunk}",
                reply_markup=reply_markup,
            )

    def _split_text(self, text: str, limit: int) -> List[str]:
        if len(text) <= limit:
            return [text]
        chunks: List[str] = []
        start = 0
        while start < len(text):
            end = min(start + limit, len(text))
            chunks.append(text[start:end])
            start = end
        return chunks

    def _lookup_menu_keyboard(self, user_id: Optional[int], username: Optional[str]) -> InlineKeyboardMarkup:
        buttons = [
            [
                InlineKeyboardButton(
                    "📅 День",
                    callback_data=AdminCB.create(AdminCB.CALL_LOOKUP, "ask", "daily"),
                ),
                InlineKeyboardButton(
                    "📆 Неделя",
                    callback_data=AdminCB.create(AdminCB.CALL_LOOKUP, "ask", "weekly"),
                ),
            ],
            [
                InlineKeyboardButton(
                    "📊 2 недели",
                    callback_data=AdminCB.create(AdminCB.CALL_LOOKUP, "ask", "biweekly"),
                ),
                InlineKeyboardButton(
                    "🗓 Месяц",
                    callback_data=AdminCB.create(AdminCB.CALL_LOOKUP, "ask", "monthly"),
                ),
            ],
            [
                InlineKeyboardButton(
                    "🗃 Полгода",
                    callback_data=AdminCB.create(AdminCB.CALL_LOOKUP, "ask", "half_year"),
                ),
                InlineKeyboardButton(
                    "📁 Год",
                    callback_data=AdminCB.create(AdminCB.CALL_LOOKUP, "ask", "yearly"),
                ),
            ],
        ]
        buttons.append(
            [
                InlineKeyboardButton(
                    "⬅️ В админ-панель",
                    callback_data=AdminCB.create(AdminCB.BACK),
                ),
            ]
        )
        return InlineKeyboardMarkup(buttons)

    def _build_lookup_intro_text(self, period: str) -> str:
        return (
            "🔍 <b>Поиск звонков</b>\n\n"
            "Выберите период на клавиатуре, а затем укажите номер.\n\n"
            "Сначала придёт текстовая расшифровка. "
            "Дождитесь скачивания звонка — это может занять некоторое время."
        )

    async def _prompt_lookup_start(
        self,
        context: CallbackContext,
        chat_id: Optional[int],
        user: Optional[User],
        *,
        default_period: str = "monthly",
    ) -> None:
        if chat_id is None:
            return
        context.chat_data[self._pending_storage_key(chat_id)] = {"period": default_period}
        text = self._build_lookup_intro_text(default_period)
        keyboard = self._lookup_menu_keyboard(
            user.id if user else None,
            user.username if user else None,
        )
        await self._safe_send_message(
            context,
            chat_id,
            text,
            parse_mode="HTML",
            reply_markup=keyboard,
        )

    @staticmethod
    def _human_period_name(period: str) -> str:
        mapping = {
            "daily": "день",
            "weekly": "неделя",
            "biweekly": "две недели",
            "monthly": "месяц",
            "half_year": "полгода",
            "yearly": "год",
            "custom": "указанный период",
        }
        return mapping.get(period, "месяц")
