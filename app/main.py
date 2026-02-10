# Файл: app/main.py

"""
Главный модуль приложения.
"""

from __future__ import annotations

import asyncio
import fcntl
import types
import sys
import logging
import signal
import os
import re
import time
from typing import Callable, Optional
from pathlib import Path
from collections import OrderedDict

import httpx
from telegram import BotCommand, Update
from telegram.error import NetworkError, TelegramError
from telegram.ext import ApplicationBuilder, ContextTypes, MessageHandler, TypeHandler, filters
from telegram.request import HTTPXRequest
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
from app.error_policy import resolve_user_message, should_alert
from app.errors import AppError, TelegramIntegrationError
from app.logging_config import (
    get_trace_id,
    install_polling_noise_filter,
    is_polling_noise_record,
    setup_watchdog,
    get_watchdog_logger,
)

# Импортируем error handlers для установки глобальных обработчиков
from app.utils.error_handlers import (
    install_loop_exception_handler,
    safe_job,
    setup_global_exception_handlers,
)

from app.db.manager import DatabaseManager
from app.db.repositories.lm_repository import LMRepository
from app.db.repositories.lm_dictionary_repository import LMDictionaryRepository
from app.db.utils_schema import validate_schema
from app.db.repositories.users import UserRepository
from app.telegram.middlewares.permissions import PermissionsManager
from app.utils.rate_limit import RateLimiter
from app.utils.action_guard import ActionGuard

# Сервисы
from app.services.call_lookup import CallLookupService
from app.services.yandex import YandexDiskCache, YandexDiskClient
from app.services.weekly_quality import WeeklyQualityService
from app.services.call_export import CallExportService
from app.services.reports import ReportService
from app.services.lm_service import LMService

# Хендлеры
from app.telegram.handlers.auth import setup_auth_handlers
from app.telegram.handlers.start import StartHandler
from app.telegram.handlers.call_lookup import register_call_lookup_handlers
from app.telegram.handlers.logging_middleware import register_logging_handlers
from app.telegram.handlers.dev_messages import register_dev_messages_handlers
from app.telegram.handlers.weekly_quality import register_weekly_quality_handlers
from app.telegram.handlers.reports import register_report_handlers
from app.telegram.handlers.system_menu import register_system_handlers
from app.telegram.handlers.manual import register_manual_handlers
from app.telegram.handlers.transcripts import TranscriptHandler

# Воркеры
from app.workers.task_worker import start_workers, stop_workers

# Инициализация логирования
setup_watchdog()
setup_global_exception_handlers()
logger = get_watchdog_logger(__name__)
polling_callback_logger = logging.getLogger(f"{__name__}.polling_callback")
updater_logger = logging.getLogger("telegram.ext.Updater")

# Блокировка повторного запуска
BASE_DIR = Path(__file__).resolve().parent.parent
LOCK_FILE = BASE_DIR / "operabot.lock"


POLLING_ERROR_THROTTLE_WINDOW_SEC = 60.0
ERROR_NOTIFY_TTL_SEC = 60.0
ERROR_NOTIFY_CACHE_KEY = "_error_notify_cache"
_UPDATER_POLLING_FILTER_FLAG = "_operabot_polling_filter_installed"


def _classify_polling_error(error: TelegramError) -> tuple[bool, str]:
    """Возвращает (is_transient, kind) для polling network ошибок."""
    if not isinstance(error, NetworkError):
        return False, "non_network"

    cause = getattr(error, "__cause__", None) or getattr(error, "__context__", None)
    if isinstance(cause, httpx.TimeoutException):
        return True, "timeout"
    if isinstance(cause, httpx.RemoteProtocolError):
        return True, "remote_disconnect"
    if isinstance(cause, httpx.ConnectError):
        return True, "connect_error"
    if isinstance(cause, httpx.ReadError):
        return True, "read_error"
    if isinstance(cause, httpx.NetworkError):
        return True, "network_transient"
    return True, "network_transient"


class _UpdaterPollingNoiseFilter(logging.Filter):
    """Скрывает noisy traceback от Updater, чтобы не дублировать наши polling-логи."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            return not is_polling_noise_record(record)
        except Exception:
            # Фильтр не должен ломать логирование в Updater.
            return True


def _install_updater_polling_filter() -> None:
    if getattr(updater_logger, _UPDATER_POLLING_FILTER_FLAG, False):
        return
    install_polling_noise_filter()
    updater_logger.addFilter(_UpdaterPollingNoiseFilter())
    setattr(updater_logger, _UPDATER_POLLING_FILTER_FLAG, True)


def _extract_handler_name(error: Optional[BaseException]) -> Optional[str]:
    tb = getattr(error, "__traceback__", None)
    if not tb:
        return None
    while tb.tb_next:
        tb = tb.tb_next
    return tb.tb_frame.f_code.co_name


def _already_notified_key(trace_id: str, update_id: Optional[int]) -> str:
    return f"{trace_id}:{update_id or '-'}"


def _register_notification_once(
    app_data: dict,
    trace_id: str,
    update_id: Optional[int],
    *,
    now_ts: Optional[float] = None,
    ttl_sec: float = ERROR_NOTIFY_TTL_SEC,
) -> bool:
    if now_ts is None:
        now_ts = time.monotonic()
    cache = app_data.setdefault(ERROR_NOTIFY_CACHE_KEY, OrderedDict())
    if not isinstance(cache, OrderedDict):
        cache = OrderedDict()
        app_data[ERROR_NOTIFY_CACHE_KEY] = cache

    # Evict stale entries.
    stale_keys = [key for key, ts in cache.items() if (now_ts - ts) > ttl_sec]
    for key in stale_keys:
        cache.pop(key, None)

    key = _already_notified_key(trace_id, update_id)
    if key in cache:
        return False
    cache[key] = now_ts
    while len(cache) > 500:
        cache.popitem(last=False)
    return True


def make_polling_error_callback(
    *,
    now_fn: Callable[[], float] = time.monotonic,
    throttle_window_sec: float = POLLING_ERROR_THROTTLE_WINDOW_SEC,
) -> Callable[[TelegramError], None]:
    """Создает безопасный callback для Updater polling ошибок."""
    state = {
        "last_emit_ts": None,
        "suppressed_count": 0,
        "last_kind": None,
    }

    def polling_error_callback(error: TelegramError) -> None:
        try:
            is_transient, kind = _classify_polling_error(error)
            if not is_transient:
                logger.error(
                    "Non-transient polling error from Telegram API.",
                    exc_info=(type(error), error, error.__traceback__),
                )
                return

            now_ts = now_fn()
            last_emit_ts = state["last_emit_ts"]
            last_kind = state["last_kind"]
            suppressed_count = int(state["suppressed_count"])
            should_emit = (
                last_kind != kind
                or last_emit_ts is None
                or (now_ts - float(last_emit_ts)) >= throttle_window_sec
            )

            if should_emit:
                logger.warning(
                    "Transient polling network error (%s). "
                    "Suppressed repeats since last log: %d. Details: %s",
                    kind,
                    suppressed_count,
                    error,
                )
                state["last_emit_ts"] = now_ts
                state["suppressed_count"] = 0
                state["last_kind"] = kind
                return

            state["suppressed_count"] = suppressed_count + 1
        except Exception:
            # Callback не должен ронять polling loop ни при каких условиях.
            polling_callback_logger.exception("Polling error callback failed unexpectedly.")

    return polling_error_callback


async def user_context_injector(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Единый резолвер пользователя: сохраняет user_ctx в context.user_data."""
    user = update.effective_user
    if not user:
        return
    repo: Optional[UserRepository] = context.application.bot_data.get("user_repository")  # type: ignore[assignment]
    if not repo:
        return
    try:
        user_ctx = await repo.get_user_context_by_telegram_id(user.id)
    except AppError as exc:
        logger.warning(
            "Не удалось получить контекст пользователя %s: %s",
            user.id,
            exc,
        )
        return
    if user_ctx:
        context.user_data["user_ctx"] = user_ctx
    else:
        context.user_data.pop("user_ctx", None)


_INCOMING_TEXT_SKIP_PATTERNS = [
    re.compile(r"(?i)^\s*(?:📊\s*)?(?:ai\s+)?отч[её]ты\s*$"),
    re.compile(r"(?i)^\s*🔍\s*поиск\s+звонк[ао]в?\s*$"),
]
_CALL_LOOKUP_PENDING_PREFIX = "call_lookup_pending"
_CALL_LOOKUP_NUMBER_PATTERN = re.compile(r"^[\d\+\-\s\.\(\)\*…]+$")


async def debug_incoming(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Логирует входящие текстовые сообщения для отладки Reply-клавиатуры."""
    message = update.effective_message
    if not message or not message.text:
        return

    normalized = message.text.strip()
    for pattern in _INCOMING_TEXT_SKIP_PATTERNS:
        if pattern.match(normalized):
            return

    chat = update.effective_chat
    if chat:
        pending_key = f"{_CALL_LOOKUP_PENDING_PREFIX}:{chat.id}"
        if pending_key in context.chat_data and _CALL_LOOKUP_NUMBER_PATTERN.match(normalized):
            return

    logger.warning("[INCOMING TEXT] %r", message.text)


async def telegram_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Единственная верхнеуровневая точка финальной обработки ошибок PTB."""
    error = context.error
    update_obj: Optional[Update] = update if isinstance(update, Update) else None
    user = update_obj.effective_user if update_obj else None
    chat = update_obj.effective_chat if update_obj else None
    trace_id = get_trace_id() or "no-trace"
    handler_name = _extract_handler_name(error)

    if update_obj and update_obj.callback_query:
        update_type = "callback_query"
    elif update_obj and update_obj.message:
        update_type = "message"
    elif update_obj and update_obj.inline_query:
        update_type = "inline_query"
    else:
        update_type = "unknown"

    incident_logged = bool(getattr(error, "_incident_logged", False)) if error else False
    if incident_logged:
        logger.debug(
            "Incident already logged for this exception object",
            extra={"trace_id": trace_id, "handler_name": handler_name},
        )
    else:
        logger.error(
            "Unhandled exception in Telegram handler",
            exc_info=(type(error), error, error.__traceback__) if error else None,
            extra={
                "source": "telegram.application",
                "error_type": type(error).__name__ if error else None,
                "handler_name": handler_name,
                "update_type": update_type,
                "update_id": update_obj.update_id if update_obj else None,
                "user_id": user.id if user else None,
                "username": user.username if user else None,
                "chat_id": chat.id if chat else None,
                "trace_id": trace_id,
            },
        )
        if error is not None:
            setattr(error, "_incident_logged", True)

    if should_alert(error if isinstance(error, Exception) else Exception("unknown")):
        logger.warning(
            "Alert-worthy incident in Telegram handler",
            extra={
                "trace_id": trace_id,
                "handler_name": handler_name,
                "error_type": type(error).__name__ if error else None,
            },
        )
        if TELEGRAM_CHAT_ID:
            try:
                await context.application.bot.send_message(
                    chat_id=TELEGRAM_CHAT_ID,
                    text=(
                        "🚨 Incident in Telegram handler\n"
                        f"trace_id: {trace_id}\n"
                        f"handler: {handler_name or 'unknown'}\n"
                        f"error: {type(error).__name__ if error else 'None'}"
                    ),
                )
            except TelegramError as notify_exc:
                logger.warning(
                    "Failed to send alert to TELEGRAM_CHAT_ID: %s",
                    notify_exc,
                )

    if not update_obj:
        return

    notify_allowed = _register_notification_once(
        context.application.bot_data,
        trace_id,
        update_obj.update_id,
    )
    if not notify_allowed:
        logger.debug(
            "Skipping duplicate user notification for incident",
            extra={"trace_id": trace_id, "update_id": update_obj.update_id},
        )
        return

    user_message = resolve_user_message(
        error if isinstance(error, Exception) else AppError("Unknown incident")
    )
    if not user_message:
        user_message = "Команда временно недоступна. Попробуйте позже."

    if update_obj.callback_query:
        try:
            await update_obj.callback_query.answer(user_message, show_alert=True)
            return
        except TelegramError as notify_error:
            wrapped = TelegramIntegrationError(
                "Failed to answer callback_query about incident",
                user_visible=False,
                details={"trace_id": trace_id, "update_id": update_obj.update_id},
            )
            logger.warning(
                "Failed to notify user via callback_query: %s",
                notify_error,
                exc_info=(type(wrapped), wrapped, wrapped.__traceback__),
            )

    if update_obj.effective_message:
        try:
            await update_obj.effective_message.reply_text(user_message)
        except TelegramError as notify_error:
            wrapped = TelegramIntegrationError(
                "Failed to send user-facing incident message",
                user_visible=False,
                details={"trace_id": trace_id, "update_id": update_obj.update_id},
            )
            logger.warning(
                "Failed to send user error message: %s",
                notify_error,
                exc_info=(type(wrapped), wrapped, wrapped.__traceback__),
            )

def acquire_lock():
    LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    fp = open(LOCK_FILE, "w")
    try:
        fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fp
    except IOError as exc:
        logger.error("Не удалось получить блокировку запуска: %s", exc, exc_info=True)
        print("Бот уже запущен!")
        sys.exit(1)

# Обработчик необработанных исключений
def log_uncaught_exceptions(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.error(
        "Необработанное исключение", exc_info=(exc_type, exc_value, exc_traceback)
    )

sys.excepthook = log_uncaught_exceptions


def _patch_handler_registration(application):
    """Позволяет писать код с group даже на старых версиях PTB10."""
    original_add_handler = application.add_handler

    def safe_add_handler(self, handler, *args, **kwargs):
        try:
            return original_add_handler(handler, *args, **kwargs)
        except TypeError as exc:
            if "unexpected keyword argument 'group'" in str(exc) and "group" in kwargs:
                kwargs = dict(kwargs)
                kwargs.pop("group", None)
                logger.warning(
                    "PTB version does not support grouped handlers, falling back to default. Details: %s",
                    exc,
                )
                return original_add_handler(handler, *args, **kwargs)
            raise

    application.add_handler = types.MethodType(safe_add_handler, application)
async def main():
    # Блокировка запуска
    lock_fp = acquire_lock()
    
    logger.info("Запуск бота (новая архитектура)...")

    # 1. Инициализация БД
    db_manager = DatabaseManager()
    await db_manager.create_pool()
    logger.info("Пул соединений с БД создан.")
    await validate_schema(db_manager)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    install_loop_exception_handler(loop)
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, RuntimeError) as signal_exc:
            # Например, on Windows или если цикл уже завершается
            logger.debug(
                "Не удалось зарегистрировать обработчик сигнала %s: %s",
                sig,
                signal_exc,
                exc_info=True,
            )

    try:
        # 2. Инициализация сервисов
        permissions_manager = PermissionsManager(db_manager)
        lm_repo = LMRepository(db_manager)
        dictionary_repo = LMDictionaryRepository(db_manager)
        lm_service = LMService(lm_repo, dictionary_repository=dictionary_repo)
        user_repo = UserRepository(db_manager)
        call_lookup_service = CallLookupService(db_manager, lm_repo)
        yandex_disk_client = YandexDiskClient.from_env()
        yandex_disk_cache = YandexDiskCache(
            os.getenv("REDIS_URL"),
            file_ttl_seconds=int(os.getenv("YDISK_TG_FILE_TTL", "0") or 0) or None,
        )
        weekly_quality_service = WeeklyQualityService(db_manager)
        call_export_service = CallExportService(db_manager)
        report_service = ReportService(db_manager)
        rate_limiter = RateLimiter()
        action_guard = ActionGuard()
        
        # Admin panel components
        from app.db.repositories.admin import AdminRepository
        from app.services.notifications import NotificationsManager as NotificationService
        
        admin_repo = AdminRepository(db_manager)
        notification_service = NotificationService()  # Existing service

        # 3. Инициализация приложения Telegram
        telegram_transport = httpx.AsyncHTTPTransport(retries=3)
        telegram_limits = httpx.Limits(max_keepalive_connections=0, max_connections=20)
        request = HTTPXRequest(
            connect_timeout=15,
            read_timeout=70,
            write_timeout=30,
            pool_timeout=15,
            http_version="1.1",
            httpx_kwargs={
                "http2": False,
                "transport": telegram_transport,
                "limits": telegram_limits,
            },
        )
        application = (
            ApplicationBuilder()
            .token(TELEGRAM_TOKEN)
            .request(request)
            .build()
        )
        _patch_handler_registration(application)
        application.add_error_handler(telegram_error_handler)
        workers_started = False
        
        # Привязываем сервисы к bot_data для доступа из воркеров и хендлеров
        application.bot_data["db_manager"] = db_manager
        application.bot_data["report_service"] = report_service
        application.bot_data["weekly_quality_service"] = weekly_quality_service
        application.bot_data["call_export_service"] = call_export_service
        application.bot_data["permissions_manager"] = permissions_manager
        application.bot_data["admin_repo"] = admin_repo
        application.bot_data["user_repository"] = user_repo
        application.bot_data["rate_limiter"] = rate_limiter
        application.bot_data["action_guard"] = action_guard
        application.bot_data["yandex_disk_cache"] = yandex_disk_cache

        # 4. Регистрация хендлеров
        logger.info("Регистрация хендлеров...")

        context_handler = TypeHandler(Update, user_context_injector)
        context_handler.block = False  # Не блокируем последующие MessageHandler-ы с reply-кнопок
        application.add_handler(context_handler, group=-2)
        debug_handler = MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            debug_incoming,
        )
        debug_handler.block = False
        application.add_handler(debug_handler, group=99)
        
        # Вспомогательные лог-хендлеры
        register_logging_handlers(application)

        # Auth
        setup_auth_handlers(application, db_manager, permissions_manager)

        # /start с новым UX
        start_handler = StartHandler(db_manager)
        application.add_handler(start_handler.get_handler())
        # Live dashboard (личная статистика операторов)
        from app.telegram.handlers.dashboard import DashboardHandler
        dashboard_handler = DashboardHandler(db_manager)
        for handler in dashboard_handler.get_handlers():
            application.add_handler(handler)
        application.add_handler(
            MessageHandler(
                filters.Regex(r"(?i)^\s*(?:📊\s*)?моя\s+статистик[аи]\s*$"),
                dashboard_handler.dashboard_command,
            ),
            group=0,
        )
        
        # Admin Panel
        from app.telegram.handlers.admin_panel import register_admin_panel_handlers
        from app.telegram.handlers.admin_users import register_admin_users_handlers
        from app.telegram.handlers.admin_commands import register_admin_commands_handlers
        from app.telegram.handlers.admin_stats import register_admin_stats_handlers
        from app.telegram.handlers.admin_admins import register_admin_admins_handlers
        from app.telegram.handlers.admin_lookup import register_admin_lookup_handlers
        from app.telegram.handlers.admin_settings import register_admin_settings_handlers
        
        # Initialize MetricsService for stats
        from app.services.metrics_service import MetricsService
        from app.db.repositories.operators import OperatorRepository
        operator_repo = OperatorRepository(db_manager)
        metrics_service = MetricsService(operator_repo)
        
        register_admin_panel_handlers(application, admin_repo, permissions_manager)
        register_admin_users_handlers(application, admin_repo, permissions_manager, notification_service)
        register_admin_admins_handlers(application, admin_repo, permissions_manager, notification_service)
        register_admin_commands_handlers(application, admin_repo, permissions_manager, notification_service)
        register_admin_stats_handlers(application, admin_repo, metrics_service, permissions_manager)
        register_admin_lookup_handlers(application, permissions_manager)
        register_admin_settings_handlers(application, admin_repo, permissions_manager)
        register_dev_messages_handlers(application, db_manager, permissions_manager, admin_repo)
        
        # Legacy Adapter (перехват старых кнопок)
        from app.telegram.handlers.legacy_adapter import LegacyCallbackAdapter
        application.add_handler(LegacyCallbackAdapter.get_handler())
        
        # LM Metrics
        from app.telegram.handlers.admin_lm import register_admin_lm_handlers
        register_admin_lm_handlers(application, lm_repo, permissions_manager, lm_service)
        
        # Call Lookup (/call_lookup)
        register_call_lookup_handlers(
            application,
            call_lookup_service,
            permissions_manager,
            yandex_disk_client=yandex_disk_client,
            yandex_disk_cache=yandex_disk_cache,
        )
        
        # Weekly Quality (/weekly_quality)
        register_weekly_quality_handlers(application, weekly_quality_service, permissions_manager)
        
        # Reports (/report)
        register_report_handlers(application, report_service, permissions_manager, db_manager)
        
        # Transcripts (/transcript)
        transcript_handler = TranscriptHandler(db_manager, permissions_manager, admin_repo)
        for handler in transcript_handler.get_handlers():
            application.add_handler(handler)

        # Системное меню и кнопка помощи
        register_system_handlers(application, db_manager, permissions_manager)
        register_manual_handlers(application)
        
        # Text Router (центральный обработчик текста)
        from app.telegram.handlers.text_router import TextRouter
        application.add_handler(TextRouter.get_handler(), group=10)

        await set_bot_commands(application)

        # 5. Настройка планировщика (APScheduler)
        scheduler = AsyncIOScheduler()
        
        async def send_weekly_report():
            logger.info("Запуск автоматической отправки еженедельного отчета...")
            report_text = await weekly_quality_service.get_text_report(period="weekly")
            if TELEGRAM_CHAT_ID:
                await application.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=report_text)
                logger.info("Еженедельный отчет отправлен в чат %s", TELEGRAM_CHAT_ID)
            else:
                logger.warning("TELEGRAM_CHAT_ID не установлен, отчет не отправлен.")

        # Запуск каждый понедельник в 09:00
        scheduler.add_job(
            safe_job,
            args=('weekly_quality_report', send_weekly_report),
            trigger=CronTrigger(day_of_week='mon', hour=9, minute=0),
            id='weekly_quality_report',
            replace_existing=True
        )

        # Инициализация сервиса синхронизации аналитики
        from app.services.call_analytics_sync import CallAnalyticsSyncService
        analytics_sync_service = CallAnalyticsSyncService(db_manager)

        async def run_analytics_sync():
            logger.info("Запуск плановой синхронизации аналитики...")
            await analytics_sync_service.sync_new()

        # Запуск синхронизации каждые 30 минут
        scheduler.add_job(
            safe_job,
            args=('analytics_sync', run_analytics_sync),
            trigger=CronTrigger(minute='*/30'),
            id='analytics_sync',
            replace_existing=True
        )
        await application.initialize()
        await application.start()

        # 6. Запуск воркеров очереди
        await start_workers(application)
        workers_started = True

        scheduler.start()
        logger.info("Планировщик запущен.")

        await application.bot.delete_webhook(drop_pending_updates=True)
        logger.info("Webhook удален (если был), переключаемся на Polling.")
        _install_updater_polling_filter()
        polling_error_callback = make_polling_error_callback()
        
        try:
            await application.updater.start_polling(
                timeout=30,
                bootstrap_retries=3,
                read_timeout=70,
                write_timeout=30,
                connect_timeout=15,
                pool_timeout=15,
                error_callback=polling_error_callback,
            )
        except Exception:
            logger.exception("Ошибка при запуске polling")
            raise
        logger.info("Бот запущен и готов к работе (Polling).")
        await stop_event.wait()

    finally:
        stop_event.set()
        # Остановка и очистка ресурсов
        logger.info("Остановка бота...")
        if 'scheduler' in locals():
            scheduler.shutdown(wait=False)
        
        # Остановка воркеров и приложения
        if 'application' in locals():
            updater = getattr(application, "updater", None)
            if updater:
                try:
                    await updater.stop()
                except RuntimeError as exc:
                    logger.warning("Updater stop skipped: %s", exc)
            if 'workers_started' in locals() and workers_started:
                await stop_workers(application)
        if 'application' in locals():
            try:
                await application.stop()
            except RuntimeError as exc:
                logger.warning("Application stop skipped: %s", exc)
            try:
                await application.shutdown()
            except RuntimeError as exc:
                logger.warning("Application shutdown skipped: %s", exc)

        if 'yandex_disk_cache' in locals() and yandex_disk_cache:
            await yandex_disk_cache.close()
        await db_manager.close()
        logger.info("Бот остановлен.")
        
        # Освобождение блокировки (хотя ОС сделает это сама при выходе)
        try:
            lock_fp.close()
        finally:
            try:
                LOCK_FILE.unlink()
            except FileNotFoundError:
                logger.debug("Lock файл %s уже отсутствует при завершении", LOCK_FILE)
            except OSError as exc:
                logger.warning("Не удалось удалить lock файл %s: %s", LOCK_FILE, exc)


async def set_bot_commands(application):
    """Устанавливает команды бота для меню."""
    commands = [
        BotCommand("start", "🏠 Главное меню"),
        BotCommand("help", "❓ Справка и инструкции"),
        BotCommand("admin", "👑 Админ-панель"),
    ]
    
    try:
        await application.bot.set_my_commands(commands)
        logger.info("✅ Команды бота установлены: /start, /help, /admin")
    except Exception as e:
        logger.error("❌ Ошибка при установке команд бота: %s", e, exc_info=True)


if __name__ == "__main__":

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Остановка бота по сигналу KeyboardInterrupt")
