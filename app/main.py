# Файл: app/main.py

"""
Главный модуль приложения.
"""

from __future__ import annotations

import asyncio
import fcntl
import sys
import logging
import signal
import os
import re
from typing import Optional

import httpx
from telegram import BotCommand, Update
from telegram.error import TelegramError
from telegram.ext import ApplicationBuilder, ContextTypes, MessageHandler, TypeHandler, filters
from telegram.request import HTTPXRequest
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
from app.logging_config import get_trace_id, setup_watchdog, get_watchdog_logger

# Импортируем error handlers для установки глобальных обработчиков
from app.utils.error_handlers import (
    ErrorContext,
    install_loop_exception_handler,
    log_async_exceptions,
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

# Блокировка повторного запуска
LOCK_FILE = "/app/operabot.lock"


USER_ERROR_MESSAGE = "Ошибка доступа к базе. Проверьте конфигурацию/схему БД."


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
    except Exception as exc:
        logger.warning(
            "Не удалось получить контекст пользователя %s: %s",
            user.id,
            exc,
            exc_info=True,
        )
        return
    if user_ctx:
        context.user_data["user_ctx"] = user_ctx
    else:
        context.user_data.pop("user_ctx", None)


_INCOMING_TEXT_SKIP_PATTERNS = [
    re.compile(r"(?i)^\s*(?:📊\s*)?(?:ai\s+)?отч[её]ты\s*$"),
]


async def debug_incoming(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Логирует входящие текстовые сообщения для отладки Reply-клавиатуры."""
    message = update.effective_message
    if not message or not message.text:
        return

    normalized = message.text.strip()
    for pattern in _INCOMING_TEXT_SKIP_PATTERNS:
        if pattern.match(normalized):
            return
    logger.warning("[INCOMING TEXT] %r", message.text)


async def telegram_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Глобально логируем необработанные ошибки PTB, чтобы не терять контекст."""
    error = context.error
    already_logged = bool(getattr(error, "_already_logged", False)) if error else False
    update_obj: Optional[Update] = update if isinstance(update, Update) else None
    user = update_obj.effective_user if update_obj else None
    chat = update_obj.effective_chat if update_obj else None

    if update_obj and update_obj.callback_query:
        update_type = "callback_query"
    elif update_obj and update_obj.message:
        update_type = "message"
    elif update_obj and update_obj.inline_query:
        update_type = "inline_query"
    else:
        update_type = "unknown"

    handler_name = None
    tb = getattr(error, "__traceback__", None)
    if tb:
        while tb.tb_next:
            tb = tb.tb_next
        handler_name = tb.tb_frame.f_code.co_name

    if already_logged:
        logger.debug(
            "Исключение уже залогировано в handler-е, пропускаем дубль",
            extra={
                "source": "telegram.application",
                "handler_name": handler_name,
                "update_type": update_type,
                "update_id": update_obj.update_id if update_obj else None,
                "user_id": user.id if user else None,
                "username": user.username if user else None,
                "chat_id": chat.id if chat else None,
                "trace_id": get_trace_id(),
            },
        )
    else:
        logger.error(
            "Unhandled exception в Telegram handler",
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
                "trace_id": get_trace_id(),
            },
        )
    if update_obj:
        try:
            if update_obj.callback_query:
                try:
                    await update_obj.callback_query.answer(USER_ERROR_MESSAGE, show_alert=True)
                except Exception:
                    logger.debug("Не удалось показать alert пользователю", exc_info=True)
                if update_obj.callback_query.message:
                    await update_obj.callback_query.message.reply_text(USER_ERROR_MESSAGE)
            elif update_obj.message:
                await update_obj.message.reply_text(USER_ERROR_MESSAGE)
        except Exception:
            logger.debug("Не удалось уведомить пользователя об ошибке", exc_info=True)
    user_notified = bool(getattr(error, "_user_notified", False)) if error else False
    if update_obj and update_obj.callback_query and not user_notified:
        try:
            await update_obj.callback_query.answer(
                text="Команда временно недоступна. Попробуйте позже.",
                show_alert=True,
            )
            user_notified = True
        except TelegramError as notify_error:
            logger.warning(
                "Не удалось уведомить пользователя через callback: %s",
                notify_error,
            )
    if (
        not user_notified
        and update_obj
        and update_obj.effective_message
    ):
        try:
            await update_obj.effective_message.reply_text(
                "Команда временно недоступна. Попробуйте позже."
            )
            user_notified = True
        except TelegramError as notify_error:
            logger.warning(
                "Не удалось отправить сообщение об ошибке: %s",
                notify_error,
            )

def acquire_lock():
    os.makedirs(os.path.dirname(LOCK_FILE), exist_ok=True)
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
        application.add_error_handler(telegram_error_handler)
        workers_started = False
        
        # Привязываем сервисы к bot_data для доступа из воркеров и хендлеров
        application.bot_data["db_manager"] = db_manager
        application.bot_data["report_service"] = report_service
        application.bot_data["weekly_quality_service"] = weekly_quality_service
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
        
        await application.updater.start_polling(
            timeout=30,
            read_timeout=70,
            write_timeout=30,
            connect_timeout=15,
            pool_timeout=15,
        )
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
            await application.stop()
            await application.shutdown()

        if 'yandex_disk_cache' in locals() and yandex_disk_cache:
            await yandex_disk_cache.close()
        await db_manager.close()
        logger.info("Бот остановлен.")
        
        # Освобождение блокировки (хотя ОС сделает это сама при выходе)
        try:
            lock_fp.close()
        finally:
            try:
                os.remove(LOCK_FILE)
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
