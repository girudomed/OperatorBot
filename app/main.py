"""
Главный модуль приложения.
"""

import asyncio
import fcntl
import sys
import logging
import signal
import os
from typing import Optional

from telegram import BotCommand, Update
from telegram.ext import ApplicationBuilder, ContextTypes
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
from app.logging_config import setup_watchdog, get_watchdog_logger

# Импортируем error handlers для установки глобальных обработчиков
from app.utils.error_handlers import (
    setup_global_exception_handlers,
    log_async_exceptions,
    ErrorContext
)

from app.db.manager import DatabaseManager
from app.telegram.middlewares.permissions import PermissionsManager

# Сервисы
from app.services.call_lookup import CallLookupService
from app.services.weekly_quality import WeeklyQualityService
from app.services.reports import ReportService

# Хендлеры
from app.telegram.handlers.auth import setup_auth_handlers
from app.telegram.handlers.start import StartHandler
from app.telegram.handlers.call_lookup import register_call_lookup_handlers
from app.telegram.handlers.weekly_quality import register_weekly_quality_handlers
from app.telegram.handlers.reports import register_report_handlers

# Воркеры
from app.workers.task_worker import start_workers, stop_workers

# Инициализация логирования
setup_watchdog()
setup_global_exception_handlers()
logger = get_watchdog_logger(__name__)

# Блокировка повторного запуска
LOCK_FILE = "/app/operabot.lock"


async def telegram_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Глобально логируем необработанные ошибки PTB, чтобы не терять контекст."""
    error = context.error
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
        },
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

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
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
        
        from app.db.repositories.lm_repository import LMRepository

        lm_repo = LMRepository(db_manager)
        call_lookup_service = CallLookupService(db_manager, lm_repo)
        weekly_quality_service = WeeklyQualityService(db_manager)
        report_service = ReportService(db_manager)
        
        # Admin panel components
        from app.db.repositories.admin import AdminRepository
        from app.services.notifications import NotificationsManager as NotificationService
        
        admin_repo = AdminRepository(db_manager)
        notification_service = NotificationService()  # Existing service

        # 3. Инициализация приложения Telegram
        application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
        application.add_error_handler(telegram_error_handler)
        workers_started = False
        
        # Привязываем сервисы к bot_data для доступа из воркеров и хендлеров
        application.bot_data["db_manager"] = db_manager
        application.bot_data["report_service"] = report_service
        application.bot_data["permissions_manager"] = permissions_manager
        application.bot_data["admin_repo"] = admin_repo

        # 4. Регистрация хендлеров
        logger.info("Регистрация хендлеров...")
        
        # Auth
        setup_auth_handlers(application, db_manager, permissions_manager)

        # /start с новым UX
        start_handler = StartHandler(db_manager)
        application.add_handler(start_handler.get_handler())
        
        # Admin Panel
        from app.telegram.handlers.admin_panel import register_admin_panel_handlers
        from app.telegram.handlers.admin_users import register_admin_users_handlers
        from app.telegram.handlers.admin_commands import register_admin_commands_handlers
        from app.telegram.handlers.admin_stats import register_admin_stats_handlers
        from app.telegram.handlers.admin_admins import register_admin_admins_handlers
        from app.telegram.handlers.admin_lookup import register_admin_lookup_handlers
        
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
        
        # LM Metrics
        from app.telegram.handlers.admin_lm import register_admin_lm_handlers
        register_admin_lm_handlers(application, lm_repo, permissions_manager)
        
        # Call Lookup (/call_lookup)
        register_call_lookup_handlers(application, call_lookup_service, permissions_manager)
        
        # Weekly Quality (/weekly_quality)
        register_weekly_quality_handlers(application, weekly_quality_service, permissions_manager)
        
        # Reports (/report)
        register_report_handlers(application, report_service, permissions_manager, db_manager)

        await set_bot_commands(application)

        # 5. Настройка планировщика (APScheduler)
        scheduler = AsyncIOScheduler()
        
        async def send_weekly_report():
            logger.info("Запуск автоматической отправки еженедельного отчета...")
            try:
                report_text = await weekly_quality_service.get_text_report(period="weekly")
                if TELEGRAM_CHAT_ID:
                    await application.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=report_text)
                    logger.info(f"Еженедельный отчет отправлен в чат {TELEGRAM_CHAT_ID}")
                else:
                    logger.warning("TELEGRAM_CHAT_ID не установлен, отчет не отправлен.")
            except Exception:
                logger.exception("Ошибка при отправке еженедельного отчета.")

        # Запуск каждый понедельник в 09:00
        scheduler.add_job(
            send_weekly_report,
            CronTrigger(day_of_week='mon', hour=9, minute=0),
            id='weekly_quality_report',
            replace_existing=True
        )

        # Инициализация сервиса синхронизации аналитики
        from app.services.call_analytics_sync import CallAnalyticsSyncService
        analytics_sync_service = CallAnalyticsSyncService(db_manager)

        async def run_analytics_sync():
            logger.info("Запуск плановой синхронизации аналитики...")
            try:
                await analytics_sync_service.sync_new()
            except Exception:
                logger.exception("Ошибка при плановой синхронизации аналитики.")

        # Запуск синхронизации каждые 30 минут
        scheduler.add_job(
            run_analytics_sync,
            CronTrigger(minute='*/30'),
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
        
        await application.updater.start_polling()
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
            await application.updater.stop()
            if 'workers_started' in locals() and workers_started:
                await stop_workers(application)
            await application.stop()
            await application.shutdown()

        await db_manager.close()
        logger.info("Бот остановлен.")
        
        # Освобождение блокировки (хотя ОС сделает это сама при выходе)
        lock_fp.close()


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
