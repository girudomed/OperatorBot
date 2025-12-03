"""
Главный модуль приложения.
"""

import asyncio
import fcntl
import sys
import logging
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
from app.telegram.handlers.call_lookup import register_call_lookup_handlers
from app.telegram.handlers.weekly_quality import register_weekly_quality_handlers
from app.telegram.handlers.reports import register_report_handlers

# Воркеры
from app.workers.task_worker import start_workers, stop_workers

# Инициализация логирования
setup_watchdog()
logger = get_watchdog_logger(__name__)

# Блокировка повторного запуска
LOCK_FILE = "/tmp/operabot.lock"


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
    fp = open(LOCK_FILE, "w")
    try:
        fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fp
    except IOError:
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
        
        # Call Lookup (/call_lookup)
        register_call_lookup_handlers(application, call_lookup_service, permissions_manager)
        
        # Weekly Quality (/weekly_quality)
        register_weekly_quality_handlers(application, weekly_quality_service, permissions_manager)
        
        # Reports (/report)
        register_report_handlers(application, report_service, permissions_manager, db_manager)

        await _configure_bot_commands(application)

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
        await application.initialize()
        await application.start()

        # 6. Запуск воркеров очереди
        await start_workers(application)
        workers_started = True

        scheduler.start()
        logger.info("Планировщик запущен.")

        await application.updater.start_polling()
        logger.info("Бот запущен и готов к работе (Polling).")
        await application.updater.wait_for_stop()

    except Exception:
        logger.exception("Критическая ошибка во время работы бота.")
        raise
    finally:
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


async def _configure_bot_commands(application):
    """Устанавливает список доступных команд бота."""
    commands = [
        BotCommand("start", "Перезапустить диалог"),
        BotCommand("help", "Показать список команд"),
        BotCommand("register", "Отправить заявку на доступ"),
        BotCommand("weekly_quality", "Еженедельный отчёт качества"),
        BotCommand("report", "AI-отчёт"),
        BotCommand("call_lookup", "Поиск звонков по номеру"),
        BotCommand("admin", "Открыть админ-панель"),
        BotCommand("approve", "Одобрить пользователя"),
        BotCommand("make_admin", "Назначить администратора"),
        BotCommand("make_superadmin", "Назначить супер-админа"),
        BotCommand("admins", "Показать администраторов"),
    ]
    await application.bot.set_my_commands(commands)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
