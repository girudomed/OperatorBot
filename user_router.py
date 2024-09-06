import io
import logging
import traceback
import html
import json
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import telegram
from telegram import Update, BotCommand
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackContext,
    CommandHandler,
    AIORateLimiter
)
from telegram.constants import ParseMode

import config
from operator_data import OperatorData  # Импорт модуля
from report_generator import ReportGenerator  # Импорт модуля
from admin_utils import get_user_role, is_authorized  # Импорт функций для работы с уровнями доступа

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
from logger_utils import setup_logging

logger = setup_logging()

def some_function():
    logger.info("Функция some_function начала работу.")
    # Логика функции
    try:
        # Некоторый код
        logger.info("Успешное выполнение.")
    except Exception as e:
        logger.error(f"Произошла ошибка: {e}")


# Инициализация Telegram бота
bot = telegram.Bot(token=config.telegram_token)

# Инициализация генератора отчетов
report_generator = ReportGenerator(model="gpt-4o-mini")
operator_data = OperatorData(db_config=config.db_config)

# Планировщик задач
scheduler = AsyncIOScheduler()

# Помощь / Команды
HELP_MESSAGE = """Commands:
/start – Приветствие и инструкция
/generate_report <operator_id> – Генерация отчета
/request_stats – Запрос текущей статистики
/help – Показать помощь
/report_summary – Сводка по отчетам
/settings – Показать настройки (опционально)
/cancel – Отменить текущую задачу
"""

def split_text_into_chunks(text, chunk_size=4096):
    """
    Разделяет текст на куски определенного размера.
    Это полезно для отправки длинных сообщений в Telegram, где есть ограничение на количество символов в сообщении.
    """
    for i in range(0, len(text), chunk_size):
        yield text[i:i + chunk_size]

# Ежедневная задача
async def daily_report():
    operators = await operator_data.get_all_operators_metrics()  # Асинхронный вызов
    for operator in operators:
        recommendations = await report_generator.generate_recommendations(operator)  # Асинхронный вызов
        report = await report_generator.create_report(operator_id=operator["operator_id"], operator_data=operator, recommendations=recommendations)  # Асинхронный вызов
        await bot.send_message(chat_id=config.manager_chat_id, text=report)

# Еженедельная задача
async def weekly_report():
    await bot.send_message(chat_id=config.manager_chat_id, text="Еженедельный отчет...")

# Ежемесячная задача
async def monthly_report():
    await bot.send_message(chat_id=config.manager_chat_id, text="Ежемесячный отчет...")

# Планирование задач
scheduler.add_job(daily_report, 'cron', hour=18, minute=0)  # Каждый день в 18:00
scheduler.add_job(weekly_report, 'cron', day_of_week='sun', hour=18, minute=0)  # Каждое воскресенье в 18:00
scheduler.add_job(monthly_report, 'cron', day=1, hour=18, minute=0)  # Первого числа каждого месяца в 18:00

# Запуск планировщика
scheduler.start()

async def start_handle(update: Update, context: CallbackContext):
    reply_text = "Привет! Я бот для генерации отчетов на основе данных операторов.\n\n"
    reply_text += HELP_MESSAGE
    await update.message.reply_text(reply_text, parse_mode=ParseMode.HTML)

async def help_handle(update: Update, context: CallbackContext):
    await update.message.reply_text(HELP_MESSAGE, parse_mode=ParseMode.HTML)

async def generate_report_handle(update: Update, context: CallbackContext):
    try:
        user_id = update.message.from_user.id
        if not await is_authorized(user_id, ["Founder", "Developer", "Head of Marketing", "Head of Registration"]):
            await update.message.reply_text("У вас нет доступа для выполнения этой команды.")
            return

        operator_id = update.message.text.split()[1]
        operator = await operator_data.get_operator_metrics(operator_id)  # Асинхронный вызов
        if not operator:
            await update.message.reply_text(f"Оператор с ID {operator_id} не найден.")
            return

        recommendations = await report_generator.generate_recommendations(operator)  # Асинхронный вызов
        report = await report_generator.create_report(operator_id=operator_id, operator_data=operator, recommendations=recommendations)  # Асинхронный вызов
        await update.message.reply_text(report, parse_mode=ParseMode.HTML)
    except IndexError:
        await update.message.reply_text("Пожалуйста, укажите operator_id после команды /generate_report.", parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка при генерации отчета: {e}")
        await update.message.reply_text(f"Произошла ошибка: {e}")

async def request_current_stats_handle(update: Update, context: CallbackContext):
    try:
        user_id = update.message.from_user.id
        operator = await operator_data.get_operator_metrics(user_id)  # Асинхронный вызов
        if not operator:
            await update.message.reply_text("Данные для вашего аккаунта не найдены.")
            return

        report = await report_generator.create_report(
            operator_id=user_id, 
            operator_data=operator, 
            recommendations="Ваш текущий отчет"
        )
        await update.message.reply_text(report, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка при генерации текущей статистики: {e}")
        await update.message.reply_text(f"Произошла ошибка при запросе статистики: {e}")

async def report_summary_handle(update: Update, context: CallbackContext):
    try:
        user_id = update.message.from_user.id
        if not await is_authorized(user_id, ["Founder", "Developer", "Head of Marketing", "Head of Registration"]):
            await update.message.reply_text("У вас нет доступа для выполнения этой команды.")
            return
        await update.message.reply_text("Сводка по отчетам...")
    except Exception as e:
        logger.error(f"Ошибка при запросе сводки по отчетам: {e}")
        await update.message.reply_text(f"Произошла ошибка: {e}")

async def settings_handle(update: Update, context: CallbackContext):
    try:
        user_id = update.message.from_user.id
        if not await is_authorized(user_id, ["Founder", "Developer"]):
            await update.message.reply_text("У вас нет доступа для выполнения этой команды.")
            return
        await update.message.reply_text("Текущие настройки...")
    except Exception as e:
        logger.error(f"Ошибка при запросе настроек: {e}")
        await update.message.reply_text(f"Произошла ошибка: {e}")

async def cancel_handle(update: Update, context: CallbackContext):
    try:
        user_id = update.message.from_user.id
        if not await is_authorized(user_id, ["Founder", "Developer"]):
            await update.message.reply_text("У вас нет доступа для выполнения этой команды.")
            return
        await update.message.reply_text("Задача отменена.")
    except Exception as e:
        logger.error(f"Ошибка при отмене задачи: {e}")
        await update.message.reply_text(f"Произошла ошибка: {e}")

async def error_handle(update: Update, context: CallbackContext) -> None:
    logger.error(msg="Exception while handling an update:", exc_info=context.error)

    try:
        tb_list = traceback.format_exception(None, context.error, context.error.__traceback__)
        tb_string = "".join(tb_list)
        update_str = update.to_dict() if isinstance(update, Update) else str(update)
        message = (
            f"An exception was raised while handling an update\n"
            f"<pre>update = {html.escape(json.dumps(update_str, indent=2, ensure_ascii=False))}"
            "</pre>\n\n"
            f"<pre>{html.escape(tb_string)}</pre>"
        )
        for message_chunk in split_text_into_chunks(message, 4096):
            try:
                await context.bot.send_message(update.effective_chat.id, message_chunk, parse_mode=ParseMode.HTML)
            except telegram.error.BadRequest:
                await context.bot.send_message(update.effective_chat.id, message_chunk)
    except:
        await context.bot.send_message(update.effective_chat.id, "Some error in error handler")

async def post_init(application: Application):
    await application.bot.set_my_commands([
        BotCommand("/start", "Приветствие и инструкция"),
        BotCommand("/generate_report", "Генерация отчета"),
        BotCommand("/request_stats", "Запрос текущей статистики"),
        BotCommand("/report_summary", "Сводка по отчетам"),
        BotCommand("/settings", "Настройки"),
        BotCommand("/help", "Помощь"),
        BotCommand("/cancel", "Отмена задачи"),
    ])

def run_bot() -> None:
    application = (
        ApplicationBuilder()
        .token(config.telegram_token)
        .concurrent_updates(True)
        .rate_limiter(AIORateLimiter(max_retries=5))
        .http_version("1.1")
        .get_updates_http_version("1.1")
        .post_init(post_init)
        .build()
    )

    application.add_handler(CommandHandler("start", start_handle))
    application.add_handler(CommandHandler("help", help_handle))
    application.add_handler(CommandHandler("generate_report", generate_report_handle))
    application.add_handler(CommandHandler("request_stats", request_current_stats_handle))
    application.add_handler(CommandHandler("report_summary", report_summary_handle))
    application.add_handler(CommandHandler("settings", settings_handle))
    application.add_handler(CommandHandler("cancel", cancel_handle))

    application.add_error_handler(error_handle)

    application.run_polling()

if __name__ == "__main__":
    run_bot()
