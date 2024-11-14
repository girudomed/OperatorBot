import logging
import traceback
import html
import json
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

from bot import split_text_into_chunks
import config
from operator_data import OperatorData
from admin_utils import get_user_role, is_authorized
from logger_utils import setup_logging
import time  # Для замера времени выполнения

# Настройка логирования
logger = setup_logging()

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
/generate_report <user_id> – Генерация отчета
/request_stats – Запрос текущей статистики
/help – Показать помощь
/report_summary – Сводка по отчетам
/settings – Показать настройки (опционально)
/cancel – Отменить текущую задачу
"""

# Функция для загрузки ролей из файла roles.json
def load_roles():
    with open("roles.json", "r") as file:
        roles_data = json.load(file)
    return roles_data

# Загрузка ролей при инициализации
roles_data = {}

async def post_init(application: Application):
    global roles_data
    roles_data = load_roles()  # Загрузка ролей из JSON
    
    await application.bot.set_my_commands([
        BotCommand("/start", "Приветствие и инструкция"),
        BotCommand("/generate_report", "Генерация отчета"),
        BotCommand("/request_stats", "Запрос текущей статистики"),
        BotCommand("/report_summary", "Сводка по отчетам"),
        BotCommand("/settings", "Настройки"),
        BotCommand("/help", "Помощь"),
        BotCommand("/cancel", "Отмена задачи"),
    ])

# Декоратор для проверки прав доступа на основе разрешений
def authorized(required_permissions):
    async def decorator(func):
        async def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
            user_id = update.message.from_user.id
            user_role = await get_user_role(user_id)  # Получаем роль пользователя
            if not user_role:
                await update.message.reply_text("Роль пользователя не найдена.")
                return

            role_permissions = roles_data.get(user_role, {}).get("permissions", [])

            # Проверка наличия необходимых разрешений
            if not any(permission in role_permissions for permission in required_permissions):
                await update.message.reply_text("У вас нет доступа для выполнения этой команды.")
                return

            return await func(update, context, *args, **kwargs)
        return wrapper
    return decorator

# Ежедневная задача
async def daily_report():
    start_time = time.time()
    try:
        logger.info("[КРОТ]: Начало генерации ежедневного отчета...")
        operators = await operator_data.get_all_operators_metrics()  # Асинхронный вызов
        for operator in operators:
            recommendations = await report_generator.generate_recommendations(operator)  # Асинхронный вызов
            report = await report_generator.create_report(user_id=operator["user_id"], operator_data=operator, recommendations=recommendations)  # Асинхронный вызов
            await bot.send_message(chat_id=config.manager_chat_id, text=report)
        elapsed_time = time.time() - start_time
        logger.info(f"[КРОТ]: Ежедневный отчет успешно отправлен (Время выполнения: {elapsed_time:.4f} сек).")
    except Exception as e:
        logger.error(f"[КРОТ]: Ошибка при генерации ежедневного отчета: {e}")

# Еженедельная задача
async def weekly_report():
    try:
        logger.info("[КРОТ]: Генерация еженедельного отчета...")
        await bot.send_message(chat_id=config.manager_chat_id, text="Еженедельный отчет...")
        logger.info("[КРОТ]: Еженедельный отчет успешно отправлен.")
    except Exception as e:
        logger.error(f"[КРОТ]: Ошибка при отправке еженедельного отчета: {e}")

# Ежемесячная задача
async def monthly_report():
    try:
        logger.info("[КРОТ]: Генерация ежемесячного отчета...")
        await bot.send_message(chat_id=config.manager_chat_id, text="Ежемесячный отчет...")
        logger.info("[КРОТ]: Ежемесячный отчет успешно отправлен.")
    except Exception as e:
        logger.error(f"[КРОТ]: Ошибка при отправке ежемесячного отчета: {e}")

# Планирование задач
scheduler.add_job(daily_report, 'cron', hour=18, minute=0)  # Каждый день в 18:00
scheduler.add_job(weekly_report, 'cron', day_of_week='sun', hour=18, minute=0)  # Каждое воскресенье в 18:00
scheduler.add_job(monthly_report, 'cron', day=1, hour=18, minute=0)  # Первого числа каждого месяца в 18:00

# Запуск планировщика
scheduler.start()

# Хендлеры команд
async def start_handle(update: Update, context: CallbackContext):
    reply_text = "Привет! Я бот для генерации отчетов на основе данных операторов.\n\n"
    reply_text += HELP_MESSAGE
    await update.message.reply_text(reply_text, parse_mode=ParseMode.HTML)

async def help_handle(update: Update, context: CallbackContext):
    await update.message.reply_text(HELP_MESSAGE, parse_mode=ParseMode.HTML)

@authorized(["view_reports", "manage_operators"])
async def generate_report_handle(update: Update, context: CallbackContext):
    try:
        start_time = time.time()

        # Парсинг аргументов команды
        command_args = update.message.text.split()

        # Проверка на минимальное количество аргументов: должно быть хотя бы два (команда и user_id)
        if len(command_args) < 2:
            await update.message.reply_text(
                "Пожалуйста, укажите user_id и опционально период (daily, weekly, monthly и т.д.).",
                parse_mode=ParseMode.HTML
            )
            return

        # Извлечение user_id
        user_id = command_args[1]

        # Если указан период, используем его, иначе по умолчанию "daily"
        if len(command_args) > 2:
            period = command_args[2].lower()
        else:
            period = "daily"

        # Проверка валидности периода
        valid_periods = ['daily', 'weekly', 'biweekly', 'monthly', 'half_year', 'yearly']
        if period not in valid_periods:
            await update.message.reply_text(
                f"Некорректный период. Допустимые значения: {', '.join(valid_periods)}.",
                parse_mode=ParseMode.HTML
            )
            return

        # Получение метрик оператора за указанный период
        operator = await operator_data.get_operator_metrics(user_id, period)
        if not operator:
            await update.message.reply_text(
                f"Пользователь с ID {user_id} за период {period} не найден.",
                parse_mode=ParseMode.HTML
            )
            return

        # Генерация рекомендаций и отчета
        recommendations = await report_generator.generate_recommendations(operator)
        report = await report_generator.create_report(
            user_id=user_id, 
            operator_data=operator, 
            recommendations=recommendations
        )

        # Отправка отчета пользователю
        await update.message.reply_text(report, parse_mode=ParseMode.HTML)

        # Логируем успешную генерацию отчета
        elapsed_time = time.time() - start_time
        logger.info(f"[КРОТ]: Отчет для пользователя {user_id} за период {period} успешно сгенерирован (Время выполнения: {elapsed_time:.4f} сек).")

    except IndexError:
        # Если недостаточно аргументов в команде
        await update.message.reply_text(
            "Пожалуйста, укажите user_id и период после команды /generate_report.",
            parse_mode=ParseMode.HTML
        )

    except Exception as e:
        # Логируем любую непредвиденную ошибку
        logger.error(f"[КРОТ]: Ошибка при генерации отчета: {traceback.format_exc()}")
        await update.message.reply_text(
            f"Произошла ошибка: {e}",
            parse_mode=ParseMode.HTML
        )

@authorized(["view_reports"])
async def report_summary_handle(update: Update, context: CallbackContext):
    try:
        await update.message.reply_text("Сводка по отчетам...")
    except Exception as e:
        logger.error(f"[КРОТ]: Ошибка при запросе сводки по отчетам: {traceback.format_exc()}")
        await update.message.reply_text(f"Произошла ошибка: {e}")

@authorized(["manage_settings"])
async def settings_handle(update: Update, context: CallbackContext):
    try:
        await update.message.reply_text("Текущие настройки...")
    except Exception as e:
        logger.error(f"[КРОТ]: Ошибка при запросе настроек: {traceback.format_exc()}")
        await update.message.reply_text(f"Произошла ошибка: {e}")

@authorized(["view_own_reports"])
async def request_current_stats_handle(update: Update, context: CallbackContext):
    try:
        user_id = update.message.from_user.id
        operator = await operator_data.get_operator_metrics(user_id)  # Асинхронный вызов
        if not operator:
            await update.message.reply_text("Данные для вашего аккаунта не найдены.")
            return

        report = await report_generator.create_report(
            user_id=user_id, 
            operator_data=operator, 
            recommendations="Ваш текущий отчет"
        )
        await update.message.reply_text(report, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"[КРОТ]: Ошибка при запросе статистики: {traceback.format_exc()}")
        await update.message.reply_text(f"Произошла ошибка: {e}")

@authorized(["manage_settings"])
async def cancel_handle(update: Update, context: CallbackContext):
    try:
        await update.message.reply_text("Задача отменена.")
    except Exception as e:
        logger.error(f"[КРОТ]: Ошибка при отмене задачи: {traceback.format_exc()}")
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
