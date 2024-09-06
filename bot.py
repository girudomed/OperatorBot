import asyncio
import logging
import traceback
import html
import json
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from logging.handlers import RotatingFileHandler

import telegram
from telegram import Update, KeyboardButton, ReplyKeyboardMarkup
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackContext,
    CommandHandler,
    AIORateLimiter,
    MessageHandler,
    filters,
    ConversationHandler
)
from telegram.constants import ParseMode

import config
from operator_data import OperatorData
from report_generator import ReportGenerator
from db_helpers import register_user_if_not_exists, create_async_connection, get_user_role, add_user, get_user_password
from kbs import main_kb
from auth import AuthManager

# Инициализация менеджера аутентификации
auth_manager = AuthManager()

# Настройка логирования
log_handler = RotatingFileHandler('logs.log', maxBytes=10**6, backupCount=10)
log_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)

# Инициализация Telegram бота
bot = telegram.Bot(token=config.telegram_token)

# Инициализация генератора отчетов
report_generator = ReportGenerator(model="gpt-4o-mini")

# Инициализация OperatorData с конфигурацией базы данных
operator_data = OperatorData()

# Планировщик задач
scheduler = AsyncIOScheduler()

# Помощь / Команды
HELP_MESSAGE = """Commands:
/start – Приветствие и инструкция
/register – Регистрация нового пользователя
/generate_report [operator_id] – Генерация отчета
/request_stats – Запрос текущей статистики
/help – Показать помощь
/report_summary – Сводка по отчетам
/settings – Показать настройки (опционально)
/cancel – Отменить текущую задачу
"""

# Состояния для процесса регистрации
ASK_NAME, ASK_ROLE, ASK_PASSWORD = range(3)

# Функция для подключения к БД
async def setup_db_connection():
    try:
        await operator_data.create_connection()
        logger.info("Успешное подключение к базе данных.")
    except Exception as e:
        logger.error(f"Ошибка при подключении к базе данных: {e}")
        raise

# Разделение текста на части для отправки длинных сообщений
def split_text_into_chunks(text, chunk_size=4096):
    for i in range(0, len(text), chunk_size):
        yield text[i:i + chunk_size]

# Ежедневная задача
async def daily_report():
    try:
        await setup_db_connection()
        operators = await operator_data.get_all_operators_metrics()
        if not operators:
            logger.warning("Отсутствуют данные операторов для генерации отчета.")
            return
        for operator in operators:
            recommendations = await report_generator.generate_recommendations(operator)
            report = report_generator.create_report(operator_id=operator["operator_id"], operator_data=operator, recommendations=recommendations)
            await bot.send_message(chat_id=config.manager_chat_id, text=report)
        logger.info("Ежедневный отчет успешно отправлен.")
    except Exception as e:
        logger.error(f"Ошибка при выполнении ежедневной задачи: {e}")
    finally:
        await operator_data.close_connection()

# Задачи планировщика
scheduler.add_job(daily_report, 'cron', hour=18, minute=0)
scheduler.add_job(daily_report, 'cron', day_of_week='sun', hour=18, minute=0)
scheduler.add_job(daily_report, 'cron', day=1, hour=18, minute=0)

scheduler.start()

# Команда /start
async def start_handle(update: Update, context: CallbackContext):
    try:
        user_id = update.message.from_user.id
        kb = await main_kb(user_id)
        reply_text = f"Привет! Я бот для генерации отчетов на основе данных операторов.\n\n{HELP_MESSAGE}"
        await update.message.reply_text(reply_text, parse_mode=ParseMode.HTML, reply_markup=kb)
        logger.info(f"Команда /start обработана для пользователя {user_id}.")
    except Exception as e:
        logger.error(f"Ошибка при обработке команды /start: {e}")

# Начало процесса регистрации
async def register_handle(update: Update, context: CallbackContext):
    await update.message.reply_text("Введите ваше ФИО:")
    return ASK_NAME

# Получение ФИО
async def ask_name_handle(update: Update, context: CallbackContext):
    context.user_data['full_name'] = update.message.text
    await update.message.reply_text("Введите вашу роль (например, Operator):")
    return ASK_ROLE

# Получение роли
async def ask_role_handle(update: Update, context: CallbackContext):
    role = update.message.text
    context.user_data['role'] = role

    # Проверка, если пользователь уже существует и у него есть пароль
    user_id = update.message.from_user.id
    connection = await create_async_connection()
    existing_password = await get_user_password(connection, user_id)

    if existing_password:
        await update.message.reply_text(f"Пользователь уже зарегистрирован. Ваш пароль: {existing_password}")
        return ConversationHandler.END
    else:
        # Регистрация пользователя
        full_name = context.user_data['full_name']
        registration_result = await auth_manager.register_user(full_name, role)

        if registration_result["status"] == "success":
            password = registration_result["password"]
            await update.message.reply_text(f"Пользователь зарегистрирован! Ваш пароль: {password}.")
            return ConversationHandler.END
        else:
            await update.message.reply_text(f"Ошибка регистрации: {registration_result['message']}")
            return ConversationHandler.END

# Ввод пароля пользователем
async def password_handle(update: Update, context: CallbackContext):
    user_password = update.message.text
    verification_result = await auth_manager.verify_password(user_password)

    if verification_result["status"] == "success":
        await update.message.reply_text(f"Регистрация завершена! Ваш JWT-токен: {verification_result['token']}")
    else:
        await update.message.reply_text(f"Ошибка: {verification_result['message']}")
    return ConversationHandler.END

# Настройка ConversationHandler для регистрации
registration_handler = ConversationHandler(
    entry_points=[CommandHandler('register', register_handle)],
    states={
        ASK_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_name_handle)],
        ASK_ROLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_role_handle)],
        ASK_PASSWORD: [MessageHandler(filters.TEXT & ~filters.COMMAND, password_handle)],
    },
    fallbacks=[]
)

# Команда /help
async def help_handle(update: Update, context: CallbackContext):
    try:
        await update.message.reply_text(HELP_MESSAGE, parse_mode=ParseMode.HTML)
        logger.info(f"Команда /help обработана для пользователя {update.message.from_user.id}.")
    except Exception as e:
        logger.error(f"Ошибка при обработке команды /help: {e}")

# Генерация отчета
async def generate_report_handle(update: Update, context: CallbackContext):
    try:
        await setup_db_connection()
        operator_id = update.message.text.split()[1]
        operator = await operator_data.get_operator_metrics(operator_id)
        if not operator:
            await update.message.reply_text(f"Оператор с ID {operator_id} не найден.", parse_mode=ParseMode.HTML)
            return
        recommendations = await report_generator.generate_recommendations(operator)
        report = report_generator.create_report(operator_id=operator_id, operator_data=operator, recommendations=recommendations)
        await update.message.reply_text(report, parse_mode=ParseMode.HTML)
        logger.info(f"Отчет с operator_id {operator_id} успешно сгенерирован.")
    except Exception as e:
        logger.error(f"Ошибка при генерации отчета: {e}")
        await update.message.reply_text(f"Ошибка: {e}")
    finally:
        await operator_data.close_connection()

# Команда для запроса текущей статистики
async def request_current_stats_handle(update: Update, context: CallbackContext):
    try:
        await setup_db_connection()
        user_id = update.message.from_user.id
        connection = await create_async_connection()
        role = await get_user_role(connection, user_id)
        operator = await operator_data.get_operator_metrics(user_id)
        if not operator:
            await update.message.reply_text(f"Данные для пользователя {user_id} не найдены.", parse_mode=ParseMode.HTML)
            return
        report = report_generator.create_report(
            operator_id=user_id,
            operator_data=operator,
            recommendations="Ваш текущий отчет"
        )
        await update.message.reply_text(report, parse_mode=ParseMode.HTML)
        logger.info(f"Запрос статистики для пользователя {user_id} обработан.")
    except Exception as e:
        logger.error(f"Ошибка при запросе статистики: {e}")
        await update.message.reply_text(f"Ошибка: {e}")
    finally:
        await operator_data.close_connection()

# Обработка ошибок
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

        if update.effective_chat:
            for message_chunk in split_text_into_chunks(message, 4096):
                await context.bot.send_message(update.effective_chat.id, message_chunk, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка в обработчике ошибок: {e}")
        if update and update.effective_chat:
            await context.bot.send_message(update.effective_chat.id, f"Произошла ошибка: {e}")

# Главная функция
def main():
    application = (
        ApplicationBuilder()
        .token(config.telegram_token)
        .rate_limiter(AIORateLimiter())
        .build()
    )

    application.add_handler(CommandHandler("start", start_handle))
    application.add_handler(CommandHandler("help", help_handle))
    application.add_handler(CommandHandler("generate_report", generate_report_handle))
    application.add_handler(CommandHandler("request_stats", request_current_stats_handle))
    application.add_handler(registration_handler)
    application.add_error_handler(error_handle)

    application.run_polling()

if __name__ == '__main__':
    main()
