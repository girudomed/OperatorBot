import asyncio
import logging
import traceback
import html
import json
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from logging.handlers import RotatingFileHandler

import telegram
from telegram import Update, BotCommand
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
from openai_telebot import OpenAIReportGenerator
from db_helpers import create_async_connection, get_user_password, find_operator_by_name
from auth import AuthManager

import nest_asyncio
nest_asyncio.apply()

# Проверка переменной окружения telegram_token
if not config.telegram_token:
    raise ValueError("Telegram token отсутствует в конфигурации")

# Инициализация менеджера аутентификации
auth_manager = AuthManager()

# Инициализация OpenAIReportGenerator для генерации отчетов
openai_report_generator = OpenAIReportGenerator()

# Настройка логирования
log_handler = RotatingFileHandler('logs.log', maxBytes=10**6, backupCount=10)
log_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)

# Инициализация Telegram бота
bot = telegram.Bot(token=config.telegram_token)

# Планировщик задач
scheduler = AsyncIOScheduler()

# Команды
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

# Функция для установки команд бота через Telegram API
async def set_bot_commands(application):
    commands = [
        BotCommand("/start", "Запуск бота"),
        BotCommand("/register", "Регистрация нового пользователя"),
        BotCommand("/generate_report", "Генерация отчета"),
        BotCommand("/request_stats", "Запрос текущей статистики"),
        BotCommand("/help", "Показать помощь"),
        BotCommand("/report_summary", "Сводка по всем операторам"),
        BotCommand("/settings", "Показать настройки"),
        BotCommand("/cancel", "Отменить текущую задачу")
    ]
    await application.bot.set_my_commands(commands)

# Объявляем состояние ASK_OPERATOR_ID для ConversationHandler
ASK_OPERATOR_ID = range(1)

# Функция для подключения к БД с экспоненциальной задержкой
async def setup_db_connection(retries=3, delay=2):
    for attempt in range(retries):
        try:
            connection = await create_async_connection()
            logger.info("Успешное подключение к базе данных.")
            return connection
        except Exception as e:
            logger.error(f"Ошибка подключения: {e}, попытка {attempt + 1} из {retries}")
            if attempt < retries - 1:
                await asyncio.sleep(delay * (attempt + 1))
            else:
                raise

# Разделение текста на части для отправки длинных сообщений
def split_text_into_chunks(text, chunk_size=4096):
    if len(text) <= chunk_size:
        yield text
        return
    while len(text) > chunk_size:
        split_at = text.rfind('\n', 0, chunk_size)
        if split_at == -1:
            split_at = chunk_size
        yield text[:split_at]
        text = text[split_at:]
    yield text

# Функция отправки отчета по частям
async def send_report(report, chat_id):
    """Отправка отчета по частям, если он слишком длинный"""
    if len(report) > 4096:
        for chunk in split_text_into_chunks(report):
            await bot.send_message(chat_id=chat_id, text=chunk)
    else:
        await bot.send_message(chat_id=chat_id, text=report)

# Функция для сохранения отчета в БД
async def save_report_to_db(user_id, report_data):
    connection = await create_async_connection()
    try:
        query = """
        INSERT INTO reports (user_id, report_date, total_calls, accepted_calls, booked_services, conversion_rate, 
                             avg_call_rating, total_cancellations, cancellation_rate, total_conversation_time, 
                             avg_conversation_time, avg_spam_time, total_spam_time, total_navigation_time, 
                             avg_navigation_time, total_talk_time, complaint_calls, complaint_rating, recommendations)
        VALUES (%s, CURRENT_DATE, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        async with connection.cursor() as cursor:
            await cursor.execute(query, (
                user_id,
                report_data['total_calls'],
                report_data['accepted_calls'],
                report_data['booked_services'],
                report_data['conversion_rate'],
                report_data['avg_call_rating'],
                report_data['total_cancellations'],
                report_data['cancellation_rate'],
                report_data['total_conversation_time'],
                report_data['avg_conversation_time'],
                report_data['avg_spam_time'],
                report_data['total_spam_time'],
                report_data['total_navigation_time'],
                report_data['avg_navigation_time'],
                report_data['total_talk_time'],
                report_data['complaint_calls'],
                report_data['complaint_rating'],
                report_data['recommendations']
            ))
            await connection.commit()
        logger.info(f"Отчет для пользователя с user_id {user_id} сохранен в базе данных.")
    except Exception as e:
        logger.error(f"Ошибка при сохранении отчета для пользователя с user_id {user_id}: {e}")
    finally:
        await connection.ensure_closed()

# Команда /cancel для отмены задач
async def cancel_handle(update: Update, context: CallbackContext):
    """Отмена текущей задачи или диалога"""
    user_id = update.message.from_user.id
    logger.info(f"Команда /cancel получена от пользователя {user_id}")
    
    if "conversation_data" in context.user_data:
        context.user_data.clear()
        await update.message.reply_text("Текущая задача отменена.")
        logger.info(f"Задача или диалог для пользователя {user_id} отменена.")
    else:
        await update.message.reply_text("Нет активных задач для отмены.")
        logger.info(f"У пользователя {user_id} не было активных задач для отмены.")

# Команда /start
async def start_handle(update: Update, context: CallbackContext):
    try:
        user_id = update.message.from_user.id
        logger.info(f"Команда /start получена от пользователя {user_id}")
        reply_text = f"Привет! Я бот для генерации отчетов на основе данных операторов.\n\n{HELP_MESSAGE}"
        await update.message.reply_text(reply_text, parse_mode=ParseMode.HTML)
        logger.info(f"Команда /start обработана для пользователя {user_id}.")
    except Exception as e:
        logger.error(f"Ошибка при обработке команды /start: {e}")

# Начало процесса регистрации
async def register_handle(update: Update, context: CallbackContext):
    """Запрос имени оператора для регистрации"""
    try:
        user_id = update.message.from_user.id
        logger.info(f"Команда /register получена от пользователя {user_id}")
        await update.message.reply_text("Введите ваше имя оператора (для идентификации в системе звонков):")
        return ASK_OPERATOR_ID
    except Exception as e:
        logger.error(f"Ошибка при обработке команды /register: {e}")

# Получение имени оператора и поиск по базе
async def ask_operator_id_handle(update: Update, context: CallbackContext):
    connection = None
    try:
        operator_name = update.message.text
        context.user_data['operator_name'] = operator_name

        connection = await setup_db_connection()
        if not connection:
            await update.message.reply_text("Ошибка подключения к базе данных.")
            return

        # Проверяем, существует ли оператор в базе данных
        result = await find_operator_by_name(operator_name)
        if result:
            operator_id = result['user_id']
            extension = result['extension']
            context.user_data['operator_id'] = operator_id
            context.user_data['extension'] = extension
            await update.message.reply_text(f"Оператор найден: {operator_name} с extension {extension}.")
        else:
            context.user_data['operator_id'] = None
            await update.message.reply_text(f"Оператор с именем {operator_name} не найден. Данные будут сохранены без оператора.")

        # Проверка, если пользователь уже существует и у него есть пароль
        user_id = update.message.from_user.id
        existing_password = await get_user_password(user_id)

        if existing_password:
            await update.message.reply_text(f"Пользователь уже зарегистрирован. Ваш пароль: {existing_password}")
            return ConversationHandler.END
        else:
            # Регистрация пользователя
            operator_id = context.user_data['operator_id']  # Может быть None
            registration_result = await auth_manager.register_user(user_id, operator_name, "Operator", operator_id)

            if registration_result["status"] == "success":
                password = registration_result["password"]
                await update.message.reply_text(f"Пользователь зарегистрирован! Ваш пароль: {password}.")
                return ConversationHandler.END
            else:
                await update.message.reply_text(f"Ошибка регистрации: {registration_result['message']}")
                return ConversationHandler.END
    except Exception as e:
        logger.error(f"Ошибка в ask_operator_id_handle: {e}")
    finally:
        if connection:
            await connection.ensure_closed()

# Настройка ConversationHandler для регистрации
registration_handler = ConversationHandler(
    entry_points=[CommandHandler('register', register_handle)],
    states={
        ASK_OPERATOR_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_operator_id_handle)],
    },
    fallbacks=[]
)

# Команда /help
async def help_handle(update: Update, context: CallbackContext):
    try:
        user_id = update.message.from_user.id
        logger.info(f"Команда /help получена от пользователя {user_id}")
        await update.message.reply_text(HELP_MESSAGE, parse_mode=ParseMode.HTML)
        logger.info(f"Команда /help обработана для пользователя {user_id}.")
    except Exception as e:
        logger.error(f"Ошибка при обработке команды /help: {e}")

# Функция генерации текста отчета
def generate_report_text(report_data):
    """Генерация текста отчета по шаблону на основе данных"""
    report_text = f"""
📊 Ежедневный отчет за {report_data['report_date']}

1. Общая статистика по звонкам:
   - Всего звонков за день: {report_data['total_calls']}
   - Принято звонков за день: {report_data['accepted_calls']}
   - Записаны на услугу: {report_data['booked_services']}
   - Конверсия в запись от общего числа звонков: {report_data['conversion_rate']}%

2. Качество обработки звонков:
   - Оценка разговоров (средняя по всем клиентам): {report_data['avg_call_rating']} из 10

3. Анализ отмен и ошибок:
   - Совершено отмен: {report_data['total_cancellations']}
   - Доля отмен от всех звонков: {report_data['cancellation_rate']}%

4. Время обработки и разговоров:
   - Общее время разговора при записи: {report_data['total_conversation_time']} мин.
   - Среднее время разговора при записи: {report_data['avg_conversation_time']} мин.
   - Среднее время разговора со спамом: {report_data['avg_spam_time']} мин.
   - Общее время разговора со спамом: {report_data['total_spam_time']} мин.
   - Общее время навигации звонков: {report_data['total_navigation_time']} мин.
   - Среднее время навигации звонков: {report_data['avg_navigation_time']} мин.
   - Общее время разговоров по телефону: {report_data['total_talk_time']} мин.

5. Работа с жалобами:
   - Звонки с жалобами: {report_data['complaint_calls']}
   - Оценка обработки жалобы: {report_data['complaint_rating']} из 10

6. Рекомендации на основе данных:
   {report_data['recommendations']}
    """
    return report_text

# Генерация отчета через команду /generate_report
async def generate_report_handle(update: Update, context: CallbackContext):
    try:
        logger.info(f"Команда /generate_report получена")
        command_parts = update.message.text.split()
        if len(command_parts) < 2:
            await update.message.reply_text("Пожалуйста, укажите ID оператора после команды /generate_report.")
            return

        operator_id = command_parts[1]

        if not operator_id.isdigit() or int(operator_id) <= 0:
            await update.message.reply_text("Некорректный операторский ID. Убедитесь, что вы ввели положительное число.")
            return

        report_data = await openai_report_generator.generate_report(operator_id)
        
        # Сохраняем отчет в базу данных
        await save_report_to_db(operator_id, report_data)

        # Отправляем отчет пользователю
        report_text = generate_report_text(report_data)
        await send_report(report_text, update.message.chat_id)
        logger.info(f"Отчет с operator_id {operator_id} успешно сгенерирован и сохранен в БД.")
    except Exception as e:
        logger.error(f"Ошибка при генерации отчета: {e}")
        await update.message.reply_text(f"Ошибка: {e}")

# Команда для запроса текущей статистики
async def request_current_stats_handle(update: Update, context: CallbackContext):
    connection = None
    try:
        logger.info(f"Команда /request_stats получена")
        connection = await setup_db_connection()
        user_id = update.message.from_user.id
        report = await openai_report_generator.generate_report(user_id)
        await send_report(report, update.message.chat_id)
        logger.info(f"Запрос статистики для пользователя {user_id} обработан.")
    except Exception as e:
        logger.error(f"Ошибка при запросе статистики: {e}")
        await update.message.reply_text(f"Ошибка: {e}")
    finally:
        if connection:
            await connection.ensure_closed()

# Команда /report_summary
async def report_summary_handle(update: Update, context: CallbackContext):
    """Сводка по всем операторам"""
    connection = None
    try:
        logger.info(f"Команда /report_summary получена от пользователя {update.message.from_user.id}")
        connection = await setup_db_connection()

        operators = await OperatorData().get_all_operators_metrics()
        if not operators:
            await update.message.reply_text("Нет данных для отчета.")
            return

        report = ""
        for operator in operators:
            report_data = await openai_report_generator.generate_report(operator["operator_id"])
            report += generate_report_text(report_data) + "\n"

        await send_report(report, update.message.chat_id)
        logger.info("Сводка по всем операторам успешно отправлена.")
    except Exception as e:
        logger.error(f"Ошибка при создании сводки по операторам: {e}")
        await update.message.reply_text(f"Ошибка при создании сводки: {e}")
    finally:
        if connection:
            await connection.ensure_closed()

# Команда /settings
async def settings_handle(update: Update, context: CallbackContext):
    """Показ настроек бота (опционально)"""
    try:
        user_id = update.message.from_user.id
        logger.info(f"Команда /settings получена от пользователя {user_id}")

        settings = {
            "language": "Русский",
            "timezone": "UTC+3",
            "notifications": "Включены"
        }

        settings_message = f"Настройки бота:\nЯзык: {settings['language']}\nЧасовой пояс: {settings['timezone']}\nУведомления: {settings['notifications']}"
        await update.message.reply_text(settings_message)
        logger.info(f"Настройки успешно отправлены пользователю {user_id}")
    except Exception as e:
        logger.error(f"Ошибка при обработке команды /settings: {e}")

# Обработка ошибок
async def error_handle(update: Update, context: CallbackContext) -> None:
    logger.error(msg="Exception while handling an update:", exc_info=context.error)
    
    try:
        tb_list = traceback.format_exception(None, context.error, context.error.__traceback__)
        tb_string = "".join(tb_list)
        update_str = update.to_dict() if isinstance(update, Update) else str(update)
        message = (
            f"An exception was raised while handling an update\n"
            f"<pre>update = {html.escape(json.dumps(update_str, indent=2, ensure_ascii=False))}</pre>\n\n"
            f"<pre>{html.escape(tb_string)}</pre>"
        )

        if update and update.effective_chat:
            for message_chunk in split_text_into_chunks(message, 4096):
                await context.bot.send_message(update.effective_chat.id, message_chunk, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка в обработчике ошибок: {e}")

# Отправка отчетов операторам в конце рабочего дня
async def send_daily_reports():
    connection = await create_async_connection()
    try:
        query = """
        SELECT user_id, report_text 
        FROM reports 
        WHERE report_date = CURRENT_DATE
        """
        async with connection.cursor() as cursor:
            await cursor.execute(query)
            reports = await cursor.fetchall()

        for report in reports:
            user_id = report['user_id']
            report_text = report['report_text']
            
            # Получение Telegram user_id из таблицы UsersTelegaBot
            query_telegram_user = "SELECT user_id FROM UsersTelegaBot WHERE user_id = %s"
            async with connection.cursor() as cursor:
                await cursor.execute(query_telegram_user, (user_id,))
                telegram_user = await cursor.fetchone()
                
            if telegram_user:
                telegram_user_id = telegram_user['user_id']
                await bot.send_message(chat_id=telegram_user_id, text=report_text)
                logger.info(f"Отчет отправлен пользователю с user_id {telegram_user_id}.")
    except Exception as e:
        logger.error(f"Ошибка при отправке отчетов: {e}")
    finally:
        await connection.ensure_closed()

# Добавляем задачу в планировщик
scheduler.add_job(send_daily_reports, 'cron', hour=18, minute=0)

async def main():
    application = (
        ApplicationBuilder()
        .token(config.telegram_token)
        .rate_limiter(AIORateLimiter())
        .build()
    )

    # Установка команд для бота
    await set_bot_commands(application)

    application.add_handler(CommandHandler("start", start_handle))
    application.add_handler(CommandHandler("help", help_handle))
    application.add_handler(CommandHandler("generate_report", generate_report_handle))
    application.add_handler(CommandHandler("request_stats", request_current_stats_handle))
    application.add_handler(CommandHandler("report_summary", report_summary_handle))
    application.add_handler(CommandHandler("settings", settings_handle))
    application.add_handler(CommandHandler("cancel", cancel_handle))
    application.add_handler(registration_handler)
    application.add_error_handler(error_handle)

    logger.info("Бот инициализирован, пытаемся запустить run_polling")

    # Запуск polling
    await application.run_polling()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    if not scheduler.running:  # Проверяем, что планировщик не запущен, перед запуском
        scheduler.start()  # Запускаем планировщик

    try:
        loop = asyncio.get_event_loop()  # Получаем существующий event loop
        loop.run_until_complete(main())  # Запускаем основную функцию в текущем event loop
    except RuntimeError as e:
        logger.error(f"Ошибка при запуске event loop: {e}")
