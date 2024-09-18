import asyncio
import logging
import traceback
import html
import json
from datetime import datetime
from logging.handlers import RotatingFileHandler

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Update, BotCommand
from telegram.ext import (
    ApplicationBuilder,
    CallbackContext,
    CommandHandler,
    AIORateLimiter,
)
from telegram.constants import ParseMode

import config
from operator_data import OperatorData
from openai_telebot import OpenAIReportGenerator
from db_module import DatabaseManager
from auth import setup_auth_handlers
import nest_asyncio

nest_asyncio.apply()

# Настройка логирования
log_handler = RotatingFileHandler('logs.log', maxBytes=10**6, backupCount=10)
log_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)

# Команда помощи
HELP_MESSAGE = """Команды:
/start – Приветствие и инструкция
/register – Регистрация нового пользователя
/generate_report [user_id] – Генерация отчета
/request_stats – Запрос текущей статистики
/help – Показать помощь
/report_summary – Сводка по отчетам
/settings – Показать настройки
/cancel – Отменить текущую задачу
"""

def split_text_into_chunks(text, chunk_size=4096):
    """Разделение текста на части для отправки длинных сообщений."""
    for i in range(0, len(text), chunk_size):
        yield text[i:i + chunk_size]

class TelegramBot:
    def __init__(self, token):
        self.token = token
        self.application = ApplicationBuilder().token(token).rate_limiter(AIORateLimiter()).build()
        self.db_manager = DatabaseManager()
        self.scheduler = AsyncIOScheduler()
        self.operator_data = OperatorData(self.db_manager)
        self.report_generator = OpenAIReportGenerator(self.db_manager)
        # Настройка обработчиков аутентификации
        setup_auth_handlers(self.application, self.db_manager)

    async def setup(self):
        """Инициализация бота и всех компонентов."""
        await self.set_bot_commands()
        self.setup_handlers()
        await self.setup_db_connection()
        if not self.scheduler.running:
            self.scheduler.start()
        self.scheduler.add_job(self.send_daily_reports, 'cron', hour=18, minute=0)
        logger.info("Бот успешно инициализирован.")

    async def setup_db_connection(self, retries=3, delay=2):
        """Настройка подключения к базе данных с повторными попытками."""
        for attempt in range(retries):
            try:
                await self.db_manager.create_pool()
                logger.info("Успешное подключение к базе данных.")
                return
            except Exception as e:
                logger.error(f"Ошибка подключения к БД: {e}, попытка {attempt + 1} из {retries}")
                if attempt < retries - 1:
                    await asyncio.sleep(delay * (attempt + 1))
                else:
                    raise

    async def set_bot_commands(self):
        """Установка команд бота."""
        commands = [
            BotCommand("/start", "Запуск бота"),
            BotCommand("/register", "Регистрация нового пользователя"),
            BotCommand("/generate_report", "Генерация отчета"),
            BotCommand("/request_stats", "Запрос текущей статистики"),
            BotCommand("/help", "Показать помощь"),
            BotCommand("/report_summary", "Сводка по отчетам"),
            BotCommand("/settings", "Показать настройки"),
            BotCommand("/cancel", "Отменить текущую задачу"),
        ]
        await self.application.bot.set_my_commands(commands)

    def setup_handlers(self):
        """Настройка обработчиков команд."""
        self.application.add_handler(CommandHandler("start", self.start_handle))
        self.application.add_handler(CommandHandler("help", self.help_handle))
        self.application.add_handler(CommandHandler("generate_report", self.generate_report_handle))
        self.application.add_handler(CommandHandler("request_stats", self.request_current_stats_handle))
        self.application.add_handler(CommandHandler("report_summary", self.report_summary_handle))
        self.application.add_handler(CommandHandler("settings", self.settings_handle))
        self.application.add_handler(CommandHandler("cancel", self.cancel_handle))
        self.application.add_error_handler(self.error_handle)

    async def start_handle(self, update: Update, context: CallbackContext):
        """Обработчик команды /start."""
        user_id = update.effective_user.id
        logger.info(f"Команда /start получена от пользователя {user_id}")
        reply_text = f"Привет! Я бот для генерации отчетов на основе данных операторов.\n\n{HELP_MESSAGE}"
        await update.message.reply_text(reply_text, parse_mode=ParseMode.HTML)

    async def help_handle(self, update: Update, context: CallbackContext):
        """Обработчик команды /help."""
        user_id = update.effective_user.id
        logger.info(f"Команда /help получена от пользователя {user_id}")
        await update.message.reply_text(HELP_MESSAGE, parse_mode=ParseMode.HTML)

    async def cancel_handle(self, update: Update, context: CallbackContext):
        """Обработчик команды /cancel для отмены текущей задачи."""
        user_id = update.effective_user.id
        logger.info(f"Команда /cancel получена от пользователя {user_id}")

        if context.user_data:
            context.user_data.clear()
            await update.message.reply_text("Текущая задача отменена.")
            logger.info(f"Задача для пользователя {user_id} отменена.")
        else:
            await update.message.reply_text("Нет активных задач для отмены.")

    async def generate_report_handle(self, update: Update, context: CallbackContext):
        """Обработчик команды /generate_report для генерации отчета."""
        user_id = update.effective_user.id
        logger.info(f"Команда /generate_report получена от пользователя {user_id}")
        
        operator_id = await self.get_operator_id_from_command(update)
        if operator_id is None:
            await update.message.reply_text("Пожалуйста, укажите ID пользователя после команды /generate_report.")
            return

        if not self.is_valid_operator_id(operator_id):
            await update.message.reply_text("Некорректный ID пользователя. Убедитесь, что вы ввели положительное число.")
            return

        # Асинхронная генерация отчета
        try:
            logger.info(f"Запрашиваем данные для оператора с ID {operator_id}")
            report_data = await self.report_generator.generate_report(operator_id)
            if report_data is None:
                await update.message.reply_text(f"Данные по оператору с ID {operator_id} не найдены.")
                logger.error(f"[КРОТ]: Данные по оператору с ID {operator_id} не найдены.")
                return

            # Логируем данные перед проверкой формата
            logger.debug(f"Полученные данные для оператора {operator_id}: {report_data}")
            
            if not isinstance(report_data, dict):
                await update.message.reply_text("Ошибка при генерации отчета. Ожидался формат словаря.")
                logger.error(f"Неправильный формат данных отчета для пользователя {operator_id}: {report_data}")
                return

            # Сохранение данных и отправка отчета
            await self.save_report_to_db(user_id, report_data)
            report_text = self.generate_report_text(report_data)
            await self.send_long_message(update.effective_chat.id, report_text)
            logger.info(f"Отчет для пользователя {operator_id} успешно сгенерирован и отправлен.")
        except Exception as e:
            logger.error(f"Ошибка при генерации отчета для пользователя {operator_id}: {e}")
            await update.message.reply_text("Произошла ошибка при генерации отчета.")

    async def get_operator_id_from_command(self, update: Update):
        """Извлекаем ID оператора из команды"""
        command_parts = update.message.text.strip().split()
        return command_parts[1] if len(command_parts) >= 2 else None

    def is_valid_operator_id(self, operator_id):
        """Проверяем, что ID оператора корректный"""
        return operator_id.isdigit() and int(operator_id) > 0

    async def save_report_to_db(self, user_id, report_data):
        """Сохраняем отчет в базу данных"""
        await self.db_manager.save_report_to_db(
            user_id,
            report_data.get('total_calls', 0),
            report_data.get('accepted_calls', 0),
            report_data.get('booked_services', 0),
            report_data.get('conversion_rate', 0),
            report_data.get('avg_call_rating', 0),
            report_data.get('total_cancellations', 0),
            report_data.get('cancellation_rate', 0),
            report_data.get('total_conversation_time', 0),
            report_data.get('avg_conversation_time', 0),
            report_data.get('avg_spam_time', 0),
            report_data.get('total_spam_time', 0),
            report_data.get('total_navigation_time', 0),
            report_data.get('avg_navigation_time', 0),
            report_data.get('total_talk_time', 0),
            report_data.get('complaint_calls', 0),
            report_data.get('complaint_rating', 0),
            report_data.get('recommendations', '')
        )

    async def request_current_stats_handle(self, update: Update, context: CallbackContext):
        """Обработчик команды /request_stats для получения текущей статистики."""
        user_id = update.effective_user.id
        logger.info(f"Команда /request_stats получена от пользователя {user_id}")

        operator_data = await self.db_manager.get_user_by_id(user_id)
        if not operator_data:
            await update.message.reply_text("Ваш ID пользователя не найден. Пожалуйста, зарегистрируйтесь с помощью команды /register.")
            return

        # Асинхронная генерация отчета
        try:
            report_data = await self.report_generator.generate_report(user_id)
            if report_data is None:
                await update.message.reply_text(f"Данные по оператору с ID {user_id} не найдены.")
                logger.error(f"[КРОТ]: Данные по оператору с ID {user_id} не найдены.")
                return

            if not isinstance(report_data, dict):
                await update.message.reply_text("Ошибка при получении статистики. Ожидался формат словаря.")
                logger.error(f"Неправильный формат данных статистики для пользователя {user_id}: {report_data}")
                return

            report_text = self.generate_report_text(report_data)
            await self.send_long_message(update.effective_chat.id, report_text)
            logger.info(f"Статистика для пользователя {user_id} успешно отправлена.")
        except Exception as e:
            logger.error(f"Ошибка при получении статистики для пользователя {user_id}: {e}")
            await update.message.reply_text("Произошла ошибка при получении статистики.")

    async def report_summary_handle(self, update: Update, context: CallbackContext):
        """Обработчик команды /report_summary для сводки по всем пользователям."""
        user_id = update.effective_user.id
        logger.info(f"Команда /report_summary получена от пользователя {user_id}")

        try:
            operators = await self.operator_data.get_all_operators_metrics()
            if not operators:
                await update.message.reply_text("Нет данных для отчета.")
                return

            tasks = [self.report_generator.generate_report(op['user_id']) for op in operators]
            reports_data = await asyncio.gather(*tasks)

            report_texts = [self.generate_report_text(report_data) for report_data in reports_data if isinstance(report_data, dict)]
            full_report = "\n".join(report_texts)
            await self.send_long_message(update.effective_chat.id, full_report)
            logger.info("Сводка по всем пользователям успешно отправлена.")
        except Exception as e:
            logger.error(f"Ошибка при создании сводки по пользователям: {e}")
            await update.message.reply_text("Произошла ошибка при создании сводки.")

    async def settings_handle(self, update: Update, context: CallbackContext):
        """Обработчик команды /settings для отображения настроек."""
        user_id = update.effective_user.id
        logger.info(f"Команда /settings получена от пользователя {user_id}")

        settings = {
            "language": "Русский",
            "timezone": "UTC+3",
            "notifications": "Включены",
        }

        settings_message = (
            f"Настройки бота:\n"
            f"Язык: {settings['language']}\n"
            f"Часовой пояс: {settings['timezone']}\n"
            f"Уведомления: {settings['notifications']}"
        )
        await update.message.reply_text(settings_message)

    async def error_handle(self, update: Update, context: CallbackContext):
        """Централизованная обработка ошибок."""
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
                for message_chunk in split_text_into_chunks(message):
                    await context.bot.send_message(update.effective_chat.id, message_chunk, parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.error(f"Ошибка в обработчике ошибок: {e}")

    async def send_daily_reports(self):
        """Отправка ежедневных отчетов в конце рабочего дня."""
        logger.info("Начата отправка ежедневных отчетов.")
        try:
            operators = await self.operator_data.get_all_operators_metrics()
            tasks = []
            for operator in operators:
                user_id = operator['user_id']
                tasks.append(self.report_generator.generate_report(user_id))

            reports_data = await asyncio.gather(*tasks)
            for operator, report_data in zip(operators, reports_data):
                if report_data is None:
                    logger.error(f"Данные по оператору с ID {operator['user_id']} не найдены.")
                    continue
                
                if isinstance(report_data, dict):
                    user_id = operator['user_id']
                    report_text = self.generate_report_text(report_data)
                    await self.send_long_message(user_id, report_text)
                    await self.save_report_to_db(user_id, report_data)
            logger.info("Ежедневные отчеты успешно отправлены.")
        except Exception as e:
            logger.error(f"Ошибка при отправке ежедневных отчетов: {e}")

    def generate_report_text(self, report_data):
        """Генерация текста отчета по шаблону на основе данных."""
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

    async def send_long_message(self, chat_id, message):
        """Отправка длинного сообщения, разбивая его на части при необходимости."""
        for chunk in split_text_into_chunks(message):
            await self.application.bot.send_message(chat_id=chat_id, text=chunk)

    async def run(self):
        """Запуск бота."""
        await self.setup()
        try:
            await self.application.run_polling()
        finally:
            await self.db_manager.close_connection()
            if self.scheduler.running:
                self.scheduler.shutdown()

# Основная функция для запуска бота
async def main():
    if not config.telegram_token:
        raise ValueError("Telegram token отсутствует в конфигурации")
    bot = TelegramBot(config.telegram_token)
    await bot.run()

if __name__ == '__main__':
    asyncio.run(main())
