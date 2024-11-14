##bot.py
import asyncio
import logging
import os
import traceback
import html
import json
import re
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler

from apscheduler.schedulers.asyncio import AsyncIOScheduler
import httpx
from telegram import Update, BotCommand
from telegram.ext import (
    ApplicationBuilder,
    CallbackContext,
    CommandHandler,
    AIORateLimiter,
)
from telegram.error import TimedOut

from telegram.constants import ParseMode
from telegram.ext import CommandHandler

import config
from logger_utils import setup_logging
from operator_data import OperatorData
from openai_telebot import OpenAIReportGenerator #импорт класса тут из опенаителебота
from permissions_manager import PermissionsManager
from db_module import DatabaseManager
from auth import AuthManager, setup_auth_handlers
import nest_asyncio
from dotenv import load_dotenv
from telegram.error import TelegramError

nest_asyncio.apply()
# Команда помощи
HELP_MESSAGE = """Команды:
        /start – Приветствие и инструкция
        /register – Регистрация нового пользователя
        /generate_report [user_id] [period] – Генерация отчета
        /request_stats – Запрос текущей статистики
        /help – Показать помощь
        /report_summary – Сводка по отчетам
        /settings – Показать настройки
        /cancel – Отменить текущую задачу

        Запрос оператора осуществляется по user_id
        2	 Альбина
        3	 ГВ ст.админ
        5	 Ирина
        6	 Ксения
        7	 ПП Ст.админ
        8	 Ресепшн ГВ
        9	 Ресепшн ПП
        10	 Энже

        Пример: "/generate_report 2 yearly"

        Если вы нажали не ту команду, то выполните команду "/cancel"
        """
# Загрузка переменных из .env файла
load_dotenv()
# Настройка логирования
logger = setup_logging()
log_handler = RotatingFileHandler('logs.log', maxBytes=10**6, backupCount=10)
log_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)
# Чтение конфигурации из .env файла

db_config = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'db': os.getenv('DB_NAME'),
    'autocommit': True
}
# Функция для разделения текста на части
def split_text_into_chunks(text, chunk_size=4096):
    """Разделение текста на части для отправки длинных сообщений."""
    for i in range(0, len(text), chunk_size):
        yield text[i:i + chunk_size]
class TelegramBot:
    def __init__(self, token):
        self.token = token
        # Создаем экземпляр DBManager с конфигурацией
        self.db_manager = DatabaseManager()
        self.auth_manager = AuthManager(self.db_manager)  # Инициализация AuthManager
        self.application = ApplicationBuilder().token(token).rate_limiter(AIORateLimiter()).build()
        self.scheduler = AsyncIOScheduler()
        self.operator_data = OperatorData(self.db_manager)
        self.permissions_manager = PermissionsManager(self.db_manager)  # Инициализация PermissionsManager
        self.report_generator = OpenAIReportGenerator(self.db_manager, model="gpt-4o-mini")     
        # Настройка обработчиков аутентификации
        setup_auth_handlers(self.application, self.db_manager)
    async def setup(self):
        """Инициализация бота и всех компонентов."""
        await self.setup_db_connection()
        self.setup_handlers()
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
                
                
    async def get_help_message(self, user_id):
        """Возвращает текст помощи в зависимости от роли пользователя."""
        current_user_role = await self.permissions_manager.get_user_role(user_id)

        base_help = """Команды:
        /start – Приветствие и инструкция
        /register – Регистрация нового пользователя
        /help – Показать помощь"""

        # Добавляем команды для операторов и администраторов
        if current_user_role in ['Operator', 'Admin']:
            base_help += """
            /generate_report [user_id] [period] – Генерация отчета
            /request_stats – Запрос текущей статистики
            /cancel – Отменить текущую задачу"""

        # Добавляем команды для разработчиков и более высоких ролей
        if current_user_role in ['Developer', 'SuperAdmin', 'Head of Registry', 'Founder', 'Marketing Director']:
            base_help += """
            /report_summary – Сводка по отчетам
            /settings – Показать настройки
            /debug – Отладка"""

        # Информация о запросах по user_id (это может быть полезно всем пользователям)
        base_help += """
        
        Запрос оператора осуществляется по user_id:
        2  Альбина
        3  ГВ ст.админ
        5  Ирина
        6  Ксения
        7  ПП Ст.админ
        8  Ресепшн ГВ
        9  Ресепшн ПП
        10 Энже

        Пример: "/generate_report 2 yearly"
        Если вы нажали не ту команду, выполните команду "/cancel".
        """

        return base_help

    async def login_handle(self, update: Update, context: CallbackContext):
        """Обработчик команды /login для входа с паролем."""
        if len(context.args) < 1:
            await update.message.reply_text("Пожалуйста, введите ваш пароль. Пример: /login ваш_пароль")
            return

        input_password = context.args[0]
        user_id = update.effective_user.id

        # Используем AuthManager для проверки пароля
        verification_result = await self.auth_manager.verify_password(user_id, input_password)
        if verification_result["status"] == "success":
            context.user_data['is_authenticated'] = True
            await self.set_bot_commands(user_id)  # Устанавливаем команды в зависимости от роли
            context.user_data['user_role'] = verification_result["role"]
            await update.message.reply_text(f"Вы успешно вошли в систему как {verification_result['role']}.")
            logger.info(f"Пользователь {user_id} успешно вошел в систему с ролью {verification_result['role']}.")
        else:
            await update.message.reply_text(f"Ошибка авторизации: {verification_result['message']}")
            logger.warning(f"Неуспешная попытка входа пользователя {user_id}: {verification_result['message']}")

    async def set_bot_commands(self, user_id):
        """Установка команд бота в зависимости от роли пользователя."""
        current_user_role = await self.permissions_manager.get_user_role(user_id)

        # Базовые команды, доступные всем
        commands = [BotCommand("/start", "Запуск бота"), BotCommand("/help", "Показать помощь")]

        # Команды для операторов и администраторов
        if current_user_role in ['Operator', 'Admin']:
            commands.append(BotCommand("/generate_report", "Генерация отчета"))
            commands.append(BotCommand("/request_stats", "Запрос текущей статистики"))
            commands.append(BotCommand("/cancel", "Отменить текущую задачу"))

        # Команды для разработчиков и более высоких ролей
        elif current_user_role in ['Developer', 'SuperAdmin', 'Head of Registry', 'Founder', 'Marketing Director']:
            commands.extend([
                BotCommand("/generate_report", "Генерация отчета"),
                BotCommand("/request_stats", "Запрос текущей статистики"),
                BotCommand("/report_summary", "Сводка по отчетам"),
                BotCommand("/settings", "Показать настройки"),
                BotCommand("/debug", "Отладка"),
                BotCommand("/cancel", "Отменить текущую задачу")
            ])

        # Устанавливаем команды в Telegram
        await self.application.bot.set_my_commands(commands)
        logger.info(f"Команды установлены для роли: {current_user_role}")

    def setup_handlers(self):
        """Настройка базовых обработчиков команд. Роль пользователя будет проверяться динамически."""
        # Добавляем базовые команды
        self.application.add_handler(CommandHandler("register", self.register_handle))  # Добавляем обработчик /register
        self.application.add_handler(CommandHandler("start", self.start_handle))
        self.application.add_handler(CommandHandler("help", self.help_handle))
        self.application.add_handler(CommandHandler("cancel", self.cancel_handle))
        self.application.add_handler(CommandHandler("login", self.login_handle))  # Добавляем обработчик /login

        # Команды, доступ к которым зависит от роли, проверяются в самих обработчиках
        self.application.add_handler(CommandHandler("generate_report", self.generate_report_handle))
        self.application.add_handler(CommandHandler("request_stats", self.request_current_stats_handle))
        self.application.add_handler(CommandHandler("report_summary", self.report_summary_handle))
        self.application.add_handler(CommandHandler("settings", self.settings_handle))
        self.application.add_handler(CommandHandler("debug", self.debug_handle))
        
        # Обработчик ошибок
        self.application.add_error_handler(self.error_handle)
        logger.info(f"Обработчики команд настроены.")
        
    async def run(self):
        """Запуск бота."""
        await self.setup()
        try:
            await self.application.run_polling()
        finally:
            await self.db_manager.close_connection()
            if self.scheduler.running:
                self.scheduler.shutdown()
                
    async def register_handle(self, update: Update, context: CallbackContext):
        """Обработчик команды /register для регистрации нового пользователя."""
        user = update.effective_user
        logger.info(f"Регистрация начата для пользователя {user.id} ({user.full_name}).")

        await update.message.reply_text("Пожалуйста, введите вашу роль (например, Operator, Developer, Admin):")
        role_name = await self.get_user_input(update, context)

        # Ожидаем ввода роли и пароля
        if len(context.args) < 2:
            await update.message.reply_text("Пожалуйста, укажите роль и пароль через пробел. Пример: /register Operator ваш_пароль")
            return
        role_name = context.args[0]
        input_password = context.args[1]
        logger.info(f"Получена роль: {role_name} для пользователя {user.id}.")
        if not role_name:
            await update.message.reply_text("Не удалось получить роль. Пожалуйста, попробуйте снова.")
            return

        registration_result = await self.auth_manager.register_user(
            user_id=user.id,
            full_name=user.full_name,
            role=role_name,
            input_password=input_password
        )

        if registration_result["status"] == "success":
            password = registration_result["password"]
            await update.message.reply_text(f"Регистрация прошла успешно! Ваш пароль: {password}. Пожалуйста, сохраните его в безопасном месте.")
        else:
            await update.message.reply_text(f"Ошибка при регистрации: {registration_result['message']}")

        
    async def get_command_stats(self):
        """Получает статистику по использованию команд."""
        # Здесь вы можете обратиться к базе данных или к метрикам бота
        # Например, считывание данных из таблицы `command_usage` или другой метрики
        try:
            async with self.db_manager.acquire() as connection:
                query = "SELECT command, COUNT(*) as usage_count FROM CommandUsage GROUP BY command"
                async with connection.cursor() as cursor:
                    await cursor.execute(query)
                    result = await cursor.fetchall()
                    command_stats = "\n".join([f"{row['command']}: {row['usage_count']} раз" for row in result])
                    return command_stats
        except Exception as e:
                    logger.error(f"Ошибка при получении статистики команд: {e}")
                    return "Не удалось получить статистику по командам"
    def get_last_log_entries(self, log_file='logs.log', num_lines=10):
        """Получает последние записи из файла лога."""
        try:
            with open(log_file, 'r') as f:
                lines = f.readlines()
            # Возвращаем последние `num_lines` строк
            return "".join(lines[-num_lines:])
        except Exception as e:
            logger.error(f"Ошибка при чтении файла лога: {e}")
            return "Не удалось загрузить последние записи лога."

        
    async def debug_handle(self, update: Update, context: CallbackContext):
        """Обработчик команды /debug для диагностики и отладки (доступно только разработчику)."""
        user_id = update.effective_user.id
        logger.info(f"Команда /debug получена от пользователя {user_id}")

        try:
            # Получаем роль текущего пользователя
            current_user_role = await self.permissions_manager.get_user_role(user_id)
            if current_user_role != 'developer':
                await update.message.reply_text("У вас нет прав для выполнения этой команды.")
                return

            # Собираем информацию для отладки
            debug_info = "🛠️ Debug Information:\n"
                
            # Проверка состояния соединения с базой данных
            async with self.db_manager.acquire() as connection:
                db_status = "База данных подключена"
                await connection.ping()
                
            debug_info += f"- DB Status: {db_status}\n"

            # Статистика по использованию команд
            command_stats = await self.get_command_stats()
            debug_info += f"- Использование команд: {command_stats}\n"

            # Логи последних ошибок (условно, можно загружать логи из файла)
            last_log_lines = self.get_last_log_entries()
            debug_info += f"- Последние записи лога:\n{last_log_lines}"

            # Отправляем сообщение с собранной информацией
            await update.message.reply_text(debug_info, parse_mode=ParseMode.HTML)
            logger.info("Информация по отладке успешно отправлена разработчику.")
        except Exception as e:
            logger.error(f"Ошибка при выполнении команды /debug: {e}")
            await update.message.reply_text("Произошла ошибка при выполнении команды /debug. Попробуйте позже.")


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
        help_message = await self.get_help_message(user_id)
        await update.message.reply_text(help_message, parse_mode=ParseMode.HTML)

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
            
    async def verify_role_password(self, user_id, input_password, role_password):
        """Проверка пароля роли для пользователя."""
        try:
            async with self.db_manager.acquire() as connection:
                query = """
                SELECT r.role_name, r.role_password 
                FROM UsersTelegaBot u
                JOIN RolesTelegaBot r ON u.role_id = r.id
                WHERE u.user_id = %s
                """
                async with connection.cursor() as cursor:
                    await cursor.execute(query, (user_id,))
                    result = await cursor.fetchone()
                    
                    if not result:
                        return False, "Роль пользователя не найдена."

                    role_name = result["role_name"]
                     # Сравниваем введенный пароль с паролем роли
                    if input_password == role_password:
                        return True, role_name
                    else:
                        return False, "Неверный пароль для роли."
        except Exception as e:
            logger.error(f"Ошибка при проверке пароля роли для пользователя с ID {user_id}: {e}")
            return False, "Ошибка при проверке пароля."
            

    def parse_period(self, period_str):
        """Парсинг периода из строки. Возвращает дату или диапазон."""
        today = datetime.today().date()

        if period_str == 'daily':
            return today, today
        elif period_str == 'weekly':
            start_week = today - timedelta(days=today.weekday())
            return start_week, today
        elif period_str == 'biweekly':
            start_biweek = today - timedelta(days=14)
            return start_biweek, today
        elif period_str == 'monthly':
            start_month = today.replace(day=1)
            return start_month, today
        elif period_str == 'half_year':
            start_half_year = today - timedelta(days=183)
            return start_half_year, today
        elif period_str == 'yearly':
            start_year = today - timedelta(days=365)
            return start_year, today
        else:
            raise ValueError(f"Неизвестный период: {period_str}")

    async def generate_report_handle(self, update: Update, context: CallbackContext):
        """Обработчик команды /generate_report для генерации отчета."""
        user_id = update.effective_user.id
        logger.info(f"Команда /generate_report получена от пользователя {user_id}")

        # Проверка авторизации пользователя
        if not context.user_data.get('is_authenticated'):
            await update.message.reply_text(
                "Пожалуйста, сначала войдите в систему с помощью команды /login ваш_пароль."
            )
            return

        # Проверка наличия аргументов команды
        if len(context.args) < 2:
            await update.message.reply_text(
                "Пожалуйста, укажите ID пользователя и период (daily, weekly, biweekly, monthly, half_year, yearly).\n"
                "Пример: /generate_report 2 daily"
            )
            return

        # Извлекаем ID пользователя и период
        target_user_id_str = context.args[0]
        period_str = context.args[1].lower()

        # Проверяем корректность target_user_id
        if not target_user_id_str.isdigit() or int(target_user_id_str) <= 0:
            logger.error(f"Некорректный ID пользователя: {target_user_id_str}")
            await update.message.reply_text(
                "Некорректный ID пользователя. Убедитесь, что это положительное целое число."
            )
            return

        target_user_id = int(target_user_id_str)

        # Проверка корректности периода
        valid_periods = ['daily', 'weekly', 'biweekly', 'monthly', 'half_year', 'yearly']
        if period_str not in valid_periods:
            await update.message.reply_text(
                f"Некорректный период. Допустимые значения: {', '.join(valid_periods)}."
            )
            return

        # Проверка прав доступа на основе роли пользователя
        current_user_role = context.user_data.get('user_role')
        logger.info(f"Пользователь {user_id} авторизован с ролью {current_user_role} для генерации отчета.")
        restricted_roles = ['Operator', 'Admin']
        if current_user_role in restricted_roles and user_id != target_user_id:
            logger.warning(
                f"Пользователь {user_id} с ролью {current_user_role} не имеет прав для просмотра отчетов других пользователей."
            )
            await update.message.reply_text(
                "У вас нет прав для просмотра отчетов других пользователей."
            )
            return

        # Генерация отчета с учетом подключения к базе данных
        try:
            logger.info(f"Начата генерация отчета для пользователя {target_user_id} за период '{period_str}'.")
            async with self.db_manager.acquire() as connection:
                logger.info(f"Запрос на генерацию отчета для user_id {target_user_id} с периодом {period_str}")
                # Вызов метода generate_report напрямую, user_id передается как есть.
                report = await self.report_generator.generate_report(connection, target_user_id, period=period_str)
            
            # Проверка, если отчет пустой или не был сгенерирован
            if not report:
                logger.warning(
                    f"Отчет для пользователя {target_user_id} за период '{period_str}' не был сгенерирован."
                )
                await update.message.reply_text(
                    f"Данные для пользователя с ID {target_user_id} за указанный период не найдены."
                )
                return

            # Отправка отчета пользователю
            await self.send_long_message(update.effective_chat.id, report)
            logger.info(f"Отчет для пользователя {target_user_id} успешно сгенерирован и отправлен.")
            await update.message.reply_text("Отчет успешно сгенерирован и отправлен.")
        except Exception as e:
            logger.error(f"Ошибка при генерации отчета для пользователя {target_user_id}: {e}")
            await update.message.reply_text(
                "Произошла ошибка при генерации отчета. Пожалуйста, попробуйте позже."
            )


    async def request_current_stats_handle(self, update: Update, context: CallbackContext):
        """Обработчик команды /request_stats для получения текущей статистики."""
        user_id = update.effective_user.id
        logger.info(f"Команда /request_stats получена от пользователя {user_id}")
        operator_data = await self.db_manager.get_user_by_id(user_id)
        if not operator_data:
            await update.message.reply_text("Ваш ID пользователя не найден. Пожалуйста, зарегистрируйтесь с помощью команды /register.")
            return
        try:
            async with self.db_manager.acquire() as connection:
                report_data = await self.report_generator.generate_report(connection, user_id, period="daily")
            if report_data is None:
                await update.message.reply_text(f"Данные по пользователю с ID {user_id} не найдены.")
                logger.error(f"Данные по пользователю с ID {user_id} не найдены.")
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
            # Получаем данные по всем операторам
            operators = await self.operator_data.get_all_operators_metrics()
            if not operators:
                await update.message.reply_text("Нет данных для отчета.")
                return

            # Создаем задачи для генерации отчетов
            tasks = [
                self.generate_and_send_report(op['user_id'], "daily") for op in operators
            ]
            reports_data = await asyncio.gather(*tasks, return_exceptions=True)

            # Фильтруем ошибки и форматируем успешные отчеты
            report_texts = []
            for report_data in reports_data:
                if isinstance(report_data, Exception):
                    logger.error(f"Ошибка при генерации отчета: {report_data}")
                    continue
                if isinstance(report_data, str):
                    report_texts.append(report_data)
                else:
                    logger.warning("Получен некорректный формат данных отчета.")

            # Формируем полный текст отчета
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

    async def send_daily_reports(self):
        """Отправка ежедневных отчетов в конце рабочего дня."""
        logger.info("Начата отправка ежедневных отчетов.")
        try:
            operators = await self.operator_data.get_all_operators_metrics()
            tasks = [
                self.generate_and_send_report(operator['user_id'], "daily")
                for operator in operators
            ]
            await asyncio.gather(*tasks, return_exceptions=True)
            logger.info("Ежедневные отчеты успешно отправлены.")
        except Exception as e:
            logger.error(f"Ошибка при отправке ежедневных отчетов: {e}")

    async def generate_and_send_report(self, user_id, period):
        """Генерация и отправка отчета для конкретного пользователя."""
        try:
            async with self.db_manager.acquire() as connection:
                report = await self.report_generator.generate_report(connection, user_id, period=period)
            
            if not report:
                logger.error(f"Данные по пользователю с ID {user_id} не найдены.")
                return

            # Отправка отчета пользователю
            await self.send_report_to_user(user_id, report)
            logger.info(f"Отчет успешно отправлен пользователю {user_id}")
        except Exception as e:
            logger.error(f"Ошибка при генерации отчета для пользователя {user_id}: {e}")

    def generate_report_text(self, report_data):
        """Генерация текста отчета по шаблону на основе данных."""
        report_text = f"""
        📊 Ежедневный отчет за {report_data['report_date']} для оператора {report_data['name']}

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
        logger.info(f"[КРОТ]: МЕТОД ГЕНЕРАЦИИ ИЗ МЭЙНФАЙЛА, ТРЕТЬЯ ЛОВУШКА СРАБОТАЛА. Отчет успешно отформатирован")
        return report_text
    

    async def send_message_with_retry(self, bot, chat_id, text, retry_attempts=3):
        """
        Отправка сообщения с повторной попыткой в случае ошибки TimedOut.
        :param bot: экземпляр бота.
        :param chat_id: ID чата для отправки сообщения.
        :param text: Текст сообщения.
        :param retry_attempts: Количество попыток отправки сообщения.
        """
        for attempt in range(retry_attempts):
            try:
                await bot.send_message(chat_id=chat_id, text=text)
                return
            except TimedOut:
                if attempt < retry_attempts - 1:
                    logger.warning(f"Попытка {attempt + 1} из {retry_attempts} для отправки сообщения.")
                    # Экспоненциальная задержка перед повтором
                    await asyncio.sleep(2 ** attempt)
                else:
                    logger.error("Не удалось отправить сообщение после нескольких попыток.")

    async def send_long_message(self, chat_id, message: str):
        """Отправка длинного сообщения, разбивая его на части при необходимости."""
        # Разбиваем сообщение на части по 4096 символов, так как это ограничение Telegram.
        for chunk in split_text_into_chunks(message, chunk_size=4096):
            try:
                # Отправляем каждую часть сообщения
                await self.application.bot.send_message(chat_id=chat_id, text=chunk)
                await asyncio.sleep(0.1)  # Небольшая задержка между отправками
            except Exception as e:
                logger.error(f"Ошибка при отправке сообщения: {e}")
                await self.application.bot.send_message(
                    chat_id=chat_id,
                    text="Произошла ошибка при отправке длинного сообщения. Пожалуйста, попробуйте позже."
                )
                break
    
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
                    await self.send_message_with_retry(self.application.bot, update.effective_chat.id, message_chunk, parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.error(f"Ошибка в обработчике ошибок: {e}")
    

    async def get_user_chat_id(self, connection, user_id):
        """
        Получает chat_id пользователя в Telegram по его user_id.
        """
        query = "SELECT chat_id FROM UsersTelegaBot WHERE user_id = %s LIMIT 1"
        try:
            async with connection.cursor() as cursor:
                await cursor.execute(query, (user_id,))
                result = await cursor.fetchone()
                if result and result.get('chat_id'):
                    return result['chat_id']
                else:
                    logger.error(f"[КРОТ]: Не найден chat_id для пользователя с user_id {user_id}.")
                    return None
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при получении chat_id пользователя: {e}")
            return None
        
    async def send_report_to_user(self, user_id, report_text):
        """Отправляет сформированный отчет пользователю через Telegram-бот."""
        async with self.db_manager.acquire() as connection:
            chat_id = await self.get_user_chat_id(connection, user_id)
        if not chat_id:
            logger.error(f"[КРОТ]: Не удалось получить chat_id для пользователя {user_id}.")
            return
        try:
            messages = [report_text[i:i+4000] for i in range(0, len(report_text), 4000)]
            for msg in messages:
                await self.send_message_with_retry(chat_id=chat_id, text=msg)
            logger.info(f"[КРОТ]: Отчет успешно отправлен пользователю с chat_id {chat_id}.")
        except TelegramError as e:
            logger.error(f"[КРОТ]: Бот заблокирован пользователем с chat_id {chat_id}.")
        else:
            logger.error(f"[КРОТ]: Ошибка при отправке отчета пользователю с chat_id {chat_id}: {e}")
            
    async def send_password_to_chief(self, password):
        """
        Отправляет сгенерированный пароль заведующей регистратуры через Telegram.
        Получает юзернейм заведующей регистратуры с role_id 5 из базы данных.
         """
        # Извлекаем юзернейм заведующей с role_id = 5
        query = "SELECT username FROM UsersTelegaBot WHERE role_id = 5 LIMIT 1"
        async with self.db_manager.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query)
                result = await cur.fetchone()

            if not result or not result.get("username"):
                logger.error("[КРОТ]: Не удалось найти заведующую регистратуры (role_id = 5) в базе данных.")
                return

            chief_telegram_username = result["username"]
            logger.info(f"[КРОТ]: Отправляем пароль заведующей регистратуры @{chief_telegram_username}")
            message = f"Сгенерированный пароль для нового пользователя: {password}"
            url = f"https://api.telegram.org/bot{os.getenv('TELEGRAM_BOT_TOKEN')}/sendMessage"
            async with httpx.AsyncClient() as client:
                response = await client.post(url, json={"chat_id": f"@{chief_telegram_username}", "text": message})
            if response.status_code == 200:
                logger.info(f"[КРОТ]: Пароль успешно отправлен заведующей @{chief_telegram_username}.")
            else:
                logger.error(f"[КРОТ]: Не удалось отправить сообщение в Telegram. Код ошибки: {response.status_code}")

# Основная функция для запуска бота
async def main():
    if not config.telegram_token:
        raise ValueError("Telegram token отсутствует в конфигурации")
    bot = TelegramBot(config.telegram_token)
    await bot.run()
if __name__ == '__main__':
    asyncio.run(main())