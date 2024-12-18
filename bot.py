##bot.py
import asyncio
import logging
import os
import sys
import traceback
import html
import json
import re
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler, QueueHandler, QueueListener
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import httpx
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, BotCommand
from telegram.ext import (
    ApplicationBuilder,
    CallbackContext,
    CommandHandler,
    AIORateLimiter,
    filters,
    CallbackQueryHandler,
)
from telegram.error import TimedOut

from telegram.constants import ParseMode

import config
from logger_utils import setup_logging
from operator_data import OperatorData
from openai_telebot import (
    OpenAIReportGenerator,
    create_async_connection,
)  # импорт класса тут из опенаителебота
from permissions_manager import PermissionsManager
from db_module import DatabaseManager
from auth import AuthManager, setup_auth_handlers
import nest_asyncio
from dotenv import load_dotenv
from telegram.error import TelegramError
from telegram.request import HTTPXRequest
import html
from telegram import Bot
from telegram.constants import ParseMode
import queue  # Добавлено для Queue
from logging.handlers import RotatingFileHandler, QueueHandler, QueueListener
import fcntl
from telegram.ext import Application

# Импортируем ProgressData и visualization
from progress_data import ProgressData
from visualization import (
    create_multi_metric_graph,
    calculate_trends,
    create_all_operators_progress_graph,
)
from openai import AsyncOpenAI, OpenAIError  # импорт класса

lock_file = "/tmp/bot.lock"
fp = open(lock_file, "w")
try:
    fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
except IOError:
    print("Бот уже запущен!")
    exit(1)

nest_asyncio.apply()
# Загрузка переменных из .env файла
load_dotenv()
telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
if not telegram_token:
    raise ValueError("Токен не найден. Проверьте файл .env и переменную TELEGRAM_TOKEN")
print(f"Загруженный токен: {telegram_token}")  # Отладочная печать
# Убедитесь, что `token` является строкой
if not isinstance(telegram_token, str):
    raise TypeError("Значение токена должно быть строкой")
# Создаем очередь для логов
log_queue = queue.Queue(-1)

# Настраиваем корневой логгер
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Настраиваем обработчик для записи логов в файл с ротацией
log_file = "logs.log"
max_log_lines = 150000
average_line_length = 100
max_bytes = max_log_lines * average_line_length
backup_count = 0

file_handler = RotatingFileHandler(
    log_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
)
file_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
file_handler.setLevel(logging.INFO)

# Настраиваем QueueHandler и QueueListener
queue_handler = QueueHandler(log_queue)
listener = QueueListener(log_queue, file_handler)
listener.start()

logger.addHandler(queue_handler)

# Настройка обработчика для консоли
console_handler = logging.StreamHandler()
console_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
console_handler.setLevel(logging.INFO)
logger.addHandler(console_handler)


# Обработчик необработанных исключений
def log_uncaught_exceptions(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.error(
        "Необработанное исключение", exc_info=(exc_type, exc_value, exc_traceback)
    )


sys.excepthook = log_uncaught_exceptions

# Настройка минимальных параметров для HTTPXRequest
logger.info("Настройка HTTPXRequest...")
httpx_request = HTTPXRequest(
    connection_pool_size=100,  # Размер пула соединений
    read_timeout=10.0,  # Таймаут на чтение
    write_timeout=10.0,  # Таймаут на запись
    connect_timeout=5.0,  # Таймаут на подключение
)

# Инициализация приложения Telegram
telegram_token = "YOUR_BOT_TOKEN"
logger.info("Настройка приложения Telegram...")
app = (
    ApplicationBuilder()
    .token(telegram_token)
    .request(httpx_request)
    .rate_limiter(AIORateLimiter())
    .build()
)

# Задачи
MAX_CONCURRENT_TASKS = 3
task_queue = asyncio.Queue()


async def worker(queue: asyncio.Queue, bot_instance):
    while True:
        task = await queue.get()
        user_id = task["user_id"]
        report_type = task["report_type"]
        period = task["period"]
        chat_id = task["chat_id"]
        date_range = task["date_range"]

        try:
            # Открываем соединение с БД через async with
            async with bot_instance.db_manager.acquire() as connection:
                report = await bot_instance.report_generator.generate_report(
                    connection, user_id, period=period, date_range=date_range
                )

            # Теперь report либо содержит текст отчета, либо сообщение об ошибке, если данных нет
            if report and not report.startswith("Ошибка:"):
                # Отчет успешно сгенерирован
                await bot_instance.send_long_message(chat_id, report)
                logger.info(f"Отчет для user_id={user_id} отправлен.")
            else:
                # Если вернулось None или строка с "Ошибка", информируем пользователя
                if not report:
                    # В случае если вообще ничего не вернулось
                    message = "Ошибка при извлечении данных оператора или данных нет."
                else:
                    # report уже содержит текст ошибки, например "Ошибка..."
                    message = report
                await bot_instance.application.bot.send_message(
                    chat_id=chat_id, text=message
                )
                logger.info(
                    f"Нет данных или ошибка для user_id={user_id}. Сообщение пользователю: {message}"
                )

        except Exception as e:
            logger.error(f"Ошибка при обработке задачи для user_id={user_id}: {e}")
            await bot_instance.application.bot.send_message(
                chat_id=chat_id,
                text="Произошла ошибка при генерации отчета. Попробуйте позже.",
            )
        finally:
            queue.task_done()
            logger.info(f"Воркеры завершили обработку задачи: {task}")


async def add_task(
    bot_instance, user_id, report_type, period, chat_id, date_range=None
):
    task = {
        "user_id": user_id,
        "report_type": report_type,
        "period": period,
        "chat_id": chat_id,
        "date_range": date_range,
    }
    await task_queue.put(task)
    logger.info(
        f"Задача добавлена в очередь для user_id={user_id}, {report_type}, {period}."
    )
    await bot_instance.application.bot.send_message(
        chat_id=chat_id, text="Ваш запрос поставлен в очередь на обработку."
    )


# Чтение конфигурации из .env файла
db_config = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "db": os.getenv("DB_NAME"),
    "autocommit": True,
}

# Команда помощи
HELP_MESSAGE = """Команды:
        /start – Приветствие и инструкция
        /register – Регистрация нового пользователя
        /generate_report [user_id] [period] – Генерация отчета
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

        Для генерации отчета по операторам с рекомендациями используйте команду: "/generate_report 5 custom 01/10/2024-25/11/2024", где custom является важной переменной после главной команды, также дата должна строго быть в таком формате
        Для генерации отчета по всем операторам без упоминании позывного без рекомендацией используйте команду: "/report_summary custom 01/10/2024-25/11/2024"
        Если вы нажали не ту команду, то выполните команду "/cancel"
        
        Сначала необходимо зайти в бота через команду /login введя пароль 
            
        По вопросам работы бота обращаться в отдел маркетинга Гирудомед.
    
    """


# Функция для разделения текста на части
def split_text_into_chunks(text, chunk_size=4096):
    """Разделение текста на части для отправки длинных сообщений."""
    for i in range(0, len(text), chunk_size):
        yield text[i : i + chunk_size]


class TelegramBot:
    def __init__(self, token, model="gpt-4o-mini"):
        # Настройка OpenAI API ключа из переменных окружения
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            logger.error(
                "OpenAI API ключ не найден. Пожалуйста, установите переменную окружения OPENAI_API_KEY."
            )
            raise EnvironmentError("OpenAI API ключ не найден.")
        self.token = token
        # Создаем экземпляр DBManager с конфигурацией
        self.db_manager = DatabaseManager()
        self.auth_manager = AuthManager(self.db_manager)  # Инициализация AuthManager
        self.application = (
            ApplicationBuilder().token(token).rate_limiter(AIORateLimiter()).build()
        )
        self.scheduler = AsyncIOScheduler()
        self.operator_data = OperatorData(self.db_manager)
        self.permissions_manager = PermissionsManager(
            self.db_manager
        )  # Инициализация PermissionsManager
        self.report_generator = OpenAIReportGenerator(
            self.db_manager, model="gpt-4o-mini"
        )
        self.application.add_handler(
            CommandHandler("operator_progress", self.operator_progress_handle)
        )
        self.application.add_handler(
            CommandHandler("all_operators_progress", self.all_operators_progress_handle)
        )
        self.progress_data = ProgressData(
            self.db_manager
        )  # Подключаем ваш модуль для прогресса
        self.client = AsyncOpenAI(api_key=api_key)
        self.model = model  # Устанавливаем модель gpt-4o-mini

        # Настройка обработчиков аутентификации
        setup_auth_handlers(self.application, self.db_manager)

    async def setup(self):
        """Инициализация бота и всех компонентов."""
        await self.setup_db_connection()
        self.setup_handlers()
        if not self.scheduler.running:
            self.scheduler.start()
        self.scheduler.add_job(
            self.send_daily_reports, "cron", hour=10, minute=6
        )  # поставить 6 утра, на проде будет не локальное мое время
        logger.info("Ежедневная задача для отправки отчетов добавлена в планировщик.")
        # Запуск воркеров
        for _ in range(MAX_CONCURRENT_TASKS):
            asyncio.create_task(worker(task_queue, self))
        logger.info(
            f"Запущено {MAX_CONCURRENT_TASKS} воркеров для обработки очереди задач."
        )

    async def setup_db_connection(self, retries=3, delay=2):
        """Настройка подключения к базе данных с повторными попытками."""
        for attempt in range(retries):
            try:
                await self.db_manager.create_pool()
                logger.info("Успешное подключение к базе данных.")
                return
            except Exception as e:
                logger.error(
                    f"Ошибка подключения к БД: {e}, попытка {attempt + 1} из {retries}"
                )
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
        if current_user_role in ["Operator", "Admin"]:
            base_help += """
            /generate_report [user_id] [period] – Генерация отчета
            /request_stats – Запрос текущей статистики
            /cancel – Отменить текущую задачу"""

        # Добавляем команды для разработчиков и более высоких ролей
        if current_user_role in [
            "Developer",
            "SuperAdmin",
            "Head of Registry",
            "Founder",
            "Marketing Director",
        ]:
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

    async def get_user_input(
        self,
        update: Update,
        context: CallbackContext,
        prompt: str = "Введите значение:",
    ):
        """
        Запрашивает ввод у пользователя и возвращает ответ.
        :param update: Объект Update, представляющий текущий апдейт от Telegram.
        :param context: Объект CallbackContext, предоставляющий доступ к данным и инструментам бота.
        :param prompt: Сообщение для запроса ввода у пользователя.
        :return: Строка с ответом пользователя или None, если ответа не было.
        """
        # Отправляем пользователю приглашение к вводу
        await update.message.reply_text(prompt)

        def check_reply(new_update):
            """Проверяет, является ли сообщение ответом от нужного пользователя."""
            return (
                new_update.message
                and new_update.effective_chat.id == update.effective_chat.id
                and new_update.effective_user.id == update.effective_user.id
            )

        try:
            # Ждем ответа от пользователя в течение 60 секунд
            new_update = await context.application.bot.get_updates(timeout=10)
            user_input = None

            for msg_update in new_update:
                if check_reply(msg_update):
                    user_input = (
                        msg_update.message.text.strip()
                        if msg_update.message.text
                        else None
                    )
                    break

            if not user_input:
                await update.message.reply_text("Ответ не распознан. Попробуйте снова.")
                return None

            return user_input

        except asyncio.TimeoutError:
            await update.message.reply_text("Время ожидания истекло. Попробуйте снова.")
            return None

    async def login_handle(self, update: Update, context: CallbackContext):
        """Обработчик команды /login для входа с паролем."""
        if len(context.args) < 1:
            await update.message.reply_text(
                "Пожалуйста, введите ваш пароль. Пример: /login ваш_пароль"
            )
            return

        input_password = context.args[0]
        user_id = update.effective_user.id

        # Используем AuthManager для проверки пароля
        verification_result = await self.auth_manager.verify_password(
            user_id, input_password
        )
        if verification_result["status"] == "success":
            context.user_data["is_authenticated"] = True
            await self.set_bot_commands(
                user_id
            )  # Устанавливаем команды в зависимости от роли
            context.user_data["user_role"] = verification_result["role"]
            await update.message.reply_text(
                f"Вы успешно вошли в систему как {verification_result['role']}."
            )
            logger.info(
                f"Пользователь {user_id} успешно вошел в систему с ролью {verification_result['role']}."
            )
        else:
            await update.message.reply_text(
                f"Ошибка авторизации: {verification_result['message']}"
            )
            logger.warning(
                f"Неуспешная попытка входа пользователя {user_id}: {verification_result['message']}"
            )

    async def set_bot_commands(self, user_id):
        """Установка команд бота в зависимости от роли пользователя."""
        current_user_role = await self.permissions_manager.get_user_role(user_id)

        # Базовые команды, доступные всем
        commands = [
            BotCommand("/start", "Запуск бота"),
            BotCommand("/help", "Показать помощь"),
        ]

        # Команды для операторов и администраторов
        if current_user_role in ["Operator", "Admin"]:
            commands.append(BotCommand("/generate_report", "Генерация отчета"))
            commands.append(BotCommand("/request_stats", "Запрос текущей статистики"))
            commands.append(BotCommand("/cancel", "Отменить текущую задачу"))

        # Команды для разработчиков и более высоких ролей
        elif current_user_role in [
            "Developer",
            "SuperAdmin",
            "Head of Registry",
            "Founder",
            "Marketing Director",
        ]:
            commands.extend(
                [
                    BotCommand("/generate_report", "Генерация отчета"),
                    BotCommand("/request_stats", "Запрос текущей статистики"),
                    BotCommand("/report_summary", "Сводка по отчетам"),
                    BotCommand("/settings", "Показать настройки"),
                    BotCommand("/debug", "Отладка"),
                    BotCommand("/cancel", "Отменить текущую задачу"),
                    BotCommand(
                        "/operator_progress_menu", "Выбрать оператора и период"
                    ),  # Добавляем сюда
                ]
            )

        # Устанавливаем команды в Telegram
        await self.application.bot.set_my_commands(commands)
        logger.info(f"Команды установлены для роли: {current_user_role}")

    def setup_handlers(self):
        """Настройка базовых обработчиков команд. Роль пользователя будет проверяться динамически."""
        # Добавляем базовые команды
        self.application.add_handler(
            CommandHandler("register", self.register_handle)
        )  # Добавляем обработчик /register
        self.application.add_handler(CommandHandler("start", self.start_handle))
        self.application.add_handler(CommandHandler("help", self.help_handle))
        self.application.add_handler(CommandHandler("cancel", self.cancel_handle))
        self.application.add_handler(
            CommandHandler("login", self.login_handle)
        )  # Добавляем обработчик /login

        # Команды, доступ к которым зависит от роли, проверяются в самих обработчиках
        self.application.add_handler(
            CommandHandler("generate_report", self.generate_report_handle)
        )
        self.application.add_handler(
            CommandHandler("request_stats", self.request_current_stats_handle)
        )
        self.application.add_handler(
            CommandHandler("report_summary", self.report_summary_handle)
        )
        self.application.add_handler(CommandHandler("settings", self.settings_handle))
        self.application.add_handler(CommandHandler("debug", self.debug_handle))
        self.application.add_handler(
            CommandHandler("report_summary", self.report_summary_handle)
        )
        self.application.add_handler(
            CommandHandler("operator_progress", self.operator_progress_handle)
        )
        self.application.add_handler(
            CommandHandler("all_operators_progress", self.all_operators_progress_handle)
        )  # Регистрация новой команды
        self.application.add_handler(
            CommandHandler("operator_progress_menu", self.operator_progress_menu_handle)
        )

        # Callback для кнопок
        self.application.add_handler(
            CallbackQueryHandler(self.operator_callback_handle, pattern="^operator_")
        )
        self.application.add_handler(
            CallbackQueryHandler(self.operator_callback_handle, pattern="^period_")
        )
        self.application.add_handler(
            CallbackQueryHandler(self.callback_query_handler, pattern="^op_prog:")
        )
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
        logger.info(
            f"Регистрация начата для пользователя {user.id} ({user.full_name})."
        )

        await update.message.reply_text(
            "Пожалуйста, введите вашу роль (например, Operator, Developer, Admin):"
        )
        role_name = await self.get_user_input(
            update, context, prompt="Введите вашу роль:"
        )
        if not role_name:
            await update.message.reply_text(
                "Не удалось получить роль. Пожалуйста, попробуйте снова."
            )
            return
        # Ожидаем ввода роли и пароля
        if len(context.args) < 2:
            await update.message.reply_text(
                "Пожалуйста, укажите роль и пароль через пробел. Пример: /register Operator ваш_пароль"
            )
            return
        role_name = context.args[0]
        input_password = context.args[1]
        logger.info(f"Получена роль: {role_name} для пользователя {user.id}.")
        if not role_name:
            await update.message.reply_text(
                "Не удалось получить роль. Пожалуйста, попробуйте снова."
            )
            return
        logger.info(f"Получена роль: {role_name} для пользователя {user.id}.")
        registration_result = await self.auth_manager.register_user(
            user_id=user.id,
            full_name=user.full_name,
            role=role_name,
            input_password=input_password,
        )

        if registration_result["status"] == "success":
            password = registration_result["password"]
            await update.message.reply_text(
                f"Регистрация прошла успешно! Ваш пароль: {password}. Пожалуйста, сохраните его в безопасном месте."
            )
        else:
            await update.message.reply_text(
                f"Ошибка при регистрации: {registration_result['message']}"
            )

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
                    command_stats = "\n".join(
                        [
                            f"{row['command']}: {row['usage_count']} раз"
                            for row in result
                        ]
                    )
                    return command_stats
        except Exception as e:
            logger.error(f"Ошибка при получении статистики команд: {e}")
            return "Не удалось получить статистику по командам"

    def get_last_log_entries(self, log_file="logs.log", num_lines=10):
        """Получает последние записи из файла лога."""
        try:
            with open(log_file, "r") as f:
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
            if current_user_role != "developer":
                await update.message.reply_text(
                    "У вас нет прав для выполнения этой команды."
                )
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
            await update.message.reply_text(
                "Произошла ошибка при выполнении команды /debug. Попробуйте позже."
            )

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
            logger.error(
                f"Ошибка при проверке пароля роли для пользователя с ID {user_id}: {e}"
            )
            return False, "Ошибка при проверке пароля."

    def parse_period(self, period_str):
        """Парсинг периода из строки. Возвращает дату или диапазон."""
        today = datetime.today().date()

        if period_str == "daily":
            return today, today
        elif period_str == "weekly":
            start_week = today - timedelta(days=today.weekday())
            return start_week, today
        elif period_str == "biweekly":
            start_biweek = today - timedelta(days=14)
            return start_biweek, today
        elif period_str == "monthly":
            start_month = today.replace(day=1)
            return start_month, today
        elif period_str == "half_year":
            start_half_year = today - timedelta(days=183)
            return start_half_year, today
        elif period_str == "yearly":
            start_year = today - timedelta(days=365)
            return start_year, today
        elif period_str.startswith("custom"):
            try:
                # Ожидаемый формат: custom dd/mm/yyyy-dd/mm/yyyy
                _, date_range = period_str.split(" ", 1)
                start_date_str, end_date_str = date_range.split("-")
                start_date = datetime.strptime(
                    start_date_str.strip(), "%d/%m/%Y"
                ).date()
                end_date = datetime.strptime(end_date_str.strip(), "%d/%m/%Y").date()
                return start_date, end_date
            except Exception as e:
                raise ValueError(
                    f"Некорректный формат для custom периода: {period_str}. Ожидается формат: 'custom dd/mm/yyyy-dd/mm/yyyy'"
                ) from e
        else:
            raise ValueError(f"Неизвестный период: {period_str}")

    async def generate_report_handle(self, update: Update, context: CallbackContext):
        """Обработчик команды /generate_report для генерации отчета."""
        user_id = update.effective_user.id
        logger.info(f"Команда /generate_report получена от пользователя {user_id}")

        # Проверка авторизации пользователя
        if not context.user_data.get("is_authenticated"):
            await update.message.reply_text(
                "Пожалуйста, сначала войдите в систему с помощью команды /login ваш_пароль."
            )
            return

        # Проверка наличия аргументов команды
        if len(context.args) < 2:
            await update.message.reply_text(
                "Пожалуйста, укажите ID пользователя и период (daily, weekly, biweekly, monthly, half_year, yearly, или custom). "
                "Для кастомного периода укажите диапазон дат в формате DD/MM/YYYY-DD/MM/YYYY. "
                "Пример: /generate_report 2 custom 20/11/2024-25/11/2024"
            )
            return

        # Извлекаем ID пользователя и период
        target_user_id_str = context.args[0]
        period_str = context.args[1].lower()

        # Проверяем корректность ID пользователя
        if not target_user_id_str.isdigit() or int(target_user_id_str) <= 0:
            logger.error(f"Некорректный ID пользователя: {target_user_id_str}")
            await update.message.reply_text(
                "Некорректный ID пользователя. Убедитесь, что это положительное целое число."
            )
            return

        target_user_id = int(target_user_id_str)

        # Проверка корректности периода
        valid_periods = [
            "daily",
            "weekly",
            "biweekly",
            "monthly",
            "half_year",
            "yearly",
            "custom",
        ]
        if period_str not in valid_periods:
            await update.message.reply_text(
                f"Некорректный период. Допустимые значения: {', '.join(valid_periods)}."
            )
            return

        date_range = None
        # Обработка кастомного периода
        if period_str == "custom":
            if len(context.args) < 3:
                await update.message.reply_text(
                    "Для кастомного периода укажите диапазон дат в формате DD/MM/YYYY-DD/MM/YYYY. "
                    "Пример: /generate_report 2 custom 20/11/2024-25/11/2024"
                )
                return
            date_range_str = context.args[2]  # "11/11/2024-11/12/2024"
            try:
                # Парсинг диапазона дат
                start_date_str, end_date_str = context.args[2].split("-")
                start_date = datetime.strptime(
                    start_date_str.strip(), "%d/%m/%Y"
                ).date()
                end_date = datetime.strptime(end_date_str.strip(), "%d/%m/%Y").date()

                if start_date > end_date:
                    await update.message.reply_text(
                        "Начальная дата не может быть позже конечной."
                    )
                    return

                date_range = (start_date, end_date)
                logger.info(f"Кастомный диапазон дат: {start_date} - {end_date}")

            except ValueError as e:
                logger.error(f"Ошибка в формате дат: {context.args[2]} ({e})")
                await update.message.reply_text(
                    "Ошибка: Неверный формат дат. Укажите диапазон в формате DD/MM/YYYY-DD/MM/YYYY."
                )
                return

        # Проверка прав доступа пользователя
        current_user_role = context.user_data.get("user_role")
        restricted_roles = ["Operator", "Admin"]
        if current_user_role in restricted_roles and user_id != target_user_id:
            logger.warning(
                f"Пользователь {user_id} с ролью {current_user_role} не имеет прав для просмотра отчетов других пользователей."
            )
            await update.message.reply_text(
                "У вас нет прав для просмотра отчетов других пользователей."
            )
            return

        # Генерация отчета с подключением к базе данных
        try:
            logger.info(
                f"Начата генерация отчета для пользователя {target_user_id} за период '{period_str}'."
            )
            async with self.db_manager.acquire() as connection:
                logger.info(
                    f"Запрос на генерацию отчета для user_id {target_user_id} с периодом {period_str}"
                )

                # Генерация отчета (с кастомным диапазоном, если указан)
                if period_str == "custom":
                    report = await self.report_generator.generate_report(
                        connection,
                        target_user_id,
                        period=period_str,
                        date_range=date_range,
                    )
                else:
                    report = await self.report_generator.generate_report(
                        connection, target_user_id, period=period_str
                    )

            # Проверка, если отчёт пустой или не был сгенерирован
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
            logger.info(
                f"Отчет для пользователя {target_user_id} успешно сгенерирован и отправлен."
            )
            await update.message.reply_text("Отчет успешно сгенерирован и отправлен.")
        except Exception as e:
            logger.error(
                f"Ошибка при генерации отчета для пользователя {target_user_id}: {e}"
            )
            await update.message.reply_text(
                "Произошла ошибка при генерации отчета. Пожалуйста, попробуйте позже."
            )

    async def request_current_stats_handle(
        self, update: Update, context: CallbackContext
    ):
        """Обработчик команды /request_stats для получения текущей статистики."""
        user_id = update.effective_user.id
        logger.info(f"Команда /request_stats получена от пользователя {user_id}")
        operator_data = await self.db_manager.get_user_by_id(user_id)
        if not operator_data:
            await update.message.reply_text(
                "Ваш ID пользователя не найден. Пожалуйста, зарегистрируйтесь с помощью команды /register."
            )
            return
        try:
            async with self.db_manager.acquire() as connection:
                report_data = await self.report_generator.generate_report(
                    connection, user_id, period="daily"
                )
            if report_data is None:
                await update.message.reply_text(
                    f"Данные по пользователю с ID {user_id} не найдены."
                )
                logger.error(f"Данные по пользователю с ID {user_id} не найдены.")
                return

            report_text = self.generate_report_text(report_data)
            await self.send_long_message(update.effective_chat.id, report_text)
            logger.info(f"Статистика для пользователя {user_id} успешно отправлена.")
        except Exception as e:
            logger.error(
                f"Ошибка при получении статистики для пользователя {user_id}: {e}"
            )
            await update.message.reply_text(
                "Произошла ошибка при получении статистики."
            )

    async def report_summary_handle(self, update: Update, context: CallbackContext):
        """Обработчик команды /report_summary для генерации сводного отчёта."""
        user_id = update.effective_user.id
        logger.info(f"Команда /report_summary получена от пользователя {user_id}")

        # Проверяем права доступа пользователя
        current_user_role = await self.permissions_manager.get_user_role(user_id)
        if current_user_role not in [
            "Admin",
            "Developer",
            "SuperAdmin",
            "Head of Registry",
            "Founder",
            "Marketing Director",
        ]:
            await update.message.reply_text(
                "У вас нет прав для просмотра сводного отчёта."
            )
            return

        # Проверка наличия аргументов
        if len(context.args) < 1:
            await update.message.reply_text(
                "Пожалуйста, укажите период (daily, weekly, monthly, yearly или custom). "
                "Для custom укажите диапазон дат в формате DD/MM/YYYY-DD/MM/YYYY. "
                "Пример: /report_summary custom 01/10/2024-25/11/2024"
            )
            return

        period = context.args[0].lower()

        # Обработка кастомного периода
        if period == "custom":
            if len(context.args) < 2:
                await update.message.reply_text(
                    "Для custom периода укажите диапазон дат в формате DD/MM/YYYY-DD/MM/YYYY. "
                    "Пример: /report_summary custom 01/10/2024-25/11/2024"
                )
                return
            date_range_str = context.args[1]
            try:
                start_date_str, end_date_str = date_range_str.split("-")
                start_date = datetime.strptime(start_date_str.strip(), "%d/%m/%Y")
                end_date = datetime.strptime(end_date_str.strip(), "%d/%m/%Y")
            except ValueError:
                await update.message.reply_text(
                    "Ошибка: Неверный формат дат. Ожидается DD/MM/YYYY-DD/MM/YYYY."
                )
                return
        else:
            start_date, end_date = self.report_generator.get_date_range(period)

        # Устанавливаем соединение с БД
        connection = await create_async_connection()
        if not connection:
            await update.message.reply_text("Ошибка подключения к базе данных.")
            return

        # Создаём экземпляр генератора отчётов
        report_generator = OpenAIReportGenerator(self.db_manager)

        # Генерируем сводный отчёт
        report = await report_generator.generate_summary_report(
            connection, start_date, end_date
        )

        # Отправляем отчёт пользователю
        await self.send_long_message(update.effective_chat.id, report)

        # Закрываем соединение
        connection.close()

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

    from datetime import datetime, timedelta

    async def send_daily_reports(self):
        """Отправка ежедневных отчетов за предыдущий день."""
        logger.info(
            "Начата постановка задач на ежедневные отчеты для нескольких операторов."
        )
        try:
            # Список руководителей
            managers = [309606681]  # Укажите chat_id руководителей
            # Список операторов, по которым нужно сформировать отчеты
            operator_ids = [2, 5, 6, 8, 9, 10]  #

            # Формируем даты за предыдущий день
            yesterday = datetime.now() - timedelta(days=1)
            date_str = yesterday.strftime("%d/%m/%Y")
            date_range = f"{date_str}-{date_str}"

            # Для каждого руководителя добавляем задачи для каждого оператора
            for manager_chat_id in managers:
                for op_id in operator_ids:
                    await add_task(
                        bot_instance=self,
                        user_id=op_id,
                        report_type="custom",  # указываете что это custom период
                        period="custom",  # строго 'custom', без лишних слов
                        chat_id=manager_chat_id,
                        date_range=date_range,  # новый аргумент
                    )
                    logger.info(
                        f"Задача на отчет для оператора {op_id} за {date_range} добавлена."
                    )
            logger.info("Все задачи на ежедневные отчеты успешно поставлены в очередь.")
        except Exception as e:
            logger.error(f"Ошибка при постановке задач на ежедневные отчеты: {e}")

    async def generate_and_send_report(self, user_id, period):
        """Генерация и отправка отчета для конкретного пользователя."""
        try:
            async with self.db_manager.acquire() as connection:
                report = await self.report_generator.generate_report(
                    connection, user_id, period=period
                )

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
            - Общее время разговоров по телефону: {report_data['total_conversation_time']} мин.

        5. Работа с жалобами:
            - Звонки с жалобами: {report_data['complaint_calls']}
            - Оценка обработки жалобы: {report_data['complaint_rating']} из 10

        6. Рекомендации на основе данных:
        {report_data['recommendations']}
                """
        logger.info(
            f"[КРОТ]: МЕТОД ГЕНЕРАЦИИ ИЗ МЭЙНФАЙЛА, ТРЕТЬЯ ЛОВУШКА СРАБОТАЛА. Отчет успешно отформатирован"
        )
        return report_text

    async def send_message_with_retry(
        self, bot, chat_id, text, retry_attempts=3, parse_mode=None
    ):
        """
        Отправка сообщения с повторной попыткой в случае ошибки TimedOut.
        :param bot: экземпляр бота.
        :param chat_id: ID чата для отправки сообщения.
        :param text: Текст сообщения.
        :param retry_attempts: Количество попыток отправки сообщения.
        :param parse_mode: Форматирование текста (например, "Markdown" или "HTML").
        """
        for attempt in range(retry_attempts):
            try:
                await bot.send_message(
                    chat_id=chat_id, text=text, parse_mode=parse_mode
                )
                return
            except TimedOut:
                if attempt < retry_attempts - 1:
                    logger.warning(
                        f"Попытка {attempt + 1} из {retry_attempts} для отправки сообщения."
                    )
                    # Экспоненциальная задержка перед повтором
                    await asyncio.sleep(2**attempt)
                else:
                    logger.error(
                        "Не удалось отправить сообщение после нескольких попыток."
                    )

    async def send_long_message(self, chat_id, message: str, chunk_size: int = 4096):
        """
        Отправка длинного сообщения, разбивая его на части, если оно превышает максимальную длину.

        :param chat_id: ID чата, куда отправляется сообщение.
        :param message: Текст сообщения для отправки.
        :param chunk_size: Максимальный размер части сообщения (по умолчанию 4096 символов).
        """
        # Экранирование текста для HTML
        message_chunks = [
            message[i : i + chunk_size] for i in range(0, len(message), chunk_size)
        ]
        # Разбиваем сообщение на части, если оно длинное
        for chunk in message_chunks:
            try:
                # Экранируем текст, если используется HTML
                chunk = html.escape(chunk)  # Если используется HTML
                await self.application.bot.send_message(
                    chat_id=chat_id, text=chunk, parse_mode=ParseMode.HTML
                )
                await asyncio.sleep(0.1)  # Небольшая задержка между отправками
            except Exception as e:
                logger.error(f"Ошибка при отправке части сообщения: {e}")
                await self.application.bot.send_message(
                    chat_id=chat_id,
                    text="Произошла ошибка при отправке длинного сообщения. Пожалуйста, попробуйте позже.",
                )
                break

    async def error_handle(self, update: Update, context: CallbackContext):
        """Централизованная обработка ошибок."""
        logger.error(msg="Exception while handling an update:", exc_info=context.error)
        try:
            # Форматирование трассировки исключения
            tb_list = traceback.format_exception(
                None, context.error, context.error.__traceback__
            )
            tb_string = "".join(tb_list)

            # Получение строки представления обновления
            update_str = update.to_dict() if isinstance(update, Update) else str(update)
            tb_string_escaped = html.escape(tb_string)

            # Формирование сообщения об ошибке с экранированными символами
            message = (
                f"An exception was raised while handling an update\n"
                f"<pre>update = {update_str}</pre>\n\n"
                f"<pre>{tb_string_escaped}</pre>"
            )

            # Отправка сообщения об ошибке в Telegram
            if update and update.effective_chat:
                for message_chunk in split_text_into_chunks(message):
                    await self.send_message_with_retry(
                        self.application.bot,
                        update.effective_chat.id,
                        message_chunk,
                        parse_mode=ParseMode.HTML,
                    )
        except Exception as e:
            # Логирование ошибки, если что-то пошло не так в процессе обработки ошибки
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
                if result and result.get("chat_id"):
                    return result["chat_id"]
                else:
                    logger.error(
                        f"[КРОТ]: Не найден chat_id для пользователя с user_id {user_id}."
                    )
                    return None
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при получении chat_id пользователя: {e}")
            return None

    async def send_report_to_user(self, user_id, report_text):
        """Отправляет сформированный отчет пользователю через Telegram-бот."""
        async with self.db_manager.acquire() as connection:
            chat_id = await self.get_user_chat_id(connection, user_id)
        if not chat_id:
            logger.error(
                f"[КРОТ]: Не удалось получить chat_id для пользователя {user_id}."
            )
            return
        try:
            messages = [
                report_text[i : i + 4000] for i in range(0, len(report_text), 4000)
            ]
            for msg in messages:
                await self.send_message_with_retry(chat_id=chat_id, text=msg)
            logger.info(
                f"[КРОТ]: Отчет успешно отправлен пользователю с chat_id {chat_id}."
            )
        except TelegramError as e:
            logger.error(f"[КРОТ]: Бот заблокирован пользователем с chat_id {chat_id}.")
        else:
            logger.error(
                f"[КРОТ]: Ошибка при отправке отчета пользователю с chat_id {chat_id}: {e}"
            )

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
                logger.error(
                    "[КРОТ]: Не удалось найти заведующую регистратуры (role_id = 5) в базе данных."
                )
                return

            chief_telegram_username = result["username"]
            logger.info(
                f"[КРОТ]: Отправляем пароль заведующей регистратуры @{chief_telegram_username}"
            )
            message = f"Сгенерированный пароль для нового пользователя: {password}"
            url = f"https://api.telegram.org/bot{os.getenv('TELEGRAM_BOT_TOKEN')}/sendMessage"
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    url,
                    json={"chat_id": f"@{chief_telegram_username}", "text": message},
                )
            if response.status_code == 200:
                logger.info(
                    f"[КРОТ]: Пароль успешно отправлен заведующей @{chief_telegram_username}."
                )
            else:
                logger.error(
                    f"[КРОТ]: Не удалось отправить сообщение в Telegram. Код ошибки: {response.status_code}"
                )

    async def operator_progress_menu_handle(
        self, update: Update, context: CallbackContext
    ):
        user_id = update.effective_user.id
        logger.info(f"Команда /operator_progress_menu от {user_id}")

        if not context.user_data.get("is_authenticated"):
            await update.message.reply_text(
                "Сначала войдите с помощью /login ваш_пароль."
            )
            return

        try:
            async with self.db_manager.acquire() as connection:
                query = "SELECT DISTINCT name FROM reports ORDER BY name"
                async with connection.cursor() as cursor:
                    await cursor.execute(query)
                    operators = await cursor.fetchall()

            if not operators:
                await update.message.reply_text("Нет операторов в базе.")
                return

            # Создаем кнопки для каждого оператора
            keyboard = [
                [
                    InlineKeyboardButton(
                        str(op["name"]), callback_data=f"operator_{op['name']}"
                    )
                ]
                for op in operators
                if "name" in op and op["name"]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(
                "Выберите оператора:", reply_markup=reply_markup
            )
        except Exception as e:
            logger.error(f"Ошибка при получении списка операторов: {e}", exc_info=True)
            await update.message.reply_text("Произошла ошибка при загрузке операторов.")

    async def operator_callback_handle(self, update: Update, context: CallbackContext):
        query = update.callback_query
        data = query.data
        await query.answer()

        if data.startswith("operator_"):
            operator_name = data.split("operator_")[1]

            # Предлагаем выбрать период
            keyboard = [
                [
                    InlineKeyboardButton(
                        "День (daily)", callback_data=f"period_{operator_name}_daily"
                    ),
                    InlineKeyboardButton(
                        "Неделя (weekly)",
                        callback_data=f"period_{operator_name}_weekly",
                    ),
                ],
                [
                    InlineKeyboardButton(
                        "Месяц (monthly)",
                        callback_data=f"period_{operator_name}_monthly",
                    ),
                    InlineKeyboardButton(
                        "Год (yearly)", callback_data=f"period_{operator_name}_yearly"
                    ),
                ],
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await query.edit_message_text(
                text=f"Оператор: {operator_name}\nВыберите период:",
                reply_markup=reply_markup,
            )

        elif data.startswith("period_"):
            # формат: period_{operator_name}_{period}
            parts = data.split("_", 2)
            operator_name = parts[1]
            period_str = parts[2]

            await self.generate_operator_progress(query, operator_name, period_str)

    def parse_report_date(self, report_date_str):
        """
        Преобразует строку даты или диапазона дат в объект datetime.
        """
        if " - " in report_date_str:
            # Берем первую дату из диапазона
            first_date_str = report_date_str.split(" - ")[0].strip()
            return datetime.strptime(first_date_str, "%Y-%m-%d")
        else:
            # Одиночная дата
            return datetime.strptime(report_date_str, "%Y-%m-%d")

    def remove_duplicates(data, key="report_date"):
        """
        Удаляет дублирующиеся записи по указанному ключу.
        """
        seen = set()
        unique_data = []
        for row in data:
            val = row[key]
            if val not in seen:
                unique_data.append(row)
                seen.add(val)
        return unique_data

    def calculate_trends(data, metrics):
        """
        Рассчитывает тренды метрик с учетом средней динамики по каждой метрике.

        Параметры:
            data (List[Dict]): данные с отсортированными датами.
            metrics (List[str]): метрики для анализа.

        Возвращает:
            Dict[str, str]: тренды в формате "метрика: тренд".
        """
        trends = {}
        for m in metrics:
            values = [row[m] for row in data if row[m] is not None]
            if len(values) >= 2:
                diff = values[-1] - values[0]
                avg_diff = sum(values) / len(values)
                trend = (
                    "выросла"
                    if diff > 0
                    else "упала" if diff < 0 else "осталась на месте"
                )
                trends[m] = (
                    f"{m}: {trend} (начальное {values[0]}, конечное {values[-1]}, среднее изменение {avg_diff:.2f})"
                )
            else:
                trends[m] = f"{m}: недостаточно данных для анализа."
        return trends

    async def generate_operator_progress(
        self, query, operator_name: str, period_str: str
    ):
        try:
            # Определяем даты периода
            start_date, end_date = self.parse_period(period_str)
            start_str = start_date.strftime("%Y-%m-%d")
            end_str = end_date.strftime("%Y-%m-%d")

            # Получаем данные из таблицы reports
            async with self.db_manager.acquire() as connection:
                query_sql = """
                SELECT report_date, avg_call_rating, total_calls, accepted_calls, booked_services, complaint_calls, conversion_rate
                FROM reports
                WHERE name = %s AND report_date BETWEEN %s AND %s
                ORDER BY report_date ASC
                """
                async with connection.cursor() as cursor:
                    await cursor.execute(query_sql, (operator_name, start_str, end_str))
                    reports_data = await cursor.fetchall()

            if not reports_data:
                await query.edit_message_text(
                    f"Нет данных за период {period_str} для {operator_name}."
                )
                return

            # Формируем данные для графика
            print("DEBUG: Начинаем преобразование данных для графика")
            transformed_data = []
            for row in reports_data:
                print(f"DEBUG: report_date = {row['report_date']}, данные = {row}")
                try:
                    transformed_data.append(
                        {
                            "report_date": self.parse_report_date(row["report_date"]),
                            "avg_call_rating": row.get("avg_call_rating", 0),
                            "total_calls": row.get("total_calls", 0),
                            "accepted_calls": row.get("accepted_calls", 0),
                            "booked_services": row.get("booked_services", 0),
                            "complaint_calls": row.get("complaint_calls", 0),
                            "conversion_rate": row.get("conversion_rate", 0),
                        }
                    )
                except ValueError as e:
                    print(f"Пропущена запись из-за ошибки: {e}")

            # Метрики
            metrics_to_plot = [
                "avg_call_rating",
                "total_calls",
                "accepted_calls",
                "booked_services",
                "complaint_calls",
                "conversion_rate",
            ]

            # Генерация заголовка
            title = (
                f"Динамика метрик для {operator_name} за период {start_str} - {end_str}"
            )

            # Генерация графика
            image_path = await create_multi_metric_graph(
                data=transformed_data,
                metrics=["avg_call_rating", "total_calls", "accepted_calls"],
                operator_name=operator_name,
                title=title,
            )
            # Генерация комментария к метрикам
            commentary = await self.generate_commentary_on_metrics(
                data=transformed_data,
                metrics=metrics_to_plot,
                operator_name=operator_name,
                period_str=f"{start_str} - {end_str}",
            )

            # Отправляем график и комментарии
            if image_path and os.path.exists(image_path):
                with open(image_path, "rb") as img:
                    await self.application.bot.send_photo(
                        chat_id=query.message.chat_id, photo=img
                    )
                await query.edit_message_text(
                    text=f"Динамика метрик для оператора {operator_name} за период {period_str}:\n\n{commentary}"
                )
            else:
                await query.edit_message_text("Не удалось построить график.")
        except Exception as e:
            logger.error(
                f"Ошибка при генерации динамики для {operator_name}: {e}", exc_info=True
            )
            await query.edit_message_text("Произошла ошибка при формировании динамики.")

    async def generate_commentary_on_metrics(
        self, data, metrics, operator_name, period_str
    ):
        """
        Генерация комментариев к изменениям метрик оператора за указанный период с использованием OpenAI API.

        :param data: Список данных с метриками.
        :param metrics: Список метрик для анализа.
        :param operator_name: Имя оператора.
        :param period_str: Строковое представление периода.
        :return: Комментарий в виде строки.
        """
        if not data or not metrics:
            return "Данных недостаточно для анализа."

        # Составляем подробное описание динамики метрик
        trends = []
        for metric in metrics:
            values = [row.get(metric) for row in data if row.get(metric) is not None]
            dates = [
                row.get("report_date") for row in data if row.get(metric) is not None
            ]

            if values and len(values) > 1:
                max_val = max(values)
                min_val = min(values)
                max_date = dates[values.index(max_val)]
                min_date = dates[values.index(min_val)]

                # Тренд: динамика от первой к последней точке
                initial = values[0]
                final = values[-1]
                trend = (
                    "выросли"
                    if final > initial
                    else "упали" if final < initial else "остались на месте"
                )

                trends.append(
                    f"""Метрика `{metric}`:
                    - Максимум был {max_val:.2f} ({max_date.strftime('%Y-%m-%d')}), затем наблюдалось {trend} до {final:.2f} ({dates[-1].strftime('%Y-%m-%d')}).
                    - Минимум: {min_val:.2f} ({min_date.strftime('%Y-%m-%d')}).
                    """
                )
            else:
                trends.append(f"Метрика `{metric}`: данных недостаточно для анализа.")

        # Формируем текстовый запрос для OpenAI API
        prompt = f"""
        Оператор: {operator_name}
        Период: {period_str}
        Ниже приведены изменения ключевых метрик за период:
        {chr(10).join(trends)}

        Напиши краткий аналитический комментарий:
        1. Укажи сильные стороны оператора.
        2. Опиши ключевые области для улучшения.
        3. Сформулируй общий вывод и рекомендации.
        """

        # Вызов OpenAI API
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=1000,  # Увеличенный лимит для более детального анализа
                temperature=0.7,
            )
            commentary = response.choices[0].message.content.strip()
        except Exception as e:
            self.logger.error(f"Ошибка при вызове OpenAI API: {e}")
            commentary = (
                "Произошла ошибка при формировании комментария. Попробуйте позже."
            )

        return commentary

    async def error_handle(self, update: Update, context: CallbackContext):
        logger.error("Exception while handling an update:", exc_info=context.error)
        # Логика обработки ошибок

    async def operator_progress_handle(self, update: Update, context: CallbackContext):
        """
        Обработчик команды /operator_progress.
        Показывает динамику по метрике для выбранного оператора за указанный период.
        """
        user_id = update.effective_user.id
        logger.info(f"Команда /operator_progress получена от пользователя {user_id}")

        if not context.user_data.get("is_authenticated"):
            await update.message.reply_text(
                "Сначала войдите с помощью /login ваш_пароль."
            )
            return

        if len(context.args) < 2:
            await update.message.reply_text(
                "Укажите ID оператора и период. Пример: /operator_progress 5 monthly\n"
                "Для custom периода: /operator_progress 5 custom 01/10/2024-25/11/2024"
            )
            return

        target_user_id = int(context.args[0])
        period_str = context.args[1].lower()

        # Определяем период
        try:
            start_date, end_date = self.parse_period(period_str)
        except ValueError as e:
            await update.message.reply_text(str(e))
            return

        # Получение данных для графика
        try:
            reports_data = await self.progress_data.get_operator_reports(
                target_user_id, start_date, end_date
            )
            if not reports_data:
                await update.message.reply_text("Нет данных за указанный период.")
                return

            metric_name = "avg_call_rating"
            transformed_data = [
                {"date": row["date"], "metric_value": row[metric_name]}
                for row in reports_data
            ]

            image_path = await create_all_operators_progress_graph(
                transformed_data, metric_name, f"Operator {target_user_id}"
            )
            if os.path.exists(image_path):
                with open(image_path, "rb") as img:
                    await self.application.bot.send_photo(
                        chat_id=update.effective_chat.id, photo=img
                    )
            else:
                await update.message.reply_text("Не удалось построить график.")
        except Exception as e:
            logger.error(f"Ошибка: {e}", exc_info=True)
            await update.message.reply_text("Произошла ошибка при обработке команды.")

    async def all_operators_progress_handle(
        self, update: Update, context: CallbackContext
    ):
        """
        Обработчик команды /all_operators_progress [period].
        Показывает сводную динамику для всех операторов за указанный период.
        """
        user_id = update.effective_user.id
        logger.info(
            f"Команда /all_operators_progress получена от пользователя {user_id}"
        )

        if not context.user_data.get("is_authenticated"):
            await update.message.reply_text(
                "Сначала войдите с помощью /login ваш_пароль."
            )
            return

        # Проверка аргументов
        if len(context.args) < 1:
            await update.message.reply_text(
                "Укажите период. Пример: /all_operators_progress monthly\n"
                "Для custom периода: /all_operators_progress custom 01/10/2024-25/11/2024"
            )
            return

        period_str = context.args[0].lower()

        # Определяем даты
        try:
            if period_str == "custom" and len(context.args) > 1:
                start_date_str, end_date_str = context.args[1].split("-")
                start_date = datetime.strptime(
                    start_date_str.strip(), "%d/%m/%Y"
                ).date()
                end_date = datetime.strptime(end_date_str.strip(), "%d/%m/%Y").date()
            else:
                start_date, end_date = self.parse_period(period_str)
        except ValueError as e:
            await update.message.reply_text(str(e))
            return

        # Получение данных из базы
        try:
            reports_data = await self.progress_data.get_all_operators_reports(
                start_date, end_date
            )
            if not reports_data:
                await update.message.reply_text("Нет данных за указанный период.")
                return

            # Преобразуем данные для графика
            metric_name = "avg_call_rating"
            transformed_data = []
            for row in reports_data:
                transformed_data.append(
                    {
                        "name": row[
                            "operator_id"
                        ],  # Пример: замените на имя, если оно есть в данных
                        "date": row["date"],
                        "metric_value": row[metric_name],
                    }
                )

            image_path = await create_all_operators_progress_graph(
                transformed_data, metric_name
            )
            if os.path.exists(image_path):
                with open(image_path, "rb") as img:
                    await self.application.bot.send_photo(
                        chat_id=update.effective_chat.id, photo=img
                    )
            else:
                await update.message.reply_text("Не удалось построить график.")
        except Exception as e:
            logger.error(f"Ошибка: {e}", exc_info=True)
            await update.message.reply_text("Произошла ошибка при обработке команды.")

    async def callback_query_handler(self, update: Update, context: CallbackContext):
        query = update.callback_query
        data = query.data

        if data.startswith("operator_"):
            operator_name = data.split("_")[1]
            # Логика для выбора периода
            await self.operator_callback_handle(update, context)

        elif data.startswith("period_"):
            _, operator_name, period = data.split("_", 2)
            await self.generate_operator_progress(query, operator_name, period)

        elif data.startswith("op_prog:"):
            # Обработка сводной статистики
            parts = data.split(":")
            operator_id = int(parts[1])
            start_date = datetime.strptime(parts[2], "%Y-%m-%d").date()
            end_date = datetime.strptime(parts[3], "%Y-%m-%d").date()
            # Логика отображения графика для конкретного оператора


# Основная функция для запуска бота
async def main():
    logger.info("Запуск бота...")
    if not config.telegram_token:
        raise ValueError("Telegram token отсутствует в конфигурации")
    bot = TelegramBot(config.telegram_token)
    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
