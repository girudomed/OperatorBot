##bot.py
import copy
import asyncio
import atexit
import fcntl
from functools import wraps
import html
from threading import Lock
import uuid
import aiomysql
import httpx
import json
import logging
import os
import queue
import re
import sys
import traceback
import time
from datetime import date, datetime, timedelta
from enum import Enum
from io import BytesIO
from logging.handlers import RotatingFileHandler, QueueHandler, QueueListener
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
    Type,
    TypeVar,
    Protocol,
    cast,
    TypedDict,
    Literal,
)

import nest_asyncio
import numpy as np
import numpy as np
import pandas as pd
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
    BotCommand,
    CallbackQuery,
    Bot,
)
import telegram
from telegram.constants import ParseMode
from telegram.error import TimedOut, TelegramError
from telegram.ext import (
    ApplicationBuilder,
    CallbackContext,
    CommandHandler,
    AIORateLimiter,
    filters,
    CallbackQueryHandler,
    Application,
)
from telegram.request import HTTPXRequest

import config
from auth import AuthManager, setup_auth_handlers
from auth import AuthManager
import db_manager
from db_module import DatabaseManager
from logger_utils import setup_logging
from openai import AsyncOpenAI
from openai_telebot import OpenAIReportGenerator, create_async_connection
from operator_data import OperatorData
from db_module import DatabaseManager

from permissions_manager import PermissionsManager
from progress_data import ProgressData
from visualization import (
    create_multi_metric_graph,
    calculate_trends,
    create_all_operators_progress_graph,
    MetricsVisualizer,
    GlobalConfig,
    PlotConfig,
    MetricsConfig,
)
from urllib.parse import quote, unquote
from visualization import MetricsVisualizer
from config import openai_api_key
import matplotlib.dates as mdates
from logger_utils import setup_logging

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


logger = setup_logging(
    log_file="logs.log",
    log_level=logging.INFO,
    max_log_lines=150000,
    average_line_length=100,
    backup_count=5,  # Количество резервных копий
    json_format=False,
    use_queue=True,
    telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
    telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID"),
)

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
    read_timeout=30.0,  # Таймаут на чтение
    write_timeout=15.0,  # Таймаут на запись
    connect_timeout=10.0,  # Таймаут на подключение
)

# Инициализация приложения Telegram
telegram_token = "YOUR_BOT_TOKEN"
logger.info("Настройка приложения Telegram...")

# Задачи
MAX_CONCURRENT_TASKS = 3
task_queue = asyncio.Queue()


async def start_workers(bot_instance):
    for i in range(MAX_CONCURRENT_TASKS):
        asyncio.create_task(worker(task_queue, bot_instance))


async def worker(queue: asyncio.Queue, bot_instance):
    while True:
        task = await queue.get()
        user_id = task["user_id"]
        report_type = task["report_type"]
        period = task["period"]
        chat_id = task["chat_id"]
        date_range = task["date_range"]

        try:
            async with bot_instance.db_manager.acquire() as connection:
                report = await bot_instance.report_generator.generate_report(
                    connection, user_id, period=period, date_range=date_range
                )

            # Вот тут проблема:
            # bot_instance.send_long_message(chat_id, report)
            # -> если chat_id=None -> BadRequest

            if chat_id is not None:
                # ... тогда отправим сообщение
                if report and not report.startswith("Ошибка:"):
                    await bot_instance.send_long_message(chat_id, report)
                    logger.info(f"Отчёт для user_id={user_id} отправлен.")
                else:
                    #msg = report or "Ошибка или нет данных"
                    #await bot_instance.application.bot.send_message(
                        #chat_id=chat_id, text=msg
                    #)
                    logger.info(f"Отчёт для user_id={user_id} отправлен (или ошибка).")
            else:
                # chat_id=None => это оператор => ничего не отправляем
                logger.debug(
                    f"chat_id=None, это оператор {user_id}. "
                    f"Отчёт сгенерирован и сохранён (без отправки в чат)."
                )

        except Exception as e:
            logger.error(
                f"Ошибка при обработке задачи для user_id={user_id}: {e}", exc_info=True
            )
            if chat_id:
                await bot_instance.application.bot.send_message(
                    chat_id=chat_id,
                    text="Произошла ошибка при генерации отчёта. Попробуйте позже.",
                )
        finally:
            queue.task_done()
            logger.info(f"Воркеры завершили обработку задачи: {task}")


async def add_task(
    bot_instance, user_id, report_type, period, chat_id=None, date_range=None
):
    """
    Добавляет задачу в очередь на генерацию отчёта.
    Если chat_id=None, значит это оператор, которому не нужно отправлять сообщение в Telegram.
    Если chat_id - int, значит это менеджер, которому можно отправить "Ваш запрос поставлен в очередь".
    """
    # Формируем задачу
    task = {
        "user_id": user_id,
        "report_type": report_type,
        "period": period,
        "chat_id": chat_id,
        "date_range": date_range,
    }
    await task_queue.put(task)
    logger.info(
        f"Задача добавлена в очередь для user_id={user_id}, report_type={report_type}, period={period}."
    )

    # Если есть chat_id (менеджер) — отправим уведомление
    if isinstance(chat_id, int):
        logger.debug(f"Уведомление не отправляется для chat_id={chat_id}.")

        #try:
            #await bot_instance.application.bot.send_message(
                #chat_id=chat_id, text="Ваш запрос поставлен в очередь на обработку."
            #)
        #except Exception as e:
            #logger.warning(
                #f"Ошибка отправки уведомления chat_id={chat_id} (user_id={user_id}): {e}"
            #)
    else:
        # Оператору не отправляем, но и не пишем в лог как ошибку
        logger.debug(f"chat_id=None для user_id={user_id}, уведомление не требуется.")


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
        4    ЧС ст.админ
        5	 Ирина
        6	 Энзе
        7	 ПП Ст.админ
        8	 Ресепшн ГВ
        9	 Ресепшн ПП
        10   Анастасия
        11  Рецепшн ЧС

        Для генерации отчета по операторам с рекомендациями используйте команду: "/generate_report 5 custom 01/10/2024-25/11/2024", где custom является важной переменной после главной команды, также дата должна строго быть в таком формате
        Для генерации отчета по всем операторам без упоминании позывного без рекомендацией используйте команду: "/report_summary custom 01/10/2024-25/11/2024"
        Если вы нажали не ту команду, то выполните команду "/cancel"
        
        Сначала необходимо зайти в бота через команду /login введя пароль выданный из БД.
            
        По вопросам работы бота обращаться в отдел маркетинга Гирудомед.

        Внимание! Если включена отправка ежедневных отчётов и вы видете "Ошибка при извлечении данных оператора или данных нет." - это значит, что по какому-то оператору данных в базе нету. Это нормально.

        Ежедневные отчеты отправляются в 7:00 по мск руководителям
    
    """


# Функция для разделения текста на части
def split_text_into_chunks(text, chunk_size=4096):
    """Разделение текста на части для отправки длинных сообщений."""
    for i in range(0, len(text), chunk_size):
        yield text[i : i + chunk_size]


class ErrorSeverity(Enum):
    """Уровни серьезности ошибок."""

    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class ErrorContext:
    """Контекст ошибки для расширенной обработки."""

    def __init__(
        self,
        error: Exception,
        severity: ErrorSeverity,
        user_id: Union[int, str],
        function_name: str,
        additional_data: Dict[str, Any] = None,
    ):
        self.error = error
        self.severity = severity
        self.user_id = user_id
        self.function_name = function_name
        self.timestamp = datetime.now()
        self.additional_data = additional_data or {}

    def to_dict(self) -> Dict[str, Any]:
        """Преобразование контекста в словарь для логирования."""
        return {
            "error_type": self.error.__class__.__name__,
            "error_message": str(self.error),
            "severity": self.severity.value,
            "user_id": self.user_id,
            "function": self.function_name,
            "timestamp": self.timestamp.isoformat(),
            "additional_data": self.additional_data,
        }


class BotError(Exception):
    """Базовый класс для ошибок бота."""

    def __init__(
        self,
        message: str,
        user_message: str = None,
        severity: ErrorSeverity = ErrorSeverity.ERROR,
        details: Dict[str, Any] = None,
        retry_allowed: bool = True,
    ):
        super().__init__(message)
        self.user_message = user_message or message
        self.severity = severity
        self.details = details or {}
        self.retry_allowed = retry_allowed
        self.timestamp = datetime.now()

    def get_user_message(self, include_details: bool = True) -> str:
        """Формирует сообщение для пользователя."""
        message = self.user_message
        if include_details and self.details:
            message += "\n\nПодробности:\n"
            for key, value in self.details.items():
                message += f"• {key}: {value}\n"
        return message


class RetryableError(BotError):
    """Ошибка, которую можно повторить."""

    def __init__(
        self,
        message: str,
        user_message: str = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        **kwargs,
    ):
        super().__init__(message, user_message, **kwargs)
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.retry_count = 0


class RateLimitError(RetryableError):
    """Ошибка превышения лимита запросов."""

    def __init__(self, message: str, reset_time: datetime = None, **kwargs):
        super().__init__(message, **kwargs)
        self.reset_time = reset_time

    def get_user_message(self, include_details: bool = True) -> str:
        message = super().get_user_message(include_details)
        if self.reset_time:
            wait_time = (self.reset_time - datetime.now()).total_seconds()
            if wait_time > 0:
                message += f"\n\nПопробуйте снова через {int(wait_time)} секунд."
        return message


class ErrorHandler:
    """Класс для централизованной обработки ошибок."""

    def __init__(self, bot_instance):
        self.bot = bot_instance
        self.logger = logging.getLogger("bot")
        self._error_configs = self._get_default_error_configs()
        self._notification_rules = self._get_default_notification_rules()
        self._retry_policies = self._get_default_retry_policies()

    @property
    def error_configs(self) -> Dict[Type[Exception], Dict[str, Any]]:
        """Конфигурация обработки ошибок."""
        return self._error_configs

    @property
    def notification_rules(self) -> Dict[ErrorSeverity, Dict[str, Any]]:
        """Правила уведомлений."""
        return self._notification_rules

    @property
    def retry_policies(self) -> Dict[Type[Exception], Dict[str, Any]]:
        """Политики повторных попыток."""
        return self._retry_policies

    def _get_default_error_configs(self) -> Dict[Type[Exception], Dict[str, Any]]:
        """Возвращает конфигурацию обработки ошибок по умолчанию."""
        return {
            AuthenticationError: {
                "message": "🔒 Ошибка аутентификации",
                "severity": ErrorSeverity.WARNING,
                "log_level": "warning",
                "retry_count": 0,
                "notify_admin": False,
            },
            PermissionError: {
                "message": "🚫 Недостаточно прав",
                "severity": ErrorSeverity.WARNING,
                "log_level": "warning",
                "retry_count": 0,
                "notify_admin": False,
            },
            ValidationError: {
                "message": "⚠️ Некорректные данные",
                "severity": ErrorSeverity.WARNING,
                "log_level": "warning",
                "retry_count": 0,
                "notify_admin": False,
            },
            DataProcessingError: {
                "message": "🔄 Ошибка обработки данных",
                "severity": ErrorSeverity.ERROR,
                "log_level": "error",
                "retry_count": 2,
                "notify_admin": True,
            },
            VisualizationError: {
                "message": "📊 Ошибка создания графика",
                "severity": ErrorSeverity.ERROR,
                "log_level": "error",
                "retry_count": 1,
                "notify_admin": True,
            },
            RateLimitError: {
                "message": "⏳ Превышен лимит запросов",
                "severity": ErrorSeverity.INFO,
                "log_level": "info",
                "retry_count": 3,
                "retry_delay": 5.0,
                "notify_admin": False,
            },
            ExternalServiceError: {
                "message": "🌐 Ошибка внешнего сервиса",
                "severity": ErrorSeverity.ERROR,
                "log_level": "error",
                "retry_count": 2,
                "notify_admin": True,
            },
        }

    def _get_default_notification_rules(self) -> Dict[ErrorSeverity, Dict[str, Any]]:
        """Возвращает правила уведомлений по умолчанию."""
        return {
            ErrorSeverity.DEBUG: {
                "notify_admin": False,
                "notification_format": "simple",
            },
            ErrorSeverity.INFO: {
                "notify_admin": False,
                "notification_format": "simple",
            },
            ErrorSeverity.WARNING: {
                "notify_admin": False,
                "notification_format": "detailed",
            },
            ErrorSeverity.ERROR: {
                "notify_admin": True,
                "notification_format": "detailed",
            },
            ErrorSeverity.CRITICAL: {
                "notify_admin": True,
                "notification_format": "full",
            },
        }

    def _get_default_retry_policies(self) -> Dict[Type[Exception], Dict[str, Any]]:
        """Возвращает политики повторных попыток по умолчанию."""
        return {
            RateLimitError: {
                "max_retries": 3,
                "base_delay": 5.0,
                "max_delay": 30.0,
                "exponential_backoff": True,
            },
            DataProcessingError: {
                "max_retries": 2,
                "base_delay": 1.0,
                "max_delay": 5.0,
                "exponential_backoff": False,
            },
            ExternalServiceError: {
                "max_retries": 2,
                "base_delay": 2.0,
                "max_delay": 10.0,
                "exponential_backoff": True,
            },
        }

    def update_error_config(
        self, error_type: Type[Exception], config: Dict[str, Any]
    ) -> None:
        """Обновляет конфигурацию для определенного типа ошибки."""
        if error_type in self.error_configs:
            self.error_configs[error_type].update(config)
        else:
            self.error_configs[error_type] = config

    def get_error_config(self, error: Exception) -> Dict[str, Any]:
        """Получает конфигурацию для конкретной ошибки."""
        error_type = type(error)

        # Ищем точное совпадение
        if error_type in self.error_configs:
            return self.error_configs[error_type]

        # Ищем по иерархии классов
        for err_type, config in self.error_configs.items():
            if isinstance(error, err_type):
                return config

        # Возвращаем конфигурацию по умолчанию
        return {
            "message": "❌ Произошла ошибка",
            "severity": ErrorSeverity.ERROR,
            "log_level": "error",
            "retry_count": 0,
            "notify_admin": True,
        }

    async def handle_error(
        self, error: Exception, context: Dict[str, Any]
    ) -> Tuple[str, bool]:
        """
        Обрабатывает ошибку согласно конфигурации.

        Args:
            error: Возникшая ошибка
            context: Контекст ошибки (функция, пользователь и т.д.)

        Returns:
            Tuple[str, bool]: Сообщение об ошибке и флаг успешности обработки
        """
        logging.info("Начало обработки ошибки.")
        logging.debug(f"Ошибка: {error}")
        logging.debug(f"Контекст: {context}")

        try:
            # Получение конфигурации ошибки
            config = self.get_error_config(error)
            logging.debug(f"Конфигурация для ошибки: {config}")

            severity = config.get("severity", "unknown")
            logging.info(f"Серьёзность ошибки: {severity}")

            # Создание контекста ошибки
            error_context = ErrorContext(
                error=error,
                severity=severity,
                user_id=context.get("user_id", "Unknown"),
                function_name=context.get("function_name", "Unknown"),
                additional_data=context,
            )
            logging.debug(f"Созданный контекст ошибки: {error_context.to_dict()}")

            # Логирование ошибки
            logging.info("Логирование ошибки.")
            self._log_error(error_context, config)

            # Уведомление администратора, если требуется
            if (
                config.get("notify_admin", False)
                or self.notification_rules[severity]["notify_admin"]
            ):
                logging.info("Уведомление администратора об ошибке.")
                await self._notify_admin(error_context)
            else:
                logging.info("Уведомление администратора не требуется.")

            # Формирование сообщения для пользователя
            logging.info("Формирование сообщения для пользователя.")
            user_message = self._format_user_message(error, config)
            logging.debug(f"Сообщение для пользователя: {user_message}")

            return user_message, True

        except Exception as handling_error:
            # Логируем ошибку в обработчике ошибок
            logging.error("Ошибка при обработке исключения.", exc_info=True)
            logging.error(f"Изначальная ошибка: {error}")
            logging.error(f"Контекст: {context}")
            logging.error(f"Ошибка в обработчике: {handling_error}")

            # Возврат общего сообщения для пользователя
            return "Произошла непредвиденная ошибка. Попробуйте позже.", False

    def _log_error(self, error_context: ErrorContext, config: Dict[str, Any]) -> None:
        """Логирует ошибку с учетом конфигурации."""
        log_level = config["log_level"]
        log_message = json.dumps(error_context.to_dict(), indent=2)

        if hasattr(self.logger, log_level):
            log_func = getattr(self.logger, log_level)
            log_func(log_message, exc_info=True)
        else:
            self.logger.error(log_message, exc_info=True)

    async def _notify_admin(self, error_context: ErrorContext) -> None:
        """Уведомляет администратора об ошибке."""
        notification_format = self.notification_rules[error_context.severity][
            "notification_format"
        ]

        if notification_format == "simple":
            message = (
                f"🚨 {error_context.severity.value.upper()}\n"
                f"Error: {str(error_context.error)}"
            )
        elif notification_format == "detailed":
            message = (
                f"🚨 {error_context.severity.value.upper()}\n"
                f"Function: {error_context.function_name}\n"
                f"User ID: {error_context.user_id}\n"
                f"Error: {str(error_context.error)}"
            )
        else:  # full
            message = (
                f"🚨 {error_context.severity.value.upper()}\n"
                f"{json.dumps(error_context.to_dict(), indent=2)}"
            )

        await self.bot.notify_admin(message)

    def _format_user_message(self, error: Exception, config: Dict[str, Any]) -> str:
        """Форматирует сообщение об ошибке для пользователя."""
        if isinstance(error, BotError):
            message = error.get_user_message()
        else:
            message = config["message"]

        if isinstance(error, RetryableError):
            message += f"\n\nПопытка {error.retry_count + 1}/{error.max_retries}"

        if isinstance(error, RateLimitError) and error.reset_time:
            wait_time = (error.reset_time - datetime.now()).total_seconds()
            if wait_time > 0:
                message += f"\n\nПопробуйте снова через {int(wait_time)} секунд"

        return message

    def get_retry_policy(self, error: Exception) -> Dict[str, Any]:
        """
        Получает политику повторных попыток для ошибки.

        Args:
            error: Возникшая ошибка

        Returns:
            Dict[str, Any]: Политика повторных попыток
        """
        error_type = type(error)

        # Проверяем точное совпадение
        if error_type in self.retry_policies:
            return self.retry_policies[error_type]

        # Проверяем по иерархии классов
        for err_type, policy in self.retry_policies.items():
            if isinstance(error, err_type):
                return policy

        # Возвращаем политику по умолчанию
        return {
            "max_retries": 0,
            "base_delay": 1.0,
            "max_delay": 5.0,
            "exponential_backoff": False,
        }

    def calculate_retry_delay(self, policy: Dict[str, Any], retry_count: int) -> float:
        """
        Вычисляет задержку для повторной попытки.

        Args:
            policy: Политика повторных попыток
            retry_count: Номер текущей попытки

        Returns:
            float: Время задержки в секундах
        """
        base_delay = policy["base_delay"]
        max_delay = policy["max_delay"]

        if policy["exponential_backoff"]:
            delay = base_delay * (2 ** (retry_count - 1))
        else:
            delay = base_delay * retry_count

        return min(delay, max_delay)

    async def handle_retry(
        self, error: Exception, retry_count: int, context: Dict[str, Any]
    ) -> Tuple[bool, float]:
        """
        Обрабатывает логику повторных попыток.

        Args:
            error: Возникшая ошибка
            retry_count: Текущий счетчик попыток
            context: Контекст ошибки

        Returns:
            Tuple[bool, float]: (можно_повторить, задержка)
        """
        policy = self.get_retry_policy(error)
        max_retries = policy["max_retries"]

        if retry_count >= max_retries:
            return False, 0.0

        delay = self.calculate_retry_delay(policy, retry_count + 1)

        # Логируем информацию о повторной попытке
        self.logger.info(
            f"Retry {retry_count + 1}/{max_retries} for {context['function_name']}. "
            f"Waiting {delay:.1f}s"
        )

        return True, delay

    def handle_bot_exceptions(func: Callable):
        """
        Декоратор для обработки исключений с использованием ErrorHandler.

        Использует конфигурацию на уровне класса через ErrorHandler.
        """

        @wraps(func)
        async def wrapper(
            self, update: Update, context: CallbackContext, *args, **kwargs
        ):
            retry_count = 0
            logging.info(f"Начало выполнения функции {func.__name__}.")

            if update:
                logging.debug(f"Получен update: {update.to_dict()}")
            if context:
                logging.debug(f"Контекст: {context.__dict__}")

            while True:
                try:
                    logging.info(f"Выполнение основной логики функции {func.__name__}.")
                    return await func(self, update, context, *args, **kwargs)

                except Exception as e:
                    # Логируем информацию об ошибке
                    logging.error(
                        f"Исключение в функции {func.__name__}: {e}", exc_info=True
                    )

                    # Формируем контекст ошибки
                    error_context = {
                        "user_id": (
                            update.effective_user.id
                            if update and update.effective_user
                            else "Unknown"
                        ),
                        "chat_id": (
                            update.effective_chat.id
                            if update and update.effective_chat
                            else None
                        ),
                        "function_name": func.__name__,
                        "command": (
                            context.args[0] if context and context.args else None
                        ),
                        "retry_count": retry_count,
                    }
                    logging.debug(f"Контекст ошибки: {error_context}")

                    # Проверяем возможность повтора
                    can_retry, delay = await self.error_handler.handle_retry(
                        e, retry_count, error_context
                    )
                    logging.info(
                        f"Возможность повторной попытки: {'Да' if can_retry else 'Нет'}, Задержка: {delay} секунд"
                    )

                    if can_retry:
                        retry_count += 1
                        logging.info(
                            f"Попытка {retry_count} для функции {func.__name__}. Ожидание {delay} секунд."
                        )
                        await asyncio.sleep(delay)
                        continue

                    # Обрабатываем ошибку
                    user_message, success = await self.error_handler.handle_error(
                        e, error_context
                    )
                    logging.debug(
                        f"Сообщение для пользователя: {user_message}, Успешность обработки: {success}"
                    )

                    # Отправляем ответ пользователю
                    if isinstance(update, CallbackQuery):
                        await update.answer()
                        message = update.message
                    else:
                        message = update.message if update else None

                    if message:
                        logging.info("Подготовка ответа пользователю.")
                        markup = None
                        error_config = self.error_handler.get_error_config(e)
                        logging.debug(f"Конфигурация ошибки: {error_config}")

                        # Добавляем кнопку повтора, если применимо
                        if isinstance(e, RetryableError) and e.retry_allowed:
                            logging.info("Добавляем кнопку 'Повторить'.")
                            markup = InlineKeyboardMarkup(
                                [
                                    [
                                        InlineKeyboardButton(
                                            "🔄 Повторить",
                                            callback_data=f"retry_{func.__name__}",
                                        )
                                    ]
                                ]
                            )
                        elif error_config.get("allow_retry", False):
                            logging.info("Кнопка 'Повторить' разрешена настройками.")
                            markup = InlineKeyboardMarkup(
                                [
                                    [
                                        InlineKeyboardButton(
                                            "🔄 Повторить",
                                            callback_data=f"retry_{func.__name__}",
                                        )
                                    ]
                                ]
                            )

                        await message.reply_text(
                            user_message, parse_mode="HTML", reply_markup=markup
                        )
                    else:
                        logging.warning(
                            "Не удалось отправить сообщение: отсутствует message в update."
                        )

                    logging.info(
                        f"Завершение обработки ошибки в функции {func.__name__}."
                    )
                    break

        return wrapper


class MetricProcessor:
    """Класс для обработки метрик и сложных данных."""

    def __init__(self, logger):
        self.logger = logger

    def process_complex_data(
        self, data: pd.Series, metric_config: Dict[str, Any]
    ) -> pd.Series:
        """
        Обработка сложных данных (списков, словарей, вложенных структур).

        Args:
            data: Серия со сложными данными
            metric_config: Конфигурация метрики

        Returns:
            pd.Series: Обработанная серия с числовыми значениями
        """
        try:
            self.logger.info("Начало обработки сложных данных.")
            self.logger.debug(f"Исходные данные: {data}")
            self.logger.debug(f"Конфигурация метрики: {metric_config}")

            if data.empty:
                self.logger.warning("Серия данных пуста. Возвращаем пустую серию.")
                return pd.Series(dtype=float)

            first_value = data.iloc[0]
            self.logger.debug(
                f"Первое значение в серии: {first_value} (тип: {type(first_value)})"
            )

            if isinstance(first_value, (list, tuple)):
                self.logger.info("Данные представлены в виде списка или кортежа.")
                result = self._process_list_data(data, metric_config)
            elif isinstance(first_value, dict):
                self.logger.info("Данные представлены в виде словаря.")
                result = self._process_dict_data(data, metric_config)
            elif isinstance(first_value, str):
                self.logger.info("Данные представлены в виде строки.")
                result = self._process_string_data(data, metric_config)
            else:
                self.logger.info("Данные представлены в виде чисел или других типов.")
                result = self._safe_convert_to_numeric(data)

            self.logger.debug(f"Результат обработки данных: {result}")
            return result

        except Exception as e:
            self.logger.error(f"Ошибка обработки сложных данных: {e}", exc_info=True)
            self.logger.debug(f"Состояние данных при ошибке: {data}")
            return pd.Series(0, index=data.index)

    def _process_list_data(
        self, data: pd.Series, metric_config: Dict[str, Any]
    ) -> pd.Series:
        """Обработка данных в виде списков."""
        try:
            self.logger.info("Начало обработки данных в виде списка.")
            self.logger.debug(f"Исходные данные: {data}")
            self.logger.debug(f"Конфигурация метрики: {metric_config}")

            # Преобразуем списки в DataFrame
            expanded = pd.DataFrame(data.tolist(), index=data.index)
            self.logger.debug(f"Преобразованный DataFrame из списка:\n{expanded}")

            # Определяем метод агрегации
            agg_method = metric_config.get("list_aggregation", "sum")
            self.logger.info(f"Метод агрегации для списка: {agg_method}")

            if agg_method == "mean":
                result = expanded.mean(axis=1)
            elif agg_method == "max":
                result = expanded.max(axis=1)
            elif agg_method == "min":
                result = expanded.min(axis=1)
            elif agg_method == "first":
                result = expanded.iloc[:, 0]
            elif agg_method == "last":
                result = expanded.iloc[:, -1]
            else:  # sum по умолчанию
                result = expanded.sum(axis=1)

            self.logger.debug(f"Результат обработки списка:\n{result}")
            return result

        except Exception as e:
            self.logger.error(
                f"Ошибка обработки данных в виде списка: {e}", exc_info=True
            )
            self.logger.debug(f"Состояние данных при ошибке:\n{data}")
            return pd.Series(0, index=data.index)

    def _process_dict_data(
        self, data: pd.Series, metric_config: Dict[str, Any]
    ) -> pd.Series:
        """Обработка данных в виде словарей."""
        try:
            self.logger.info("Начало обработки данных в виде словаря.")
            self.logger.debug(f"Исходные данные: {data}")
            self.logger.debug(f"Конфигурация метрики: {metric_config}")

            # Получаем ключи для извлечения значений
            keys = metric_config.get("dict_keys", [])
            if not keys:
                self.logger.info(
                    "Ключи для извлечения не указаны. Автоматическое определение ключей."
                )
                first_dict = data.iloc[0]
                keys = [k for k, v in first_dict.items() if isinstance(v, (int, float))]
                self.logger.debug(f"Определенные ключи для словаря: {keys}")

            # Извлекаем значения по ключам
            values = []
            for d in data:
                row_values = [float(d.get(k, 0)) for k in keys]
                values.append(row_values)

            self.logger.debug(f"Извлеченные значения:\n{values}")

            # Преобразуем в DataFrame и агрегируем
            expanded = pd.DataFrame(values, index=data.index)
            self.logger.debug(f"Преобразованный DataFrame из словаря:\n{expanded}")

            agg_method = metric_config.get("dict_aggregation", "sum")
            self.logger.info(f"Метод агрегации для словаря: {agg_method}")

            if agg_method == "mean":
                result = expanded.mean(axis=1)
            elif agg_method == "max":
                result = expanded.max(axis=1)
            elif agg_method == "min":
                result = expanded.min(axis=1)
            else:  # sum по умолчанию
                result = expanded.sum(axis=1)

            self.logger.debug(f"Результат обработки словаря:\n{result}")
            return result

        except Exception as e:
            self.logger.error(
                f"Ошибка обработки данных в виде словаря: {e}", exc_info=True
            )
            self.logger.debug(f"Состояние данных при ошибке:\n{data}")
            return pd.Series(0, index=data.index)

    def _process_string_data(
        self, data: pd.Series, metric_config: Dict[str, Any]
    ) -> pd.Series:
        """Обработка строковых данных с максимальным логированием."""
        try:
            self.logger.info("Начало обработки строковых данных.")
            self.logger.debug(f"Исходные данные: {data}")
            self.logger.debug(f"Конфигурация метрики: {metric_config}")

            # Проверяем на JSON
            if self._is_json_string(data.iloc[0]):
                self.logger.info("Данные определены как JSON строки.")
                parsed_data = data.apply(json.loads)
                self.logger.debug(f"Парсинг JSON данных завершен: {parsed_data}")
                return self.process_complex_data(parsed_data, metric_config)

            # Проверяем на числа в строках
            numeric_data = pd.to_numeric(data, errors="coerce")
            if not numeric_data.isna().all():
                self.logger.info("Данные содержат числовые значения в строках.")
                self.logger.debug(f"Распознанные числовые значения: {numeric_data}")
                return numeric_data.fillna(0)

            # Проверяем на списки/кортежи в строках
            if data.iloc[0].startswith(("[", "(")):
                self.logger.info("Данные определены как списки или кортежи в строках.")
                parsed_data = data.apply(eval)  # Безопасно только для списков/кортежей
                self.logger.debug(f"Распарсенные данные: {parsed_data}")
                return self._process_list_data(parsed_data, metric_config)

            # Если ничего не подошло, пробуем извлечь числа из строк
            self.logger.info("Попытка извлечения чисел из строк.")
            extracted_numbers = self._extract_numbers_from_strings(data)
            self.logger.debug(f"Извлеченные числа: {extracted_numbers}")
            return extracted_numbers

        except Exception as e:
            self.logger.error(f"Ошибка обработки строковых данных: {e}", exc_info=True)
            return pd.Series(0, index=data.index)

    def _is_json_string(self, s: str) -> bool:
        """Проверка, является ли строка JSON, с логированием."""
        try:
            json.loads(s)
            self.logger.debug(f"Строка определена как валидный JSON: {s}")
            return True
        except (json.JSONDecodeError, TypeError) as e:
            self.logger.debug(f"Строка не является валидным JSON: {s}. Ошибка: {e}")
            return False

    def _extract_numbers_from_strings(self, data: pd.Series) -> pd.Series:
        """Извлечение чисел из строк с логированием."""
        try:
            self.logger.info("Начало извлечения чисел из строк.")
            self.logger.debug(f"Исходные данные: {data}")

            # Используем регулярное выражение для поиска чисел
            pattern = r"[-+]?\d*\.?\d+"
            extracted = data.str.extract(pattern, expand=False)
            self.logger.debug(f"Извлеченные числа (сырые): {extracted}")

            numeric_data = pd.to_numeric(extracted, errors="coerce").fillna(0)
            self.logger.debug(f"Числовые данные после преобразования: {numeric_data}")
            return numeric_data

        except Exception as e:
            self.logger.error(f"Ошибка извлечения чисел из строк: {e}", exc_info=True)
            return pd.Series(0, index=data.index)

    def _safe_convert_to_numeric(
        self, data: pd.Series, default_value: float = 0.0
    ) -> pd.Series:
        """Безопасное преобразование в числовой формат с логированием."""
        try:
            self.logger.info(
                "Начало безопасного преобразования данных в числовой формат."
            )
            self.logger.debug(f"Исходные данные: {data}")
            numeric_data = pd.to_numeric(data, errors="coerce").fillna(default_value)
            self.logger.debug(f"Результат преобразования: {numeric_data}")
            return numeric_data
        except Exception as e:
            self.logger.error(
                f"Ошибка безопасного преобразования данных: {e}", exc_info=True
            )
            return pd.Series(default_value, index=data.index)

    def normalize_data(
        self, data: pd.Series, metric_config: Dict[str, Any]
    ) -> pd.Series:
        """Нормализация данных с логированием."""
        try:
            self.logger.info("Начало нормализации данных.")
            self.logger.debug(f"Исходные данные: {data}")
            self.logger.debug(f"Конфигурация метрики: {metric_config}")

            if data.empty:
                self.logger.warning("Серия данных пуста. Возвращаем пустую серию.")
                return data

            # Применяем масштабирование
            scale = metric_config.get("scale", 1.0)
            if scale != 1.0:
                self.logger.info(f"Применение масштабирования с коэффициентом: {scale}")
                data = data * scale

            # Применяем округление
            decimals = metric_config.get("decimals")
            if decimals is not None:
                self.logger.info(
                    f"Применение округления до {decimals} знаков после запятой."
                )
                data = data.round(decimals)

            # Применяем ограничения
            min_value = metric_config.get("min_value")
            max_value = metric_config.get("max_value")
            if min_value is not None:
                self.logger.info(f"Применение минимального значения: {min_value}")
                data = data.clip(lower=min_value)
            if max_value is not None:
                self.logger.info(f"Применение максимального значения: {max_value}")
                data = data.clip(upper=max_value)

            self.logger.debug(f"Результат нормализации: {data}")
            return data

        except Exception as e:
            self.logger.error(f"Ошибка нормализации данных: {e}", exc_info=True)
            return data


class CallbackDispatcher:
    def __init__(self, bot_instance):
        logger.debug(f"Доступные атрибуты CallbackDispatcher: {dir(self)}")
        self.logger = logging.getLogger(self.__class__.__name__)
        self.bot = bot_instance
        self.permissions_manager = (
            bot_instance.permissions_manager
        )  # Передача менеджера прав
        self.operator_data = OperatorData(
            bot_instance.db_manager
        )  # Используем db_manager из bot_instance
        self._handlers = {}
        logger.debug(
            f"Доступные атрибуты Bot: {dir(self.bot)}"
        )  # Переместили после инициализации
        self._register_handlers()
        self.logger.debug(f"Инициализация CallbackDispatcher: {dir(self)}")

    async def handle_weekly_report(self, operator_id: int) -> None:
        """
        Handle the weekly report for the given operator.
        """
        self.logger.info(f"Handling weekly report for operator {operator_id}.")
        # Add your logic for handling the weekly report here
        await asyncio.sleep(1)  # Simulate some async operation
        self.logger.info(f"Weekly report for operator {operator_id} handled.")

    async def handle_monthly_report(
        self, update: Update, context: CallbackContext, operator_id: int
    ) -> None:
        """Handle the monthly report for the given operator."""
        self.logger.info(f"Handling monthly report for operator {operator_id}.")
        # Add your logic to handle the monthly report here
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"Monthly report for operator {operator_id}",
        )

    async def handle_yearly_report(self, operator_id: int):
        # Implementation of the handle_yearly_report method
        pass

    """Диспетчер для обработки callback-запросов."""

    def _register_handlers(self):
        """Регистрация обработчиков callback."""
        self._handlers = {
            "period": self._handle_period_callback,
            "operator": self._handle_operator_callback,
            "retry": self._handle_retry_callback,
            "metric": self._handle_metric_callback,
            "filter": self._handle_filter_callback,
            "page": self._handle_page_callback,
            "graph": self._handle_graph_callback,
            "operator_menu": self._handle_operator_menu_callback,
            "menu": self._handle_operator_menu_callback,  # Добавляем обработчик для `menu`
        }

    async def dispatch(self, update: Update, context: CallbackContext) -> None:
        """
        Диспетчеризация callback-запросов с максимальным логированием.

        Args:
            update: Объект обновления
            context: Контекст callback
        """
        self.logger.info("Начало обработки callback-запроса.")

        try:
            if update is None:
                self.logger.error("Объект update отсутствует (None).")
                return

            query = update.callback_query
            if query is None:
                self.logger.warning("Отсутствует callback_query в update.")
                return

            if not query.data:
                self.logger.warning("Отсутствуют данные callback_data в запросе.")
                await query.answer("Некорректный запрос: отсутствуют данные.")
                return

            data = query.data
            self.logger.info(f"Получены данные callback: {data}")

            # Разбор callback_data
            try:
                callback_type, *params = data.split("_")
                self.logger.info(f"Определён тип callback: {callback_type}")
                self.logger.debug(f"Параметры после split: {params}")
            except ValueError as parse_error:
                self.logger.error(
                    f"Ошибка разбора callback данных: {data}, {parse_error}"
                )
                await query.answer("Некорректный формат данных.")
                return

            # Поиск соответствующего обработчика
            handler = self._handlers.get(callback_type)
            if handler:
                self.logger.info(
                    f"Обработчик для callback типа '{callback_type}' найден: {handler.__name__}"
                )
                await handler(update, context, params)
            else:
                self.logger.warning(f"Неизвестный тип callback: {callback_type}")
                await query.answer("Неизвестный тип запроса.")

        except Exception as e:
            self.logger.error(
                f"Ошибка при обработке callback-запроса: {e}", exc_info=True
            )
            # Пытаемся ответить пользователю об ошибке
            try:
                if update and update.callback_query:
                    await update.callback_query.answer(
                        "Произошла ошибка при обработке запроса."
                    )
            except Exception as answer_error:
                self.logger.error(
                    f"Ошибка при отправке ответа об ошибке: {answer_error}",
                    exc_info=True,
                )
        finally:
            self.logger.info("Завершение обработки callback-запроса.")

    def get_period_keyboard(self, operator_id: int) -> InlineKeyboardMarkup:
        keyboard = [
            [InlineKeyboardButton("День", callback_data=f"period_daily_{operator_id}")],
            [
                InlineKeyboardButton(
                    "Неделя", callback_data=f"period_weekly_{operator_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    "Месяц", callback_data=f"period_monthly_{operator_id}"
                )
            ],
            [InlineKeyboardButton("Год", callback_data=f"period_yearly_{operator_id}")],
            [
                InlineKeyboardButton(
                    "Кастомный период", callback_data=f"period_custom_{operator_id}"
                )
            ],
            [InlineKeyboardButton("Назад", callback_data=f"operator_{operator_id}")],
        ]
        return InlineKeyboardMarkup(keyboard)

    def get_initial_operator_menu(self, operator_id: int) -> InlineKeyboardMarkup:
        """
        Генерирует первичную клавиатуру для оператора с одной кнопкой: "Посмотреть прогресс".
        """
        keyboard = [
            [
                InlineKeyboardButton(
                    "Посмотреть прогресс", callback_data=f"menu_progress_{operator_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    "Назад к списку операторов",
                    callback_data=f"menu_back_{operator_id}",
                )
            ],
        ]
        return InlineKeyboardMarkup(keyboard)

    def get_period_selection_menu(self, operator_id: int) -> InlineKeyboardMarkup:
        """
        Генерирует клавиатуру для выбора периода.
        """
        keyboard = [
            [InlineKeyboardButton("День", callback_data=f"period_daily_{operator_id}")],
            [
                InlineKeyboardButton(
                    "Неделя", callback_data=f"period_weekly_{operator_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    "Месяц", callback_data=f"period_monthly_{operator_id}"
                )
            ],
            [InlineKeyboardButton("Год", callback_data=f"period_yearly_{operator_id}")],
            [
                InlineKeyboardButton(
                    "Назад", callback_data=f"menu_back_progress_{operator_id}"
                )
            ],
        ]
        return InlineKeyboardMarkup(keyboard)

    async def _handle_operator_menu_callback(
        self, update: Update, context: CallbackContext, params: List[str]
    ) -> None:
        """
        Универсальный обработчик меню оператора.
        """
        self.logger.info("Начало обработки меню оператора.")
        query = update.callback_query

        try:
            action = params[0]  # Действие: 'progress', 'period', 'back'
            operator_id = (
                int(params[1]) if len(params) > 1 and params[1].isdigit() else None
            )

            if action == "progress":
                # Показать меню выбора периода
                self.logger.info(f"Обработка 'progress' для оператора {operator_id}.")
                keyboard = self.get_period_selection_menu(operator_id)
                await query.edit_message_text(
                    text=f"Выберите период для оператора {operator_id}:",
                    reply_markup=keyboard,
                )
            elif action.startswith("period"):
                # Обработка выбора периода
                period = action.split("_")[1]  # Например, 'daily', 'weekly', и т.д.
                self.logger.info(
                    f"Выбран период '{period}' для оператора {operator_id}."
                )

                # Генерация графика
                progress_data = await self.bot.progress_data.get_operator_progress(
                    operator_id, period
                )
                buf, trend_message = await self.bot.generate_operator_graph(
                    progress_data, operator_id, period
                )

                # Отправляем график
                await query.message.reply_photo(
                    photo=buf, caption=trend_message, parse_mode=ParseMode.HTML
                )
                self.logger.info("График успешно отправлен пользователю.")

                # Возвращаем клавиатуру в начальное состояние
                await query.edit_message_text(
                    text=f"Прогресс оператора {operator_id} за период {period}:",
                    reply_markup=self.get_initial_operator_menu(operator_id),
                )
            elif action == "back":
                # Возврат к списку операторов
                self.logger.info("Возврат к списку операторов.")
                await self.show_operator_list(query)
            else:
                self.logger.warning(f"Неизвестное действие: {action}")
                await query.answer("Некорректное действие.")
        except Exception as e:
            self.logger.error(
                f"Ошибка в _handle_operator_menu_callback: {e}", exc_info=True
            )
            await query.answer("Произошла ошибка при обработке команды.")
        finally:
            self.logger.info("Завершение обработки меню оператора.")

    def get_period_keyboard(self, operator_id: int) -> InlineKeyboardMarkup:
        """
        Генерирует клавиатуру с вариантами периода: День, Неделя, Месяц, Год, Кастомный период.
        """
        keyboard = [
            [
                InlineKeyboardButton(
                    "День", callback_data=f"menu_period_daily_{operator_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    "Неделя", callback_data=f"menu_period_weekly_{operator_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    "Месяц", callback_data=f"menu_period_monthly_{operator_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    "Год", callback_data=f"menu_period_yearly_{operator_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    "Кастомный период",
                    callback_data=f"menu_period_custom_{operator_id}",
                )
            ],
            [InlineKeyboardButton("Назад", callback_data=f"menu_back_{operator_id}")],
        ]
        return InlineKeyboardMarkup(keyboard)

    def _parse_date_range(self, date_range: str) -> Tuple[date, date]:
        """
        Парсинг строки диапазона дат в объекты date.

        Args:
            date_range (str): Строка диапазона дат в формате "YYYY-MM-DD - YYYY-MM-DD".

        Returns:
            Tuple[date, date]: Кортеж из двух объектов date (start_date, end_date).

        Raises:
            ValueError: Если формат строки или значения дат некорректны.
        """
        self.logger.debug(f"Парсинг диапазона дат: '{date_range}'")
        try:
            # Удаляем лишние пробелы и разделяем строки
            if " - " not in date_range:
                self.logger.error(
                    f"Некорректный формат диапазона дат: '{date_range}'. Ожидается 'YYYY-MM-DD - YYYY-MM-DD'."
                )
                raise ValueError(
                    "Некорректный формат диапазона дат. Используйте формат 'YYYY-MM-DD - YYYY-MM-DD'."
                )

            start_str, end_str = map(str.strip, date_range.split("-"))

            # Преобразуем строки в объекты date
            start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
            end_date = datetime.strptime(end_str, "%Y-%m-%d").date()

            # Проверяем порядок дат
            if start_date > end_date:
                self.logger.error(
                    f"Дата начала позже даты окончания: {start_date} > {end_date}"
                )
                raise ValueError("Дата начала не может быть позже даты окончания.")

            # Успешный парсинг
            self.logger.debug(
                f"Успешно распарсены даты: start_date={start_date}, end_date={end_date}"
            )
            return start_date, end_date

        except ValueError as e:
            # Логируем подробности ошибки
            self.logger.error(
                f"Ошибка парсинга диапазона дат '{date_range}': {e}", exc_info=True
            )
            raise ValueError(
                "Некорректный формат диапазона дат. Используйте формат 'YYYY-MM-DD - YYYY-MM-DD'."
            )
        except Exception as e:
            # Ловим любые другие неожиданные исключения
            self.logger.error(
                f"Неизвестная ошибка при обработке диапазона дат '{date_range}': {e}",
                exc_info=True,
            )
            raise ValueError("Произошла ошибка при обработке диапазона дат.")

    async def _handle_operator_progress_callback(
        self, update: Update, context: CallbackContext, params: List[str]
    ) -> None:
        """
        Обработка запроса на просмотр прогресса оператора с максимальным логированием.

        Args:
            update: Объект обновления Telegram.
            context: Контекст вызова.
            params: Параметры из callback_data.
        """
        logging.info("Начало обработки запроса прогресса оператора.")

        # Логируем исходные данные
        try:
            logging.debug(f"CallbackQuery данные: {update.callback_query}")
            logging.debug(f"Параметры: {params}")
        except Exception as log_error:
            logging.error(
                f"Ошибка логирования исходных данных: {log_error}", exc_info=True
            )

        query = update.callback_query
        operator_id = None

        try:
            # Проверяем наличие и корректность ID оператора
            if params:
                try:
                    operator_id = int(params[0])
                    logging.info(f"Получен operator_id: {operator_id}")
                except ValueError:
                    logging.warning(f"Некорректный формат operator_id: {params[0]}")
            if not operator_id:
                logging.warning("Отсутствует или некорректный operator_id.")
                await query.answer("Некорректный запрос: не указан оператор")
                return

            # Получаем данные оператора по ID
            logging.info(f"Запрос данных оператора с ID: {operator_id}")
            operator = await self.bot.operator_data.get_operator_by_id(operator_id)
            if not operator:
                logging.warning(f"Оператор с ID {operator_id} не найден.")
                await query.answer("Оператор не найден")
                return

            logging.debug(f"Данные оператора: {operator}")

            # Устанавливаем дефолтный период
            default_period = "weekly"
            logging.info(f"Устанавливается дефолтный период: {default_period}")
            context.user_data["selected_period"] = default_period

            # Получаем данные прогресса за указанный период
            logging.info(
                f"Получение данных прогресса для оператора {operator_id} за период {default_period}."
            )
            progress_data = await self.bot.progress_data.get_operator_progress(
                operator_id, default_period
            )

            if not progress_data:
                logging.warning(
                    f"Нет данных прогресса для оператора {operator['name']} за период {default_period}."
                )
                await query.edit_message_text(
                    f"Нет данных для оператора {operator['name']} за период {default_period}."
                )
                return

            logging.debug(f"Данные прогресса оператора: {progress_data}")

            # Генерируем график прогресса
            logging.info(
                f"Генерация графика прогресса для оператора {operator['name']}."
            )
            buf, trend_message = await self.bot.generate_operator_progress(
                progress_data, operator["name"], default_period
            )
            logging.debug(f"Сгенерированное сообщение трендов: {trend_message}")

            # Отправляем график и тренды в чат
            logging.info(
                f"Отправка графика прогресса оператора {operator['name']} в чат."
            )
            await query.message.reply_photo(
                buf, caption=trend_message, parse_mode=ParseMode.HTML
            )

            # Обновляем сообщение с выбором других периодов
            logging.info(
                f"Обновление сообщения с выбором других периодов для оператора {operator['name']}."
            )
            keyboard = self.get_period_keyboard(operator_id)
            logging.debug(f"Сгенерированная клавиатура: {keyboard}")
            await query.edit_message_text(
                f"Прогресс оператора {operator['name']} за период {default_period}:",
                reply_markup=keyboard,
            )

        except Exception as e:
            # Логируем исключение
            logging.error(
                f"Ошибка при обработке прогресса оператора {operator_id}: {e}",
                exc_info=True,
            )
            await query.answer("Произошла ошибка при обработке запроса.")
        finally:
            logging.info("Завершение обработки запроса прогресса оператора.")

    async def _handle_period_select_callback(
        self, update: Update, context: CallbackContext, params: List[str]
    ) -> None:
        """
        Обработка выбора периода для оператора.
        """
        logging.info("Начало обработки выбора периода для оператора.")

        # Логируем исходные данные
        try:
            logging.debug(f"CallbackQuery данные: {update.callback_query}")
            logging.debug(f"Параметры: {params}")
        except Exception as log_error:
            logging.error(
                f"Ошибка логирования исходных данных: {log_error}", exc_info=True
            )

        query = update.callback_query
        operator_id = None

        try:
            # Проверяем наличие параметров
            if params:
                try:
                    operator_id = int(params[0])
                    logging.info(f"Получен operator_id: {operator_id}")
                except ValueError:
                    logging.warning(f"Некорректный формат operator_id: {params[0]}")
            if not operator_id:
                logging.warning("Отсутствует или некорректный operator_id.")
                await query.answer("Некорректный запрос: не указан оператор")
                return

            # Генерация клавиатуры выбора периода
            logging.info(
                f"Генерация клавиатуры выбора периода для оператора {operator_id}."
            )
            keyboard = self.get_period_keyboard(operator_id)
            logging.debug(f"Сгенерированная клавиатура: {keyboard}")

            # Отправляем сообщение с клавиатурой
            await query.edit_message_text(
                f"Выберите период для оператора с ID {operator_id}:",
                reply_markup=keyboard,
            )
            logging.info(
                f"Сообщение с клавиатурой выбора периода отправлено для оператора {operator_id}."
            )

        except Exception as e:
            # Логируем исключение
            logging.error(
                f"Ошибка при обработке выбора периода для оператора {operator_id}: {e}",
                exc_info=True,
            )
            await query.answer("Произошла ошибка при выборе периода.")
        finally:
            logging.info("Завершение обработки выбора периода для оператора.")

    async def _handle_period_callback(
        self, update: Update, context: CallbackContext, params: List[str]
    ) -> None:
        """
        Обработка выбора периода для оператора с проверкой прав и генерацией графиков.
        """
        logger.info("Начало обработки выбора периода.")
        query = update.callback_query

        try:
            logger.debug(f"CallbackQuery данные: {query}")
            logger.debug(f"Параметры: {params}")

            # Проверяем корректность параметров
            if len(params) != 2:
                logger.error("Некорректное количество параметров в callback_data.")
                await query.answer("Некорректный запрос. Попробуйте снова.")
                return

            period, operator_id_str = params
            logger.info(f"Извлечён период: {period}, operator_id: {operator_id_str}")

            # Проверяем корректность operator_id
            try:
                operator_id = int(operator_id_str)
            except ValueError:
                logger.error(f"Некорректный формат operator_id: {operator_id_str}")
                await query.answer("Ошибка: некорректный ID оператора.")
                return

            # Проверяем, есть ли can_view_periods в permissions_manager
            if not hasattr(self.bot.permissions_manager, "can_view_periods"):
                logger.error(
                    "Метод 'can_view_periods' отсутствует в PermissionsManager."
                )
                await query.answer("Ошибка: невозможно проверить права доступа.")
                return

            # Проверяем права доступа пользователя
            logger.info(
                f"Проверка прав доступа пользователя {query.from_user.id} для оператора {operator_id}."
            )
            has_access = await self.permissions_manager.can_view_periods(
                query.from_user.id
            )
            if not has_access:
                logger.warning(
                    f"Пользователь {query.from_user.id} не имеет прав для выбора периода."
                )
                await query.answer("У вас нет прав для выбора этого периода.")
                return

            # Сохраняем выбранный период в контексте
            context.user_data["selected_period"] = period
            logger.info(f"Период '{period}' сохранён в контексте пользователя.")

            # **Сразу** отвечаем на колбэк, чтобы Telegram не «протух».
            # Можно без текста, но для UX даём минимальное сообщение:
            await query.answer("Строим графики, пожалуйста подождите...")

            # Получаем данные прогресса
            logger.info(
                f"Получение данных прогресса для оператора {operator_id} за период {period}."
            )
            progress_data = await self.bot.progress_data.get_operator_progress(
                operator_id, period
            )
            if not progress_data:
                logger.warning(
                    f"Нет данных прогресса для оператора {operator_id} за период {period}."
                )
                await query.edit_message_text(
                    f"Нет данных для оператора {operator_id} за период {period}."
                )
                return

            logger.debug(f"Полученные данные прогресса: {progress_data}")

            # Генерация нескольких графиков (метод возвращает список кортежей):
            #   [ (group_name, buf, trend_msg, commentary), ... ]
            logger.info(
                f"Генерация графиков (generate_operator_progress) для оператора {operator_id} и периода {period}."
            )
            results = await self.bot.generate_operator_progress(
                progress_data, operator_id, period
            )

            logger.info(
                f"Отправка графиков и комментариев для оператора {operator_id}."
            )
            max_caption_length = 1024  # Лимит подписи в Telegram (примерно)

            for group_name, buf, trend_msg, commentary in results:
                # Склеиваем подпись
                final_caption = (trend_msg + "\n\n" + commentary).strip()

                if len(final_caption) > max_caption_length:
                    # Обрезаем подпись для фото
                    short_caption = final_caption[: (max_caption_length - 3)] + "..."
                    # 1) Отправляем фото с обрезанной подписью
                    await query.message.reply_photo(
                        photo=buf, caption=short_caption, parse_mode=ParseMode.HTML
                    )
                    # 2) А полный текст отдельным сообщением
                    await query.message.reply_text(
                        final_caption, parse_mode=ParseMode.HTML
                    )
                else:
                    # Если подпись вмещается — отправляем одним сообщением
                    await query.message.reply_photo(
                        photo=buf, caption=final_caption, parse_mode=ParseMode.HTML
                    )

            logger.info("Все графики успешно отправлены пользователю.")

        except ValueError as ve:
            logger.error(f"Ошибка формата данных callback: {ve}", exc_info=True)
            await query.answer("Некорректный формат данных. Попробуйте снова.")
        except Exception as e:
            logger.error(f"Ошибка при обработке выбора периода: {e}", exc_info=True)
            await query.answer("Произошла ошибка при обработке запроса.")
        finally:
            logger.info("Завершение обработки выбора периода.")

    async def _handle_operator_callback(
        self, update: Update, context: CallbackContext, params: List[str]
    ) -> None:
        """
        Обработка выбора оператора или специальных команд с максимальным логированием.
        """
        self.logger.info(f"Начало обработки callback оператора. Параметры: {params}")
        query = update.callback_query

        try:
            # Проверка наличия callback_query
            if not query:
                self.logger.error("CallbackQuery отсутствует в обновлении.")
                return

            # Проверяем наличие параметров
            if not params or not params[0]:
                self.logger.warning("Отсутствуют параметры callback_data.")
                await query.answer("Некорректный формат данных.")
                return

            # Первое значение — команда или имя оператора
            command = params[0].strip()
            self.logger.info(f"Распознанная команда или имя оператора: {command}")

            # Проверка на команды `menu`
            if command == "menu" and len(params) > 1:
                sub_command = params[1].strip().lower()
                operator_id = (
                    int(params[2]) if len(params) > 2 and params[2].isdigit() else None
                )

                if not operator_id:
                    self.logger.warning("Отсутствует или некорректный ID оператора.")
                    await query.answer("Некорректный формат данных.")
                    return

                # Обработка подкоманд `menu`
                if sub_command == "progress":
                    self.logger.info(
                        f"Обработка подкоманды 'progress' для оператора {operator_id}."
                    )
                    await self._handle_operator_progress_callback(
                        update, context, [operator_id]
                    )
                elif sub_command == "period":
                    self.logger.info(
                        f"Обработка подкоманды 'period' для оператора {operator_id}."
                    )
                    await self._handle_period_select_callback(
                        update, context, [operator_id]
                    )
                elif sub_command in ["daily", "weekly", "monthly", "yearly"]:
                    self.logger.info(
                        f"Обработка подкоманды периода '{sub_command}' для оператора {operator_id}."
                    )
                    await self._handle_period_select_callback(
                        update, context, [operator_id, sub_command]
                    )
                elif sub_command == "back":
                    self.logger.info("Возврат к списку операторов.")
                    await self.operator_progress_menu_handle(update, context)
                else:
                    self.logger.warning(f"Неизвестная подкоманда 'menu': {sub_command}")
                    await query.answer("Неизвестная команда.")
                return

            # Обработка имени оператора
            self.logger.info(f"Попытка найти оператора с именем: {command}")
            operator = await self.operator_data.get_operator_by_name(command)
            if not operator:
                self.logger.warning(f"Оператор с именем '{command}' не найден.")
                await query.answer("Оператор не найден.")
                return

            # Извлечение ID оператора
            operator_id = operator.get("user_id")
            if not operator_id:
                self.logger.warning(f"Не удалось извлечь ID для оператора '{command}'.")
                await query.answer("Ошибка: не удалось найти ID оператора.")
                return
            self.logger.debug(f"Извлечённый ID оператора: {operator_id}")

            # Проверка прав доступа
            self.logger.info(
                f"Проверка прав доступа для пользователя {query.from_user.id} и оператора {operator_id}."
            )
            has_access = await self.permissions_manager.can_view_operator(
                query.from_user.id, operator_id
            )
            if not has_access:
                self.logger.warning(
                    f"Пользователь {query.from_user.id} не имеет прав на доступ к оператору {operator_id}."
                )
                await query.answer("У вас нет прав для просмотра этого оператора.")
                return

            # Сохранение выбранного оператора в контексте
            context.user_data["selected_operator"] = operator_id
            self.logger.info(
                f"Оператор {operator_id} сохранён в контексте пользователя."
            )

            # Генерация клавиатуры для оператора
            self.logger.info(f"Генерация клавиатуры для оператора с ID: {operator_id}")
            keyboard = InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "Посмотреть прогресс",
                            callback_data=f"menu_progress_{operator_id}",
                        )
                    ],
                    # [InlineKeyboardButton("Выбрать период", callback_data=f"menu_period_{operator_id}")],
                    [
                        InlineKeyboardButton(
                            "Назад к списку операторов", callback_data="menu_back"
                        )
                    ],
                ]
            )
            self.logger.debug(f"Сгенерированная клавиатура: {keyboard}")

            # Обновление сообщения с клавиатурой
            try:
                await query.edit_message_text(
                    text=f"Выбран оператор: {operator['name']}", reply_markup=keyboard
                )
                self.logger.info(
                    f"Сообщение успешно обновлено для оператора: {operator['name']}"
                )
            except telegram.error.BadRequest as e:
                if "message is not modified" in str(e):
                    self.logger.info("Сообщение не требует обновления (не изменилось).")
                else:
                    self.logger.error(
                        f"Ошибка при обновлении сообщения: {e}", exc_info=True
                    )
                    await query.answer("Ошибка при обновлении сообщения.")
                return

        except Exception as e:
            self.logger.error(
                f"Ошибка при обработке callback оператора: {e}", exc_info=True
            )
            await query.answer("Произошла ошибка. Попробуйте позже.")
        finally:
            self.logger.info("Завершение обработки callback оператора.")

    async def _handle_retry_callback(
        self, update: Update, context: CallbackContext, params: List[str]
    ) -> None:
        """
        Обработка повторного запроса с максимальным логированием.
        """
        logging.info("Начало обработки повторного запроса.")

        query = update.callback_query
        function_name = params[0] if params else None
        logging.debug(f"Имя функции для повтора: {function_name}")

        try:
            # Получаем функцию для повтора
            retry_func = getattr(self.bot, function_name, None)
            if not retry_func:
                logging.warning(f"Функция '{function_name}' не найдена.")
                await query.answer("Функция не найдена")
                return

            # Выполняем повторный запрос
            logging.info(
                f"Выполнение повторного запроса через функцию '{function_name}'."
            )
            await query.answer("Повторяем запрос...")
            await retry_func(update, context)
            logging.info(
                f"Повторный запрос выполнен успешно через функцию '{function_name}'."
            )

        except Exception as e:
            # Логируем исключение
            logging.error(
                f"Ошибка при повторном запросе через функцию '{function_name}': {e}",
                exc_info=True,
            )
            await query.answer("Ошибка при повторном запросе")
        finally:
            logging.info("Завершение обработки повторного запроса.")

    async def _handle_metric_callback(
        self, update: Update, context: CallbackContext, params: List[str]
    ) -> None:
        """
        Обработка выбора метрики с максимальным логированием.
        """
        logging.info("Начало обработки выбора метрики.")

        query = update.callback_query
        metric_name = params[0] if params else None
        logging.debug(f"Выбранная метрика: {metric_name}")

        try:
            # Проверяем права доступа
            logging.info(
                f"Проверка прав доступа пользователя {query.from_user.id} к метрике '{metric_name}'."
            )
            has_access = await self.bot.permissions_manager.can_view_metric(
                query.from_user.id, metric_name
            )
            logging.debug(f"Результат проверки прав доступа: {has_access}")
            if not has_access:
                logging.warning(
                    f"Пользователь {query.from_user.id} не имеет прав для просмотра метрики '{metric_name}'."
                )
                await query.answer("У вас нет прав для просмотра этой метрики")
                return

            # Обновляем выбранную метрику в контексте
            context.user_data["selected_metric"] = metric_name
            logging.info(f"Метрика '{metric_name}' сохранена в контексте пользователя.")

            # Получаем клавиатуру для метрики
            logging.info(f"Генерация клавиатуры для метрики '{metric_name}'.")
            keyboard = self.bot.get_metric_keyboard(metric_name)
            logging.debug(f"Сгенерированная клавиатура: {keyboard}")

            # Обновляем сообщение с информацией о метрике
            await query.edit_message_text(
                f"Выбрана метрика: {metric_name}", reply_markup=keyboard
            )
            logging.info(f"Сообщение успешно обновлено для метрики '{metric_name}'.")

        except Exception as e:
            # Логируем исключение
            logging.error(
                f"Ошибка при обработке выбора метрики '{metric_name}': {e}",
                exc_info=True,
            )
            await query.answer("Ошибка при выборе метрики")
        finally:
            logging.info("Завершение обработки выбора метрики.")

    async def _handle_filter_callback(
        self, update: Update, context: CallbackContext, params: List[str]
    ) -> None:
        """
        Обработка фильтров с максимальным логированием.
        """
        logging.info("Начало обработки фильтра.")

        query = update.callback_query
        filter_type = params[0] if params else None
        filter_value = params[1] if len(params) > 1 else None

        logging.debug(f"Тип фильтра: {filter_type}, значение фильтра: {filter_value}")

        try:
            # Применяем фильтр
            logging.info(f"Применение фильтра: {filter_type}={filter_value}")
            context.user_data.setdefault("filters", {})
            context.user_data["filters"][filter_type] = filter_value
            logging.debug(
                f"Текущие фильтры пользователя: {context.user_data['filters']}"
            )

            # Обновляем сообщение
            logging.info("Генерация клавиатуры фильтров.")
            keyboard = self.bot.get_filter_keyboard(
                filter_type, context.user_data["filters"]
            )
            logging.debug(f"Сгенерированная клавиатура: {keyboard}")

            await query.edit_message_text("Выберите фильтры:", reply_markup=keyboard)
            logging.info("Сообщение успешно обновлено с новыми фильтрами.")

        except Exception as e:
            logging.error(f"Ошибка при обработке фильтра: {e}", exc_info=True)
            await query.answer("Ошибка при применении фильтра")
        finally:
            logging.info("Завершение обработки фильтра.")

    async def _handle_page_callback(
        self, update: Update, context: CallbackContext, params: List[str]
    ) -> None:
        """
        Обработка пагинации с максимальным логированием.
        """
        logging.info("Начало обработки пагинации.")

        query = update.callback_query
        page = int(params[0]) if params else 1
        logging.debug(f"Запрошенная страница: {page}")

        try:
            # Обновляем страницу в контексте
            logging.info(f"Установка текущей страницы в контекст: {page}")
            context.user_data["current_page"] = page

            # Получаем данные для страницы
            logging.info(f"Запрос данных для страницы {page}.")
            data = await self.bot.get_page_data(page, context.user_data)
            logging.debug(f"Полученные данные для страницы: {data}")

            # Обновляем сообщение
            logging.info("Генерация клавиатуры пагинации.")
            keyboard = self.bot.get_pagination_keyboard(page, data["total_pages"])
            logging.debug(f"Сгенерированная клавиатура пагинации: {keyboard}")

            await query.edit_message_text(
                data["text"], reply_markup=keyboard, parse_mode="HTML"
            )
            logging.info("Сообщение успешно обновлено для текущей страницы.")

        except Exception as e:
            logging.error(f"Ошибка при обработке пагинации: {e}", exc_info=True)
            await query.answer("Ошибка при переключении страницы")
        finally:
            logging.info("Завершение обработки пагинации.")

    async def _handle_graph_callback(
        self, update: Update, context: CallbackContext, params: List[str]
    ) -> None:
        """
        Обработка настроек графика с максимальным логированием.
        """
        logging.info("Начало обработки настроек графика.")

        query = update.callback_query
        graph_type = params[0] if params else None
        setting = params[1] if len(params) > 1 else None

        logging.debug(f"Тип графика: {graph_type}, настройка: {setting}")

        try:
            # Обновляем настройки графика
            logging.info(f"Применение настройки графика: {graph_type}={setting}")
            context.user_data.setdefault("graph_settings", {})
            context.user_data["graph_settings"][graph_type] = setting
            logging.debug(
                f"Текущие настройки графика пользователя: {context.user_data['graph_settings']}"
            )

            # Обновляем сообщение
            logging.info("Генерация клавиатуры настроек графика.")
            keyboard = self.bot.get_graph_settings_keyboard(
                context.user_data["graph_settings"]
            )
            logging.debug(f"Сгенерированная клавиатура настроек графика: {keyboard}")

            await query.edit_message_text("Настройки графика:", reply_markup=keyboard)
            logging.info("Сообщение успешно обновлено с настройками графика.")

        except Exception as e:
            logging.error(f"Ошибка при обработке настроек графика: {e}", exc_info=True)
            await query.answer("Ошибка при изменении настроек графика")
        finally:
            logging.info("Завершение обработки настроек графика.")


class BotError(Exception):
    """Базовый класс для ошибок бота."""

    def __init__(self, message: str, user_message: str = None):
        super().__init__(message)
        self.user_message = user_message or message


class AuthenticationError(BotError):
    """Ошибка аутентификации."""

    pass


class PermissionError(BotError):
    """Ошибка прав доступа."""

    pass


class ValidationError(BotError):
    """Ошибка валидации данных."""

    pass


class DataProcessingError(BotError):
    """Ошибка обработки данных."""

    pass


class VisualizationError(BotError):
    """Ошибка создания визуализации."""

    pass


class ExternalServiceError(BotError):
    """Ошибка внешнего сервиса."""

    pass


class DataProcessor:
    """Класс для обработки данных визуализации."""

    def __init__(self, logger):
        self.logger = logger

    @staticmethod
    def determine_resample_frequency(total_seconds: float, target_points: int) -> str:
        """
        Определяет оптимальную частоту ресемплинга.

        Args:
            total_seconds: Общее количество секунд
            target_points: Целевое количество точек

        Returns:
            str: Строка частоты для pandas resample
        """
        interval_seconds = max(int(total_seconds / target_points), 1)

        if interval_seconds < 60:
            return f"{interval_seconds}S"  # секунды
        elif interval_seconds < 3600:
            return f"{interval_seconds // 60}T"  # минуты
        elif interval_seconds < 86400:
            return f"{interval_seconds // 3600}H"  # часы
        else:
            return f"{interval_seconds // 86400}D"  # дни

    @staticmethod
    def get_aggregation_method(
        column_name: str, data_type: str, unique_ratio: float
    ) -> str:
        """
        Определяет оптимальный метод агрегации для колонки.

        Args:
            column_name: Имя колонки
            data_type: Тип данных
            unique_ratio: Отношение уникальных значений к общему количеству

        Returns:
            str: Метод агрегации
        """
        if not pd.api.types.is_numeric_dtype(data_type):
            return "last"

        column_lower = column_name.lower()
        if any(term in column_lower for term in ["count", "quantity", "total"]):
            return "sum"
        elif any(term in column_lower for term in ["rate", "ratio", "avg", "mean"]):
            return "mean"
        elif unique_ratio < 0.1:  # Если мало уникальных значений
            return "mode"
        else:
            return "mean"

    @staticmethod
    def safe_convert_to_numeric(
        series: pd.Series, default_value: float = 0.0
    ) -> pd.Series:
        """
        Безопасное преобразование серии в числовой формат.

        Args:
            series: Исходная серия
            default_value: Значение по умолчанию

        Returns:
            pd.Series: Преобразованная серия
        """
        try:
            return pd.to_numeric(series, errors="coerce").fillna(default_value)
        except Exception:
            return pd.Series([default_value] * len(series), index=series.index)

    def process_complex_data(
        self, data: pd.Series, aggregation: str = "sum"
    ) -> pd.Series:
        """
        Обработка сложных данных (списков, словарей, вложенных структур).

        Args:
            data: Серия со сложными данными
            aggregation: Метод агрегации

        Returns:
            pd.Series: Обработанная серия
        """
        try:
            if isinstance(data.iloc[0], dict):
                expanded = pd.DataFrame(data.tolist(), index=data.index)
            else:
                expanded = pd.DataFrame(data.tolist(), index=data.index)

            if aggregation == "mean":
                return expanded.mean(axis=1)
            return expanded.sum(axis=1)
        except Exception as e:
            self.logger.error(f"Ошибка обработки сложных данных: {e}")
            return pd.Series(0, index=data.index)


class MetricConfig(TypedDict):
    name: str
    label: str
    color: str
    line_style: Optional[str]
    marker: Optional[str]
    aggregation: Optional[Literal["sum", "mean", "max", "min"]]


class VisualizationConfig(TypedDict):
    figure: Dict[str, Any]
    grid: bool
    marker_size: int
    metrics: List[MetricConfig]
    x_label: str
    y_label: str
    title: str


class TrendData(TypedDict):
    metric: str
    current: float
    previous: float
    change: float
    trend: Literal["up", "down", "stable"]


# Определяем типы для обработки данных
DataFrameOrSeries = Union[pd.DataFrame, pd.Series]
MetricValue = Union[float, int, list, dict, str]
AggregationMethod = Literal["sum", "mean", "max", "min", "first", "last"]


class DataProcessor:
    def __init__(self, logger):
        self.logger = logger

    """Класс для обработки данных."""

    @staticmethod
    def determine_resample_frequency(total_seconds: float, target_points: int) -> str:
        """
        Определяет частоту ресемплинга на основе общего времени и целевых точек.

        Args:
            total_seconds: Общее количество секунд
            target_points: Желаемое количество точек

        Returns:
            str: Строка частоты ресемплинга (например, '1H', '1D')
        """
        seconds_per_point = total_seconds / target_points

        if seconds_per_point < 60:
            return f"{int(seconds_per_point)}S"
        elif seconds_per_point < 3600:
            return f"{int(seconds_per_point / 60)}T"
        elif seconds_per_point < 86400:
            return f"{int(seconds_per_point / 3600)}H"
        else:
            return f"{int(seconds_per_point / 86400)}D"

    @staticmethod
    def get_aggregation_method(
        column: str, dtype: np.dtype, unique_ratio: float
    ) -> AggregationMethod:
        """
        Определяет метод агрегации на основе типа данных и уникальности значений.

        Args:
            column: Имя колонки
            dtype: Тип данных колонки
            unique_ratio: Отношение уникальных значений к общему количеству

        Returns:
            AggregationMethod: Метод агрегации
        """
        if dtype in (np.float64, np.int64):
            if unique_ratio > 0.8:
                return "mean"
            else:
                return "sum"
        elif dtype == np.bool_:
            return "sum"
        else:
            return "first"


class TelegramBot:
    MAX_RECORDS_FOR_VISUALIZATION = 1000  # Define the attribute with a default value

    def __init__(
        self,
        token: str,
        api_key: str = None,
        model: str = "gpt-4o-mini",
        max_concurrent_tasks: int = 5,
        max_visualization_tasks: int = 3,
    ):
        # Настройка OpenAI API ключа
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            logger.error(
                "OpenAI API ключ не найден. Пожалуйста, установите переменную окружения OPENAI_API_KEY."
            )
            raise EnvironmentError("OpenAI API ключ не найден.")

        self.token = token
        self.db_manager = DatabaseManager()
        self.auth_manager = AuthManager(self.db_manager)
        self.scheduler = AsyncIOScheduler()
        self.operator_data = OperatorData(self.db_manager)
        self.permissions_manager = PermissionsManager(self.db_manager)
        self.callback_dispatcher = CallbackDispatcher(self)
        self.progress_data = ProgressData(self.db_manager)
        self.visualizer = MetricsVisualizer(
            output_dir="output_dir_path", global_config={"dpi": 100, "figsize": (12, 6)}
        )
        self.client = AsyncOpenAI(api_key=self.api_key)
        self.model = model
        self.logger = logging.getLogger(__name__)
        # Сначала создаём приложение
        self.application = (
            ApplicationBuilder()
            .token(token)
            .request(httpx_request)  # Передаём настроенный HTTPXRequest
            .rate_limiter(AIORateLimiter())
            .build()
        )
        # Добавляем обработчики
        self.application.add_handler(
            CallbackQueryHandler(self.callback_dispatcher.dispatch)
        )
        # Инициализация PermissionsManager
        self.report_generator = OpenAIReportGenerator(
            self.db_manager, model="gpt-4o-mini"
        )
        self.temp_dir = "temp_plots"
        os.makedirs(self.temp_dir, exist_ok=True)

        self.global_config = GlobalConfig(
            style="seaborn",
            palette="husl",
            figsize=(12, 8),
            dpi=100,
            show_trend=True,
            show_confidence_interval=True,
            show_grid=True,
            show_legend=True,
            value_labels=True,
        )

        self.metrics_visualizer = MetricsVisualizer(
            output_dir=self.temp_dir,
            global_config=self.global_config,
            max_parallel_plots=max_visualization_tasks,
        )

        self.temp_file_manager = self.TempFileManager(self.temp_dir)

        self.scheduler.add_job(
            self.temp_file_manager.cleanup_old_files,
            "interval",
            minutes=30,
            kwargs={"max_age": 3600},
        )

        self.TEMP_FILES_TTL = 3600
        self.CLEANUP_INTERVAL = 1800
        self.temp_files_lock = Lock()

        self.MAX_RECORDS_FOR_VISUALIZATION = 1000
        self.PERIOD_CONFIG = {
            "daily": {"days": 1, "label": "День", "emoji": "📅"},
            "weekly": {"days": 7, "label": "Неделя", "emoji": "📅"},
            "monthly": {"days": 30, "label": "Месяц", "emoji": "📅"},
            "yearly": {"days": 365, "label": "Год", "emoji": "📅"},
        }

        self.CALLBACK_TYPES = {
            "operator_progress": "op_prog",
            "operator_page": "op_page",
            "period_select": "period",
            "all_operators": "all_op",
        }

        setup_auth_handlers(self.application, self.db_manager)

        if not self.scheduler.running:
            self.scheduler.start()

        self.visualization_semaphore = asyncio.Semaphore(max_visualization_tasks)
        self.task_semaphore = asyncio.Semaphore(max_concurrent_tasks)

        self.error_handler = ErrorHandler(self)
        self.data_processor = DataProcessor(logger)
        self.metric_processor = MetricProcessor(logger)

    PLOT_CONFIGS = {
        "operator_progress": {
            "title_template": "Прогресс оператора {operator_name} за {period}",
            # Вместо одного "metrics": [...], указываем "groups", внутри — списки метрик
            "groups": {
                "quality": [
                    {
                        "name": "avg_call_rating",
                        "label": "Средний рейтинг звонков",
                        "color": "#2ecc71",
                        "line_style": "-",
                        "marker": "o",
                    },
                    {
                        "name": "avg_lead_call_rating",
                        "label": "Средний рейтинг лид-звонков",
                        "color": "#8e44ad",
                        "line_style": "--",
                        "marker": "s",
                    },
                    {
                        "name": "avg_cancel_score",
                        "label": "Средний рейтинг отмен",
                        "color": "#d35400",
                        "line_style": ":",
                        "marker": "D",
                    },
                ],
                "conversion": [
                    {
                        "name": "conversion_rate",
                        "label": "Конверсия, %",
                        "color": "#1abc9c",
                        "line_style": "-",
                        "marker": "*",
                    },
                    {
                        "name": "booked_services",
                        "label": "Забронированные услуги",
                        "color": "#f1c40f",
                        "line_style": ":",
                        "marker": "D",
                    },
                    {
                        "name": "total_calls",
                        "label": "Всего звонков",
                        "color": "#3498db",
                        "line_style": "--",
                        "marker": "s",
                    },
                ],
                "call_handling": [
                    {
                        "name": "accepted_calls",
                        "label": "Принятые звонки",
                        "color": "#9b59b6",
                        "line_style": "-.",
                        "marker": "^",
                    },
                    {
                        "name": "missed_calls",
                        "label": "Пропущенные звонки",
                        "color": "#e67e22",
                        "line_style": "-.",
                        "marker": "^",
                    },
                    {
                        "name": "complaint_calls",
                        "label": "Жалобы",
                        "color": "#e74c3c",
                        "line_style": "--",
                        "marker": "v",
                    },
                ],
                "time": [
                    {
                        "name": "avg_conversation_time",
                        "label": "Среднее время разговора",
                        "color": "#2ecc71",
                        "line_style": "-",
                        "marker": "o",
                    },
                    {
                        "name": "avg_navigation_time",
                        "label": "Среднее время навигации",
                        "color": "#2980b9",
                        "line_style": "--",
                        "marker": "s",
                    },
                    {
                        "name": "avg_service_time",
                        "label": "Среднее время обслуживания",
                        "color": "#c0392b",
                        "line_style": ":",
                        "marker": "D",
                    },
                ],
                "summary": [
                    {
                        "name": "avg_missed_rate",
                        "label": "Средний % пропущенных",
                        "color": "#b8e994",
                        "line_style": "-",
                        "marker": "o",
                    },
                    {
                        "name": "total_cancellations",
                        "label": "Всего отмен",
                        "color": "#d35400",
                        "line_style": "--",
                        "marker": "s",
                    },
                    {
                        "name": "user_id",
                        "label": "ID оператора",
                        "color": "#95a5a6",
                        "line_style": ":",
                        "marker": "D",
                    },
                ],
            },
            "xlabel": "Дата",
            "ylabel": "Значение",
            "grid": True,
            "legend_position": "upper right",
        },
        "all_operators": {
            "title_template": "Прогресс всех операторов за {period}",
            "metrics": [
                {
                    "name": "avg_call_rating",
                    "label": "Средний рейтинг звонков",
                    "color": "#2ecc71",
                    "line_style": "-",
                    "marker": "o",
                },
                {
                    "name": "conversion_rate",
                    "label": "Конверсия",
                    "color": "#1abc9c",
                    "line_style": "-",
                    "marker": "*",
                },
            ],
            "xlabel": "Дата",
            "ylabel": "Значение",
            "grid": True,
            "legend_position": "upper right",
        },
    }

    class TempFileManager:
        """Менеджер временных файлов с безопасным удалением."""

        def __init__(
            self, temp_dir: str, max_retries: int = 3, retry_delay: float = 0.5
        ):
            self.temp_dir = temp_dir
            self.max_retries = max_retries
            self.retry_delay = retry_delay
            self.lock = asyncio.Lock()
            self.active_files = set()

        async def create_temp_file(self, prefix: str = "", suffix: str = "") -> str:
            async with self.lock:
                filename = f"{prefix}{uuid.uuid4()}{suffix}"
                filepath = os.path.join(self.temp_dir, filename)
                self.active_files.add(filepath)
                return filepath

        async def cleanup_old_files(self, max_age: int = 3600) -> None:
            """
            Удаляет временные файлы, возраст которых превышает `max_age`.

            Args:
                max_age (int): Максимальный возраст файла в секундах (по умолчанию: 3600).
            """
            current_time = time.time()
            async with self.lock:
                for filepath in list(
                    self.active_files
                ):  # Создаем копию, чтобы избежать изменений в итерации
                    try:
                        # Проверяем, существует ли файл
                        if not os.path.exists(filepath):
                            logger.info(
                                f"Файл {filepath} больше не существует. Удаление из активного списка."
                            )
                            self.active_files.discard(filepath)
                            continue

                        # Проверяем возраст файла
                        file_age = current_time - os.path.getmtime(filepath)
                        if file_age > max_age:
                            logger.info(
                                f"Файл {filepath} старше {max_age} секунд. Попытка удаления."
                            )
                            if await self.remove_temp_file(filepath):
                                logger.info(f"Старый временный файл удален: {filepath}")
                            else:
                                logger.warning(f"Не удалось удалить файл: {filepath}")

                    except Exception as e:
                        logger.error(
                            f"Ошибка при очистке файла {filepath}: {e}", exc_info=True
                        )

    async def remove_temp_file(self, filepath: str) -> bool:
        """
        Удаляет временный файл.

        Args:
            filepath (str): Путь к файлу.

        Returns:
            bool: Успешно ли удаление.
        """
        try:
            os.remove(filepath)
            self.active_files.discard(filepath)
            return True
        except Exception as e:
            logger.error(f"Ошибка при удалении файла {filepath}: {e}", exc_info=True)
            return False

    VALIDATION_RULES = {
        "date": {
            "type": datetime,
            "min": datetime(2000, 1, 1),
            "max": datetime.now() + timedelta(days=1),
            "error": "Дата должна быть между 2000 годом и завтрашним днем",
        },
        "avg_call_rating": {
            "type": (int, float),
            "min": 0,
            "max": 100,
            "error": "Рейтинг звонка должен быть от 0 до 100",
        },
        "total_calls": {
            "type": int,
            "min": 0,
            "error": "Количество звонков не может быть отрицательным",
        },
        "accepted_calls": {
            "type": int,
            "min": 0,
            "error": "Количество принятых звонков не может быть отрицательным",
        },
        "booked_services": {
            "type": int,
            "min": 0,
            "error": "Количество забронированных услуг не может быть отрицательным",
        },
        "complaint_calls": {
            "type": int,
            "min": 0,
            "error": "Количество жалоб не может быть отрицательным",
        },
        "conversion_rate": {
            "type": (int, float),
            "min": 0,
            "max": 100,
            "error": "Конверсия должна быть от 0 до 100",
        },
    }

    class ValidationError(Exception):
        """Исключение для ошибок валидации."""

        pass

    def handle_bot_exceptions(error_message: str = None):
        """
        Декоратор для обработки исключений в методах бота.

        Args:
            error_message: Сообщение об ошибке для пользователя
        """

        def decorator(func):
            async def wrapper(self, *args, **kwargs):
                try:
                    return await func(self, *args, **kwargs)
                except ValidationError as e:
                    # Ошибки валидации логируем как warning
                    logger.warning(
                        f"Ошибка валидации в {func.__name__}: {str(e)}", exc_info=True
                    )
                    # Для Telegram-обработчиков
                    if args and isinstance(args[0], (Update, CallbackQuery)):
                        message = (
                            args[0].message
                            if isinstance(args[0], CallbackQuery)
                            else args[0].message
                        )
                        if message:
                            await message.reply_text(f"⚠️ {str(e)}")
                    return None
                except Exception as e:
                    # Остальные ошибки как error
                    logger.error(f"Ошибка в {func.__name__}: {str(e)}", exc_info=True)
                    # Для Telegram-обработчиков
                    if args and isinstance(args[0], (Update, CallbackQuery)):
                        message = (
                            args[0].message
                            if isinstance(args[0], CallbackQuery)
                            else args[0].message
                        )
                        if message:
                            await message.reply_text(
                                error_message
                                or "❌ Произошла ошибка при выполнении операции"
                            )
                    return None

            return wrapper

        return decorator

    async def _prepare_data_for_visualization(
        self, data: Dict[str, Any], resample_threshold: int = None
    ) -> Tuple[pd.DataFrame, List[str]]:
        """
        Подготовка данных для визуализации. Если ключи действительно выглядят
        как "YYYY-MM-DD - YYYY-MM-DD" (timeseries), то обрабатываем их как даты.
        Иначе предполагаем, что это просто словарь метрик ('avg_call_rating', ...),
        и возвращаем DataFrame из одной строки.
        """
        warnings = []
        resample_threshold = resample_threshold or self.MAX_RECORDS_FOR_VISUALIZATION

        try:
            logging.info("Начало подготовки данных для визуализации.")

            # Проверка наличия данных
            if not data:
                logging.error("Данные для визуализации отсутствуют.")
                raise DataProcessingError("Данные для визуализации отсутствуют.")

            # --- КЛЮЧЕВОЙ БЛОК: проверяем, действительно ли это похоже на таймсерию ---
            # Например, хотя бы один ключ должен соответствовать шаблону YYYY-MM-DD - YYYY-MM-DD
            pattern = r"^\d{4}-\d{2}-\d{2}\s-\s\d{4}-\d{2}-\d{2}$"
            is_timeseries = any(re.match(pattern, key.strip()) for key in data.keys())

            if not is_timeseries:
                # Если это не timeseries — просто создаём DataFrame с одной строкой,
                # где колонки = ключи (например, avg_call_rating, ...)
                logging.debug(
                    "Данные не выглядят как таймсерия, создаём DataFrame из одной строки."
                )
                df = pd.DataFrame([data])  # <-- одна строка, ключи = колонки
                warnings.append(
                    "Данные не являются временным рядом: парсинг дат не выполняется."
                )
                # На этом этапе пропускаем остальную логику (ресемплинг и т.п. не нужен).
                return df, warnings
            # --- Если всё же timeseries (нашли хотя бы один ключ-дата): ---
            logging.debug(
                "Обнаружены ключи, похожие на временной ряд. Парсим как даты."
            )

            # Создание DataFrame со строками = keys
            df = pd.DataFrame.from_dict(data, orient="index")
            logging.debug(f"Создан DataFrame (timeseries):\n{df}")

            # Проверяем, что DataFrame не пуст
            if df.empty:
                logging.warning("Полученный DataFrame пуст.")
                raise DataProcessingError("После обработки не осталось данных.")

            # Проверяем наличие callback_dispatcher
            if not hasattr(self, "callback_dispatcher") or not self.callback_dispatcher:
                logging.error("callback_dispatcher не настроен в TelegramBot.")
                raise AttributeError("callback_dispatcher отсутствует в TelegramBot.")

            logging.debug(f"Индекс перед преобразованием диапазонов дат:\n{df.index}")

            def parse_range(index_value: str) -> Optional[pd.Timestamp]:
                # Ещё раз проверим точно тот же шаблон
                pattern_full = r"^\d{4}-\d{2}-\d{2}$"
                if re.match(pattern_full, index_value.strip()):
                    try:
                        start_date, _ = self.callback_dispatcher._parse_date_range(
                            index_value
                        )
                        return pd.Timestamp(start_date)
                    except Exception as exc:
                        logger.debug(f"Не удалось распарсить '{index_value}': {exc}")
                        return None
                else:
                    # Если ключ не подходит под паттерн — считаем его не датой
                    logger.debug(
                        f"Не обрабатываем ключ '{index_value}' как дату (timeseries)."
                    )
                    return None

            # Применяем parse_range к индексам
            df.index = df.index.map(parse_range)
            logging.debug(f"Индекс после parse_range:\n{df.index}")

            # Удаляем строки, где index = None (NaT)
            invalid_dates = df.index.isna().sum()
            if invalid_dates > 0:
                warnings.append(
                    f"Обнаружено {invalid_dates} некорректных ключей (не дата). Строки удалены."
                )
                self.logger.warning(f"Некорректные строки:\n{df[df.index.isna()]}")
                df = df[df.index.notna()]

            # Сортировка по индексу (если остались какие-то даты)
            if not df.empty:
                df = df.sort_index()
            logging.debug(f"DataFrame после сортировки:\n{df}")

            # Если после фильтрации всё исчезло, то добавим «заглушку»
            if df.empty:
                logging.warning("После обработки дат DataFrame пуст.")
                df.loc[pd.Timestamp.now()] = [0] * len(df.columns)
                warnings.append(
                    "Добавлена строка с нулевыми значениями для предотвращения пустоты."
                )
                return df, warnings

            # Ресемплинг, если превышен лимит записей
            if len(df) > resample_threshold:
                warnings.append(
                    f"Количество записей ({len(df)}) превышает лимит ({resample_threshold}). "
                    f"Выполняется ресемплинг."
                )
                df = await self._resample_data(df, resample_threshold)
                logging.debug(f"DataFrame после ресемплинга:\n{df}")

            # Обработка пропущенных значений
            na_counts = df.isna().sum()
            if na_counts.any():
                warnings.append(
                    "Обнаружены пропущенные значения: "
                    + ", ".join(
                        f"{col} ({count})"
                        for col, count in na_counts.items()
                        if count > 0
                    )
                )
                df = df.fillna(method="ffill").fillna(0)
                logging.debug(f"DataFrame после заполнения пропусков:\n{df}")

            logging.info("Подготовка данных для визуализации завершена.")
            return df, warnings

        except Exception as e:
            logging.error(f"Ошибка при подготовке данных: {e}", exc_info=True)
            raise DataProcessingError(f"Ошибка подготовки данных: {e}")

    async def _resample_data(
        self,
        df: pd.DataFrame,
        target_points: int,
    ) -> pd.DataFrame:
        """
        Оптимизированный ресемплинг данных для визуализации.

        Args:
            df: Исходный DataFrame с DatetimeIndex
            target_points: Целевое количество точек

        Returns:
            pd.DataFrame: Ресемплированный DataFrame

        Raises:
            DataProcessingError: При ошибке ресемплинга
        """
        try:
            # Если у нас слишком мало строк или индекс не Datetime, выходим
            if df.empty or len(df) < 2:
                logging.warning(
                    "Недостаточно точек для ресемплинга или DataFrame пуст."
                )
                return df

            if not isinstance(df.index, pd.DatetimeIndex):
                raise DataProcessingError(
                    "Для ресемплинга требуется DatetimeIndex, но он отсутствует."
                )

            total_seconds = (df.index[-1] - df.index[0]).total_seconds()
            if total_seconds <= 0:
                logging.warning(
                    "Интервал дат нулевой или отрицательный. Возврат исходных данных."
                )
                return df

            # Вычисляем интервалы
            interval_seconds = max(int(total_seconds / target_points), 1)

            # Определяем подходящую частоту ресемплинга
            if interval_seconds < 60:
                freq = f"{interval_seconds}S"  # секунды
            elif interval_seconds < 3600:
                freq = f"{interval_seconds // 60}T"  # минуты
            elif interval_seconds < 86400:
                freq = f"{interval_seconds // 3600}H"  # часы
            else:
                freq = f"{interval_seconds // 86400}D"  # дни

            # Подбираем методы агрегации для каждой колонки
            agg_methods: Dict[str, str] = {}
            for column in df.columns:
                unique_ratio = df[column].nunique() / len(df)
                agg_methods[column] = self.data_processor.get_aggregation_method(
                    column, df[column].dtype, unique_ratio
                )

            # Проводим ресемплинг
            resampled = df.resample(freq).agg(agg_methods)

            # Если после ресемплинга ещё слишком много точек, прореживаем
            if len(resampled) > target_points:
                step = max(len(resampled) // target_points, 1)
                resampled = resampled.iloc[::step]

            logging.debug(f"DataFrame после ресемплинга:\n{resampled}")
            return resampled

        except Exception as e:
            logging.error(f"Ошибка при ресемплинге данных: {e}", exc_info=True)
            # Если хотим fallback: вернуть хотя бы часть данных
            if not df.empty:
                step = max(len(df) // max(target_points, 1), 1)
                return df.iloc[::step]
            raise DataProcessingError(f"Ошибка ресемплинга данных: {e}")

    def _get_optimal_aggregation(self, df: pd.DataFrame) -> Dict[str, str]:
        """
        Определяет оптимальный метод агрегации для каждой колонки.

        Args:
            df: Исходный DataFrame

        Returns:
            Dict[str, str]: Словарь методов агрегации для каждой колонки
        """
        agg_dict = {}
        for column in df.columns:
            # Определяем тип данных
            if pd.api.types.is_numeric_dtype(df[column]):
                if "count" in column.lower() or "quantity" in column.lower():
                    agg_dict[column] = "sum"  # Для счетчиков используем сумму
                elif "rate" in column.lower() or "ratio" in column.lower():
                    agg_dict[column] = "mean"  # Для коэффициентов используем среднее
                else:
                    # Анализируем распределение данных
                    if (
                        df[column].nunique() / len(df) < 0.1
                    ):  # Если мало уникальных значений
                        agg_dict[column] = "mode"  # Используем моду
                    else:
                        agg_dict[column] = "mean"  # По умолчанию среднее
            else:
                agg_dict[column] = (
                    "last"  # Для нечисловых данных берем последнее значение
                )

        return agg_dict

    async def _process_metric_data(
        self,
        df: pd.DataFrame,
        metric: Dict[str, Any],
        is_all_operators: bool = False,
    ) -> pd.Series:
        """
        Оптимизированная обработка данных метрики.

        Args:
            df: DataFrame с данными
            metric: Конфигурация метрики
            is_all_operators: Флаг обработки данных всех операторов

        Returns:
            pd.Series: Обработанные данные метрики
        """
        try:
            metric_name = metric["name"]
            if metric_name not in df.columns:
                logger.warning(f"Метрика {metric_name} отсутствует в данных")
                return pd.Series(dtype=float)

            data = df[metric_name]

            # Обрабатываем сложные данные
            processed_data = self.metric_processor.process_complex_data(data, metric)

            # Нормализуем данные
            normalized_data = self.metric_processor.normalize_data(
                processed_data, metric
            )

            return normalized_data

        except Exception as e:
            logger.error(f"Ошибка обработки метрики {metric_name}: {e}")
            return pd.Series(dtype=float)

    async def generate_progress_visualization(
        self,
        data: Dict[str, Any],
        visualization_type: str,
        period: str,
        operator_name: Optional[Union[str, int]] = None,
        override_config: Optional[Dict[str, Any]] = None,  # <-- Новый аргумент
    ) -> Tuple[BytesIO, str]:
        """
        Универсальный метод для генерации визуализации прогресса.

        Args:
            data: Данные для визуализации (словарь вида { "avg_call_rating": 4.19, ... }).
            visualization_type: Тип визуализации ('operator_progress' или 'all_operators').
            period: Период (строка), используемый в заголовках / подписях.
            operator_name: Имя или ID оператора (опционально, только для operator_progress).
            override_config: Словарь конфигурации, который содержит 'metrics' (и т.д.),
                            если мы хотим обойтись без вызова _get_visualization_config.

        Returns:
            Tuple[BytesIO, str]: Буфер изображения и текст с итоговыми трендами/предупреждениями.

        Raises:
            DataProcessingError: При ошибках обработки данных или построения графика.
        """
        async with self.visualization_semaphore:
            try:
                # Преобразуем operator_name в строку, чтобы избежать проблем с типами
                operator_name_str = (
                    f"оператор {operator_name}" if operator_name else "все операторы"
                )
                logger.info(
                    f"Начало генерации визуализации типа '{visualization_type}' для {operator_name_str}"
                )
                logger.debug(f"Входные данные для визуализации: {data}")

                # 1) Подготовка и валидация данных
                df, warnings = await self._prepare_data_for_visualization(data)
                logger.debug(f"DataFrame после _prepare_data_for_visualization:\n{df}")

                # 2) Если передали override_config, используем его;
                #    иначе — обычный путь (вызвать _get_visualization_config)
                if override_config is not None:
                    config = override_config
                    logger.debug(
                        "Используем переданный override_config вместо _get_visualization_config."
                    )
                else:
                    config = await self._get_visualization_config(
                        visualization_type, operator_name, period
                    )
                    logger.debug(f"_get_visualization_config вернул:\n{config}")

                # 3) Создаём график ( _create_visualization внутри ищет config["metrics"] )
                buf, trend_message = await self._create_visualization(
                    df, config, is_all_operators=(visualization_type == "all_operators")
                )

                # 4) Добавляем предупреждения (если вернулись из _prepare_data_for_visualization)
                if warnings:
                    trend_message += "\n\n⚠️ Предупреждения:\n" + "\n".join(
                        f"- {w}" for w in warnings
                    )

                return buf, trend_message

            except Exception as e:
                error_msg = f"Ошибка при генерации визуализации: {e}"
                logger.error(error_msg, exc_info=True)
                raise DataProcessingError(error_msg)

    def get_period_label(self, period: str) -> str:
        """
        Возвращает читаемое имя для указанного периода.
        """
        period_map = {
            "daily": "День",
            "weekly": "Неделя",
            "monthly": "Месяц",
            "yearly": "Год",
        }
        return period_map.get(period, "Неизвестный период")

    async def _get_visualization_config(
        self,
        visualization_type: str,
        operator_name: Optional[str] = None,
        period: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Получение (и копирование) конфигурации для визуализации,
        с учётом новых групп метрик (quality, conversion и т. д.).

        Args:
            visualization_type: Тип визуализации ('operator_progress' или 'all_operators').
            operator_name: Имя оператора (для заголовка).
            period: Строковое представление периода (для заголовка).

        Returns:
            Dict[str, Any]: Конфигурация визуализации.
        """
        try:
            # Проверяем, есть ли такой ключ в PLOT_CONFIGS
            if visualization_type not in self.PLOT_CONFIGS:
                raise ValueError(
                    f"Неподдерживаемый тип визуализации: {visualization_type}"
                )

            # Берём «сырую» конфигурацию и копируем
            base_config = copy.deepcopy(self.PLOT_CONFIGS[visualization_type])

            # Если у нас есть title_template — подставляем оператора и период
            if "title_template" in base_config and operator_name:
                # Пытаемся определить "человеческое" название периода, если нужно
                period_str = self.get_period_label(period) if period else ""
                base_config["title"] = base_config["title_template"].format(
                    operator_name=operator_name, period=period_str
                )
            # иначе — можно оставить base_config["title"] = (что было),
            # или без title, если в конфиге не задано.

            # Дополнительно можно проконтролировать наличие
            # base_config["groups"] или base_config["metrics"], если нужно.
            # Ниже — просто пример, как не упасть, если ["groups"] нет:
            if "groups" not in base_config and "metrics" not in base_config:
                # Это не ошибка, если вы сами не считаете, что
                # должен быть хотя бы один из ключей
                self.logger.debug(
                    f"В конфиге {visualization_type} нет ни 'groups', ни 'metrics'. "
                    f"Возможна динамическая обработка в дальнейшем."
                )

            return base_config

        except Exception as e:
            # Оборачиваем любую ошибку в DataProcessingError, чтобы
            # верхний уровень мог её перехватить.
            raise DataProcessingError(f"Ошибка получения конфигурации: {e}")

    def _configure_plot_appearance(self, fig, ax: Any, config: Dict[str, Any]) -> None:
        """
        Настройка внешнего вида графика с максимальным логированием.

        Args:
            fig: Объект фигуры matplotlib
            ax: Объект осей matplotlib
            config: Конфигурация визуализации
        """
        logging.info("Начало настройки внешнего вида графика.")
        try:
            logging.debug(f"Конфигурация графика: {config}")

            # Устанавливаем метки осей
            xlabel = config.get("xlabel", "Дата")
            ylabel = config.get("ylabel", "Значение")
            title = config.get("title", "Прогресс")
            legend_position = config.get("legend_position", "upper right")

            ax.set_xlabel(xlabel)
            logging.info(f"Установлена подпись оси X: {xlabel}")
            ax.set_ylabel(ylabel)
            logging.info(f"Установлена подпись оси Y: {ylabel}")
            ax.set_title(title)
            logging.info(f"Установлен заголовок графика: {title}")
            ax.legend(loc=legend_position)
            logging.info(f"Установлена позиция легенды: {legend_position}")

            # Форматируем ось X для отображения дат
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
            fig.autofmt_xdate()
            logging.info("Ось X отформатирована для отображения дат.")

        except Exception as e:
            logging.error(
                f"Ошибка при настройке внешнего вида графика: {e}", exc_info=True
            )
        finally:
            logging.info("Настройка внешнего вида графика завершена.")

    async def generate_all_operators_progress(
        self, operator_data: Dict[str, Any], period: str
    ) -> Tuple[BytesIO, str]:
        """
        Обертка для generate_progress_visualization для всех операторов с логированием.

        Args:
            operator_data: Данные операторов
            period: Период для визуализации

        Returns:
            Tuple[BytesIO, str]: Буфер изображения и сообщение о трендах
        """
        logging.info(
            f"Начало генерации графика прогресса для всех операторов за период {period}."
        )
        try:
            return await self.generate_progress_visualization(
                operator_data, "all_operators", period
            )
        except Exception as e:
            logging.error(
                f"Ошибка генерации прогресса всех операторов: {e}", exc_info=True
            )
            raise

    async def generate_operator_progress(
        self, operator_data: Dict[str, Any], operator_name: str, period: str
    ) -> List[Tuple[str, BytesIO, str, str]]:
        """
        Генерация нескольких графиков по группам метрик (quality, conversion, call_handling, time, summary).
        Для каждой группы:
        - Берём конфиг метрик из PLOT_CONFIGS["operator_progress"]["groups"][group_name].
        - Убираем метрики, которых реально нет в group_data, чтобы не ловить KeyError.
        - Строим график через generate_progress_visualization (передавая локальный конфиг).
        - Генерируем комментарий через generate_commentary_on_metrics.
        - Возвращаем список (group_name, buf, trend_msg, commentary).
        """

        operator_name_str = (
            str(operator_name) if operator_name else "Неизвестный оператор"
        )
        logging.info(
            f"Генерация графиков для оператора {operator_name_str} за период {period}."
        )

        try:
            if not operator_data:
                logging.error("Полученные данные для оператора пусты.")
                raise DataProcessingError("Полученные данные для оператора пусты.")

            # Берём общий конфиг для operator_progress
            op_config = self.PLOT_CONFIGS["operator_progress"]
            # Извлекаем словарь "groups"
            group_configs = op_config["groups"]

            # Список групп, которые хотим обработать
            groups = ["quality", "conversion", "call_handling", "time", "summary"]
            results = []

            for group_name in groups:
                # Если в operator_data нет этой группы, пропускаем
                if group_name not in operator_data:
                    logging.warning(
                        f"[generate_operator_progress]: Группа метрик '{group_name}' "
                        f"отсутствует у оператора {operator_name_str}"
                    )
                    continue

                # Если в PLOT_CONFIGS нет такого блока
                if group_name not in group_configs:
                    logging.warning(
                        f"[generate_operator_progress]: В PLOT_CONFIGS нет секции group '{group_name}'. Пропускаем."
                    )
                    continue

                # Берём реальные данные оператора по группе
                group_data = operator_data[group_name]
                logging.info(
                    f"[generate_operator_progress]: Построение графика для группы '{group_name}' "
                    f"оператора {operator_name_str}"
                )

                # Достаём список метрик, прописанных в конфиге для этой группы
                group_metric_list = group_configs[group_name]

                # Фильтруем только те метрики, которые действительно есть в group_data
                filtered_metrics = []
                for m_cfg in group_metric_list:
                    metric_name = m_cfg["name"]
                    if metric_name in group_data:
                        filtered_metrics.append(m_cfg)
                    else:
                        logging.debug(
                            f"Метрика '{metric_name}' отсутствует в group_data, пропускаем."
                        )

                if not filtered_metrics:
                    logging.info(
                        f"После фильтрации метрик для группы '{group_name}' "
                        f"у оператора {operator_name_str} не осталось метрик для построения."
                    )
                    continue

                # Сформируем отдельный словарь (subset) данных, где только нужные метрики
                filtered_data_for_plot = {
                    m["name"]: group_data[m["name"]] for m in filtered_metrics
                }

                # Создаём "локальный" конфиг с ключом "metrics"
                plot_config = {
                    "title": f"График: {group_name} / {operator_name_str} / {period}",
                    "xlabel": "Дата",
                    "ylabel": "Значение",
                    "grid": True,  # или op_config.get("grid", True)
                    "legend_position": "upper right",
                    "metrics": filtered_metrics,  # <--- КЛЮЧЕВОЙ момент!
                }

                # Вызываем визуализацию, передавая override_config = plot_config
                # Внутри generate_progress_visualization нужно поддержать этот параметр
                buf, trend_msg = await self.generate_progress_visualization(
                    filtered_data_for_plot,  # данные (только нужные метрики)
                    "operator_progress",  # visualization_type
                    period,
                    operator_name_str,
                    override_config=plot_config,  # <-- ключевой аргумент
                )

                # Генерация комментария
                metrics_keys = list(filtered_data_for_plot.keys())
                commentary = await self.generate_commentary_on_metrics(
                    [filtered_data_for_plot],  # список из одного словаря
                    metrics_keys,
                    operator_name_str,
                    period,
                )

                # Складываем всё в results
                results.append((group_name, buf, trend_msg, commentary))

            return results

        except DataProcessingError as e:
            logging.error(
                f"Ошибка обработки данных для оператора {operator_name_str}: {e}",
                exc_info=True,
            )
            raise
        except Exception as e:
            logging.error(
                f"Ошибка генерации прогресса для оператора {operator_name_str}: {e}",
                exc_info=True,
            )
            raise DataProcessingError(f"Общая ошибка: {e}")

    async def fetch_operator_progress_data(
        self, operator_id: str, period_str: str
    ) -> Optional[Tuple[dict, pd.DataFrame, datetime, datetime, List[str]]]:
        """
        Получение данных о прогрессе оператора с логированием.

        Args:
            operator_id: ID оператора
            period_str: Строка с периодом из PERIOD_CONFIG

        Returns:
            Optional[Tuple[dict, pd.DataFrame, datetime, datetime, List[str]]]:
                Кортеж (информация об операторе, данные прогресса, начальная дата,
                конечная дата, предупреждения) или None в случае ошибки
        """
        logging.info(
            f"Начало получения данных о прогрессе оператора с ID {operator_id} за период {period_str}."
        )
        try:
            # Получаем информацию об операторе
            logging.info(f"Получение информации об операторе с ID {operator_id}.")
            operator = await self.operator_data.get_operator_by_id(operator_id)
            if not operator:
                logging.warning(f"Оператор с ID {operator_id} не найден.")
                return None

            logging.debug(f"Информация об операторе: {operator}")

            # Определяем даты периода
            logging.info(f"Определение диапазона дат для периода: {period_str}.")
            try:
                start_date, end_date = self._get_date_range(period_str)
                logging.debug(f"Диапазон дат: {start_date} - {end_date}.")
            except ValueError as e:
                logging.error(
                    f"Ошибка определения диапазона дат для периода {period_str}: {e}",
                    exc_info=True,
                )
                return None

            # Получаем данные прогресса
            logging.info(f"Получение данных прогресса оператора с ID {operator_id}.")
            progress_data = await self.progress_data.get_operator_progress(
                int(operator_id), period_str
            )
            logging.debug(f"Данные прогресса: {progress_data}")

            # Проверяем валидность данных
            logging.info(f"Проверка валидности данных прогресса.")
            is_valid, warnings, valid_data = self.validate_progress_data(progress_data)
            if not is_valid:
                logging.warning(
                    f"Проблемы с данными прогресса для оператора {operator_id}: {', '.join(warnings)}"
                )
                return None

            logging.debug(f"Предупреждения при проверке данных: {warnings}")

            # Преобразуем в DataFrame
            logging.info("Преобразование данных прогресса в DataFrame.")
            df = pd.DataFrame(valid_data)
            logging.debug(f"DataFrame данных прогресса:\n{df}")

            return operator, df, start_date, end_date, warnings

        except Exception as e:
            logging.error(
                f"Ошибка при получении данных прогресса для оператора {operator_id}: {e}",
                exc_info=True,
            )
            return None
        finally:
            logging.info(
                f"Завершение получения данных прогресса для оператора с ID {operator_id}."
            )

    async def all_operators_progress_handle(
        self, update: Update, context: CallbackContext
    ) -> None:
        """
        Обработчик команды /all_operators_progress [period] с логированием.
        Показывает сводную динамику для всех операторов за указанный период.
        """
        logging.info("Начало обработки команды /all_operators_progress.")

        # Проверка аутентификации пользователя
        if not context.user_data.get("is_authenticated"):
            logging.warning("Попытка доступа без аутентификации.")
            await update.message.reply_text(
                "Пожалуйста, сначала войдите в систему с помощью команды /login"
            )
            return

        # Проверка наличия аргументов
        if len(context.args) < 1:
            logging.warning("Не указан период для команды /all_operators_progress.")
            await update.message.reply_text(
                "Укажите период.\nПример: /all_operators_progress monthly"
            )
            return

        try:
            # Извлечение периода из аргументов
            period = context.args[0].lower()
            logging.info(f"Получен период: {period}")

            # Получение данных всех операторов
            logging.info("Получение данных прогресса всех операторов.")
            all_progress = await self.progress_data.get_all_operators_progress(period)
            logging.debug(f"Полученные данные прогресса: {all_progress}")

            # Проверка валидности данных
            logging.info("Проверка валидности данных прогресса.")
            is_valid, warnings, valid_data = self.validate_progress_data(all_progress)
            if not is_valid:
                logging.warning(f"Проблемы с данными прогресса: {warnings}")
                await update.message.reply_text(
                    "❌ Проблема с данными:\n" + "\n".join(f"- {w}" for w in warnings)
                )
                return

            # Создание визуализации
            logging.info("Создание визуализации прогресса для всех операторов.")
            viz_result = await self.create_progress_visualization(
                operator={"name": "all"},
                data=pd.DataFrame(valid_data),
                period_str=period,
                is_all_operators=True,
            )
            if viz_result is None:
                logging.error("Не удалось создать визуализацию.")
                await update.message.reply_text("❌ Не удалось создать визуализацию.")
                return

            graph_path, trend_message = viz_result
            logging.info(f"Визуализация успешно создана. Путь к графику: {graph_path}")

            # Добавление предупреждений, если они есть
            if warnings:
                logging.info("Добавление предупреждений к сообщению.")
                trend_message += "\n\n⚠️ Предупреждения:\n" + "\n".join(
                    f"- {warning}" for warning in warnings
                )

            # Отправка результата
            logging.info("Отправка результатов визуализации.")
            await self.send_visualization_result(
                update.message,
                graph_path,
                trend_message,
                "❌ Не удалось отправить результаты.",
            )

        except Exception as e:
            logging.error(
                f"Ошибка при обработке общего прогресса всех операторов: {e}",
                exc_info=True,
            )
            await update.message.reply_text("Произошла ошибка при получении прогресса.")
        finally:
            logging.info("Завершение обработки команды /all_operators_progress.")

    async def operator_progress_handle(self, update: Update, context: CallbackContext):
        """
        Обработчик команды /operator_progress с логированием.
        Показывает прогресс конкретного оператора за указанный период.
        """
        logging.info("Начало обработки команды /operator_progress.")

        # Проверка аутентификации пользователя
        if not context.user_data.get("is_authenticated"):
            logging.warning("Попытка доступа без аутентификации.")
            await update.message.reply_text(
                "Пожалуйста, сначала войдите в систему с помощью команды /login"
            )
            return

        # Проверка наличия аргументов
        if len(context.args) < 2:
            logging.warning("Недостаточно аргументов для команды /operator_progress.")
            await update.message.reply_text(
                "Укажите ID оператора и период.\nПример: /operator_progress 5 monthly"
            )
            return

        try:
            # Извлечение аргументов
            operator_id = int(context.args[0])
            period = context.args[1].lower()
            logging.info(
                f"Получены аргументы: operator_id={operator_id}, period={period}"
            )

            # Проверка прав доступа
            user_id = update.effective_user.id
            user_role = context.user_data.get("user_role")
            logging.info(
                f"Проверка прав доступа для пользователя {user_id} с ролью {user_role}."
            )
            if user_role in ["Operator", "Admin"] and user_id != operator_id:
                logging.warning(
                    f"Пользователь {user_id} попытался получить доступ к данным оператора {operator_id}."
                )
                await update.message.reply_text(
                    "У вас нет прав для просмотра прогресса других операторов."
                )
                return

            # Получение данных оператора
            logging.info(f"Получение данных оператора с ID {operator_id}.")
            operator = await self.operator_data.get_operator_by_id(operator_id)
            if not operator:
                logging.warning(f"Оператор с ID {operator_id} не найден.")
                await update.message.reply_text("Оператор не найден.")
                return
            logging.debug(f"Данные оператора: {operator}")

            # Получение данных прогресса
            logging.info(
                f"Получение данных прогресса оператора {operator_id} за период {period}."
            )
            progress_data = await self.progress_data.get_operator_progress(
                operator_id, period
            )
            logging.debug(f"Данные прогресса: {progress_data}")

            # Генерация визуализации
            logging.info("Генерация визуализации прогресса оператора.")
            graph_data, trend_message = await self.generate_operator_progress(
                progress_data, operator["name"], period
            )
            logging.info("Визуализация прогресса оператора успешно создана.")

            # Отправка результата
            logging.info("Отправка результатов визуализации прогресса оператора.")
            await self.send_visualization_result(
                update.message,
                graph_data,
                trend_message,
                "Не удалось создать визуализацию прогресса.",
            )

        except ValueError as ve:
            logging.error(f"Неверный формат ID оператора: {ve}", exc_info=True)
            await update.message.reply_text("Неверный формат ID оператора.")
        except Exception as e:
            logging.error(
                f"Ошибка при обработке прогресса оператора: {e}", exc_info=True
            )
            await update.message.reply_text("Произошла ошибка при получении прогресса.")
        finally:
            logging.info("Завершение обработки команды /operator_progress.")

    @handle_bot_exceptions("❌ Не удалось получить список операторов")
    async def operator_progress_menu_handle(
        self, update: Update, context: CallbackContext
    ) -> None:
        """
        Обработчик команды /operator_progress_menu с максимальным логированием.
        """
        user_id = update.effective_user.id
        logger.info(f"Команда /operator_progress_menu от пользователя {user_id}")

        # Проверка аутентификации
        if not context.user_data.get("is_authenticated"):
            logger.warning(
                f"Пользователь {user_id} попытался получить доступ без аутентификации."
            )
            await update.message.reply_text(
                "Сначала войдите с помощью /login ваш_пароль."
            )
            return

        try:
            # Подключение к базе данных для получения списка операторов
            logger.info("Подключение к базе данных для получения списка операторов.")
            async with self.db_manager.acquire() as connection:
                async with connection.cursor(aiomysql.DictCursor) as cursor:
                    query = "SELECT DISTINCT name FROM reports ORDER BY name"
                    logger.debug(f"Выполнение SQL-запроса: {query}")
                    await cursor.execute(query)
                    operators = await cursor.fetchall()
                    logger.debug(f"Полученные операторы: {operators}")

            # Проверка на наличие операторов
            if not operators:
                logger.warning("Операторы в базе данных отсутствуют.")
                await update.message.reply_text("Нет операторов в базе.")
                return

            # Создание кнопок для выбора операторов
            logger.info("Создание клавиатуры для выбора операторов.")
            keyboard = []
            for op in operators:
                operator_name = op.get("name", "").strip()
                if not operator_name:
                    logger.warning(
                        "Пропущен оператор с пустым или некорректным именем."
                    )
                    continue
                # Логируем исходное имя оператора
                logger.debug(
                    f"Обработка имени оператора: '{operator_name}', длина: {len(operator_name)}"
                )

                # Проверяем длину callback_data
                callback_data = f"operator_{operator_name}"

                if len(callback_data) > 64:
                    logger.warning(
                        f"Имя оператора '{operator_name}' слишком длинное для callback_data. Урезаем."
                    )
                    max_name_length = 64 - len(
                        "operator_"
                    )  # Учитываем запас на кодирование
                    truncated_name = operator_name[:max_name_length]
                    callback_data = f"operator_{truncated_name}"

                # Проверяем корректность callback_data
                if len(callback_data) > 64:
                    logger.error(
                        f"Не удалось создать корректный callback_data для оператора '{operator_name}'. Пропускаем."
                    )
                    continue

                # Добавляем кнопку
                keyboard.append(
                    [
                        InlineKeyboardButton(
                            text=operator_name, callback_data=callback_data
                        )
                    ]
                )

            if not keyboard:
                logger.warning(
                    "Не удалось создать клавиатуру: нет доступных операторов."
                )
                await update.message.reply_text("Нет доступных операторов.")
                return
            # Создаём разметку клавиатуры
            reply_markup = InlineKeyboardMarkup(keyboard)
            logger.debug(f"Сгенерированная клавиатура: {keyboard}")

            # Отправка сообщения пользователю
            await update.message.reply_text(
                "Выберите оператора:", reply_markup=reply_markup
            )
            logger.info("Сообщение с выбором операторов отправлено пользователю.")

        except telegram.error.BadRequest as e:
            logger.error(
                f"Ошибка при отправке сообщения с клавиатурой: {e}", exc_info=True
            )
            await update.message.reply_text("Произошла ошибка при создании клавиатуры.")
        except Exception as e:
            logger.error(
                f"Ошибка при обработке команды /operator_progress_menu: {e}",
                exc_info=True,
            )
            await update.message.reply_text("Произошла ошибка при обработке команды.")

    @handle_bot_exceptions("❌ Произошла ошибка при обработке запроса")
    async def callback_query_handler(self, update: Update, context: CallbackContext):
        """
        Обработчик callback-запросов с максимальным логированием.
        """
        query = update.callback_query
        data = query.data
        logger.info(f"Получены данные callback: {data}")

        try:
            # Передача управления CallbackDispatcher
            logger.info(f"Передача управления CallbackDispatcher для данных: {data}")
            await self.callback_dispatcher.dispatch(update, context)
        except Exception as e:
            logger.error(f"Ошибка при обработке callback: {e}", exc_info=True)
            await query.answer("Произошла ошибка в обработке команды.")

    async def setup(self):
        """Инициализация бота и всех компонентов."""
        await self.setup_db_connection()
        self.setup_handlers()
        if not self.scheduler.running:
            self.scheduler.start()
        self.scheduler.add_job(
            self.send_daily_reports, "cron", hour=14, minute=19
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
            CommandHandler("all_operators_progress", self.all_operators_progress_handle)
        )  # Регистрация новой команды
        self.application.add_handler(
            CommandHandler("operator_progress_menu", self.operator_progress_menu_handle)
        )

        # Callback для кнопок пока убрал.
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

    async def send_daily_reports(self, check_days: int = 10):
        """
        Проверяем последние N дней (по умолчанию 40), включая вчера:
        1) Из таблицы users берём всех операторов (status='on').
        2) Для каждого оператора и каждой даты (из диапазона [start_date..end_date])
            проверяем наличие записи в reports (WHERE user_id = ... AND DATE(report_date) = ...).
            Если записи нет, ставим задачу (add_task) на генерацию отчёта для каждого менеджерского chat_id.
            
        В итоге, для каждого оператора и дня, если отчёт отсутствует, создаётся задача для каждого менеджера.
        """
        logger.info(f"Начата постановка задач на отчёты. Проверяем пропуски за последние {check_days} дней.")
        try:
            # Определяем диапазон дат: end_date — вчера, start_date — начало диапазона
            end_date = date.today() - timedelta(days=1)
            start_date = end_date - timedelta(days=(check_days - 1))
            
            # Список chat_id менеджеров
            managers = [309606681, 1673538157]
            
            # Исключаем определённых операторов по user_id
            excluded_user_ids = {1}
            
            # 1) Получаем всех операторов с статусом 'on'
            async with self.db_manager.acquire() as connection:
                async with connection.cursor(aiomysql.DictCursor) as cursor:
                    query_operators = """
                        SELECT user_id
                        FROM users
                        WHERE status = 'on'
                    """
                    await cursor.execute(query_operators)
                    rows = await cursor.fetchall()
            
            if not rows:
                logger.warning("Не найдено ни одного активного оператора (status='on').")
                return
            
            operator_ids = [row["user_id"] for row in rows if row["user_id"] not in excluded_user_ids]
            if not operator_ids:
                logger.warning("Все активные операторы исключены из обработки.")
                return
            
            logger.info(f"Найдено {len(operator_ids)} операторов после фильтрации: {operator_ids}")
            
            # 2) Для каждого оператора и для каждой даты в диапазоне проверяем наличие отчёта
            async with self.db_manager.acquire() as connection:
                async with connection.cursor() as cursor:
                    for op_id in operator_ids:
                        current_day = start_date
                        while current_day <= end_date:
                            report_date_str = current_day.strftime("%Y-%m-%d")
                            
                            # Проверяем, существует ли уже отчёт за текущую дату для оператора
                            query_exist = """
                                SELECT 1
                                FROM reports
                                WHERE user_id = %s
                                AND DATE(report_date) = %s
                                LIMIT 1
                            """
                            await cursor.execute(query_exist, (op_id, report_date_str))
                            row = await cursor.fetchone()
                            if row:
                                # Если отчёт уже существует, переходим к следующему дню
                                current_day += timedelta(days=1)
                                continue
                            
                            # Если отчёта нет, ставим задачу для каждого chat_id менеджера
                            for manager_chat_id in managers:
                                logger.info(
                                    f"Нет отчёта в reports для оператора {op_id}, дата={report_date_str}. "
                                    f"Добавляем задачу в очередь (chat_id={manager_chat_id})."
                                )
                                await add_task(
                                    bot_instance=self,
                                    user_id=op_id,
                                    report_type="daily",
                                    period="daily",
                                    chat_id=manager_chat_id,
                                    date_range=report_date_str,
                                )
                            # Переходим к следующему дню
                            current_day += timedelta(days=1)
                    await connection.commit()
            
            logger.info("Все задачи на отправку отчётов успешно поставлены в очередь.")
        
        except Exception as e:
            logger.error(f"Ошибка при постановке задач на отчёты: {e}", exc_info=True)


    async def generate_and_send_report(self, user_id, period):
        """
        Генерация и отправка отчета для конкретного пользователя.
        Если в результате generate_report(...) вернулся None
        или recommendations пустое — НЕ отправляем отчёт и НЕ пишем в БД.
        """
        try:
            async with self.db_manager.acquire() as connection:
                # Допустим, внутри generate_report(...) есть логика:
                # - Сформировать текст/данные
                # - Если нет recommendations, return None (и не делать INSERT в БД)
                # - Иначе записать отчёт (INSERT) и вернуть dict/объект c данными
                report = await self.report_generator.generate_report(
                    connection, user_id, period=period
                )

            # Если None, значит либо нет данных, либо нет recommendations
            if not report:
                logger.warning(
                    f"Не удалось сформировать отчёт для пользователя {user_id}. "
                    "Возможно, отсутствуют данные или нет рекомендаций."
                )
                return

            # На всякий случай дополнительная проверка
            if not report.get("recommendations"):
                logger.warning(
                    f"Отчёт сформирован, но нет recommendations (user_id={user_id}). "
                    "Пропускаем отправку и запись."
                )
                return

            # Если всё в порядке, отправляем отчёт пользователю
            await self.send_report_to_user(user_id, report)
            logger.info(f"Отчёт успешно отправлен пользователю {user_id}")

        except Exception as e:
            logger.error(f"Ошибка при генерации отчёта для пользователя {user_id}: {e}", exc_info=True)

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

    async def _create_visualization(
        self,
        df: pd.DataFrame,
        config: Dict[str, Any],
        is_all_operators: bool,
    ) -> Tuple[BytesIO, str]:
        """
        Создание визуализации на основе подготовленных данных.

        Args:
            df: Подготовленные данные в формате DataFrame.
            config: Конфигурация визуализации, где обязательно должен быть ключ "metrics".
            is_all_operators: Флаг для визуализации всех операторов.

        Returns:
            Tuple[BytesIO, str]: Буфер с изображением графика и сообщение о трендах.

        Raises:
            DataProcessingError: При проблемах с данными или конфигурацией.
        """
        try:
            logging.info("Начало создания визуализации.")

            # Проверка, что DataFrame не пуст
            if df.empty:
                raise ValueError("Переданы пустые данные для визуализации.")

            # Проверка наличия ключа 'metrics' и того, что список непуст
            metrics_list = config.get("metrics")
            if not metrics_list or not isinstance(metrics_list, list):
                raise DataProcessingError(
                    "В конфиге отсутствует непустой список 'metrics' или тип не list. "
                )

            # Берём первую метрику (чтобы, например, сделать initial plot)
            first_metric_name = metrics_list[0]["name"]
            if first_metric_name not in df.columns:
                raise DataProcessingError(
                    f"Первая метрика '{first_metric_name}' отсутствует в DataFrame: {list(df.columns)}"
                )

            # Подготовим "пустые" данные для create_plot (например, для базовой инициализации)
            data_for_init = {
                "x": df.index.tolist(),  # Индекс DataFrame как ось X
                "y": df[first_metric_name].tolist(),
            }

            # Создаём начальный график (каркас)
            fig, ax = self.visualizer.create_plot(config, data_for_init)

            # Опционально настраиваем сетку
            if config.get("grid", True):
                ax.grid(visible=True, linestyle="--", alpha=0.7)
                logging.info("Сетка графика настроена.")

            # Проходимся по всем метрикам в config["metrics"]
            for metric_cfg in metrics_list:
                metric_name = metric_cfg["name"]

                if metric_name not in df.columns:
                    logging.warning(
                        f"Метрика '{metric_name}' отсутствует в DataFrame, пропускаем."
                    )
                    continue

                # Обработка/очистка данных по метрике
                metric_data = await self._process_metric_data(
                    df, metric_cfg, is_all_operators
                )
                # Если после обработки данных по метрике нет, пропускаем
                if metric_data.empty or metric_data.sum() == 0:
                    logging.debug(
                        f"Для метрики '{metric_name}' нет данных (или все нули). Пропускаем."
                    )
                    continue

                # Строим линию / точки
                ax.plot(
                    metric_data.index,
                    metric_data.values,
                    label=metric_cfg.get("label", metric_name),
                    color=metric_cfg.get("color", "blue"),
                    linestyle=metric_cfg.get("line_style", "-"),
                    marker=metric_cfg.get("marker", "o"),
                    markersize=config.get("marker_size", 6),
                )
                logging.info(f"График для метрики '{metric_name}' построен.")

            # Финальная настройка внешнего вида (подписи осей, легенда и т.д.)
            self._configure_plot_appearance(fig, ax, config)
            logging.info("Настройка внешнего вида графика завершена.")

            # Сохраняем фигуру в буфер
            buf = BytesIO()
            fig.savefig(
                buf,
                format="png",
                dpi=config.get("dpi", self.global_config.dpi),
                bbox_inches="tight",
                pad_inches=0.1,
            )
            buf.seek(0)
            logging.info("График сохранён в буфер (BytesIO).")

            # Считаем тренды
            all_metric_names = [m["name"] for m in metrics_list]
            trends = await self._calculate_trends(df, all_metric_names)
            trend_message = await self._format_trend_message(
                trends, metrics_list, is_all_operators
            )
            logging.info("Сообщение о трендах сформировано.")

            return buf, trend_message

        except ValueError as ve:
            logging.error(f"Ошибка в данных для визуализации: {ve}", exc_info=True)
            raise DataProcessingError(f"Ошибка данных: {ve}")
        except Exception as e:
            logging.error(f"Ошибка создания визуализации: {e}", exc_info=True)
            raise DataProcessingError(f"Ошибка создания визуализации: {e}")

    async def _process_metric_data(
        self, df: pd.DataFrame, metric: MetricConfig, is_all_operators: bool = False
    ) -> pd.Series:
        """
        Оптимизированная обработка данных метрики.

        Args:
            df: DataFrame с данными метрик
            metric: Конфигурация метрики
            is_all_operators: Флаг обработки данных всех операторов

        Returns:
            pd.Series: Обработанные данные метрики с временным индексом

        Raises:
            DataProcessingError: При ошибке обработки метрики
        """
        try:
            metric_name = metric["name"]
            if metric_name not in df.columns:
                logger.warning(f"Метрика {metric_name} отсутствует в данных")
                return pd.Series(dtype=float)

            data = df[metric_name]

            if is_all_operators:
                # Проверяем тип данных
                if isinstance(data.iloc[0], (list, dict)):
                    # Преобразуем сложные значения в DataFrame
                    if isinstance(data.iloc[0], dict):
                        expanded_data = pd.DataFrame(data.tolist(), index=data.index)
                    else:
                        expanded_data = pd.DataFrame(data.tolist(), index=data.index)

                    # Агрегируем данные
                    agg_method = cast(
                        AggregationMethod, metric.get("aggregation", "sum")
                    )
                    if agg_method == "mean":
                        return expanded_data.mean(axis=1)
                    else:
                        return expanded_data.sum(axis=1)
                else:
                    return data

            # Для одного оператора
            return pd.to_numeric(data, errors="coerce").fillna(0)

        except Exception as e:
            logger.error(f"Ошибка обработки метрики {metric_name}: {e}")
            return pd.Series(dtype=float)

    async def _calculate_trends(
        self, df: pd.DataFrame, metrics: List[str]
    ) -> List[TrendData]:
        """
        Расчет трендов для метрик.

        Args:
            df: DataFrame с данными метрик
            metrics: Список имен метрик

        Returns:
            List[TrendData]: Список данных о трендах для каждой метрики
        """
        trends: List[TrendData] = []

        try:
            for metric in metrics:
                if metric not in df.columns:
                    continue

                data = df[metric].dropna()
                if len(data) < 2:
                    continue

                current = float(data.iloc[-1])
                previous = float(data.iloc[-2])
                change = ((current - previous) / previous * 100) if previous != 0 else 0

                trend_direction: Literal["up", "down", "stable"]
                if change > 1:
                    trend_direction = "up"
                elif change < -1:
                    trend_direction = "down"
                else:
                    trend_direction = "stable"

                trends.append(
                    {
                        "metric": metric,
                        "current": current,
                        "previous": previous,
                        "change": change,
                        "trend": trend_direction,
                    }
                )

        except Exception as e:
            logger.error(f"Ошибка при расчете трендов: {e}")

        return trends

    async def _format_trend_message(
        self,
        trends: List[TrendData],
        metrics: List[MetricConfig],
        is_all_operators: bool,
    ) -> str:
        """
        Форматирование сообщения с трендами.

        Args:
            trends: Список данных о трендах
            metrics: Список конфигураций метрик
            is_all_operators: Флаг для всех операторов

        Returns:
            str: Отформатированное сообщение с трендами
        """
        if not trends:
            return "Недостаточно данных для анализа трендов"

        message_parts = []
        operator_prefix = "Все операторы" if is_all_operators else "Оператор"

        for trend in trends:
            metric_config = next(
                (m for m in metrics if m["name"] == trend["metric"]), None
            )
            if not metric_config:
                continue

            trend_symbol = {"up": "📈", "down": "📉", "stable": "➡️"}[trend["trend"]]

            message_parts.append(
                f"{trend_symbol} {metric_config['label']}: "
                f"{trend['current']:.2f} "
                f"({trend['change']:+.1f}%)"
            )

        return f"{operator_prefix}:\n" + "\n".join(message_parts)

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
            values = [
                row.get(metric)
                for row in data
                if row.get(metric) is not None and row.get(metric) > 0
            ]
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


# Основная функция для запуска бота
async def main():
    """Основная функция запуска бота."""
    logger.info("Запуск бота...")

    # Проверяем конфигурацию
    if not config.telegram_token:
        raise ValueError("Telegram token отсутствует в конфигурации")
    if not hasattr(config, "db_config"):
        raise ValueError("Отсутствует конфигурация базы данных")
    bot = None
    try:
        # Инициализация и запуск бота
        bot = TelegramBot(config.telegram_token)
        logger.info("Бот успешно инициализирован.")
        await bot.run()
    except Exception as e:
        logger.error(f"Произошла ошибка при запуске бота: {e}", exc_info=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Приложение завершено пользователем.")
    except Exception as e:
        logger.critical(f"Необработанная ошибка в главной функции: {e}", exc_info=True)
