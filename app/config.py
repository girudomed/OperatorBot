# Файл: app/config.py

"""
Модуль конфигурации приложения.

Загружает переменные окружения и предоставляет конфигурационные параметры.
"""

from __future__ import annotations

import os
from dotenv import load_dotenv
from typing import Dict, Any

# Загрузка переменных окружения из файла .env
load_dotenv()

IS_CI_ENV = os.getenv("CI", "").lower() == "true"


def _get_bool(value: str | None, default: bool = False) -> bool:
    """
    Преобразует строковые значения окружения в bool.
    
    Args:
        value: Строковое значение для преобразования
        default: Значение по умолчанию
        
    Returns:
        bool: Преобразованное значение
    """
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def check_required_env_vars(required_vars: list[str]) -> None:
    """
    Проверка наличия обязательных переменных окружения.
    
    Args:
        required_vars: Список обязательных переменных
        
    Raises:
        EnvironmentError: Если какая-то переменная отсутствует
    """
    missing_vars = [
        var for var in required_vars 
        if not os.getenv(var) or os.getenv(var, "").strip() == ""
    ]
    if missing_vars:
        raise EnvironmentError(
            f"Отсутствуют необходимые переменные окружения: {', '.join(missing_vars)}"
        )


# Переменные, которые обязательно должны быть в окружении
REQUIRED_ENV_VARS = [
    "OPENAI_API_KEY",
    "TELEGRAM_BOT_TOKEN",
    "DB_HOST",
    "DB_USER",
    "DB_PASSWORD",
    "DB_NAME",
    "DB_PORT"
]

# Проверка обязательных переменных только когда запущены рабочие сервисы
if not IS_CI_ENV and _get_bool(os.getenv("CHECK_ENV_VARS", "true"), True):
    check_required_env_vars(REQUIRED_ENV_VARS)

# Конфигурация OpenAI
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
if IS_CI_ENV and not OPENAI_API_KEY:
    OPENAI_API_KEY = "ci-test-key"
OPENAI_API_BASE = os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

OPENAI_COMPLETION_OPTIONS: Dict[str, Any] = {
    "temperature": float(os.getenv("OPENAI_TEMPERATURE", "0.7")),
    "max_tokens": int(os.getenv("OPENAI_MAX_TOKENS", "2000")),
    "top_p": float(os.getenv("OPENAI_TOP_P", "1")),
    "frequency_penalty": float(os.getenv("OPENAI_FREQUENCY_PENALTY", "0")),
    "presence_penalty": float(os.getenv("OPENAI_PRESENCE_PENALTY", "0")),
    "request_timeout": float(os.getenv("OPENAI_REQUEST_TIMEOUT", "60.0")),
}

# Конфигурация Telegram
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not TELEGRAM_TOKEN and not IS_CI_ENV:
    raise EnvironmentError("Telegram Bot Token не найден.")

# Конфигурация базы данных
DB_CONFIG: Dict[str, Any] = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "db": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "charset": os.getenv("DB_CHARSET", "utf8mb4"),
    "autocommit": _get_bool(os.getenv("DB_AUTOCOMMIT", "true"), True),
    "minsize": int(os.getenv("DB_POOL_MIN", "1")),
    "maxsize": int(os.getenv("DB_POOL_MAX", "50")),
}

# Проверка конфигурации базы данных
if not IS_CI_ENV and not all(DB_CONFIG[key] for key in ["host", "user", "password", "db", "port"]):
    raise EnvironmentError("Конфигурация базы данных неполная.")

# Совместимость с legacy-конфигом
DATABASE_CONFIG = DB_CONFIG

# Параметры для планировщика задач
REPORT_SEND_TIME = os.getenv("REPORT_SEND_TIME", "18:00")

# Дополнительные сервисы
SENTRY_DSN = os.getenv("SENTRY_DSN")

# Очередь задач
TASK_QUEUE_CONFIG: Dict[str, Any] = {
    "worker_count": int(os.getenv("TASK_WORKERS", "3")),
    "queue_max_size": int(os.getenv("TASK_QUEUE_MAX_SIZE", "50")),
    "max_retries": int(os.getenv("TASK_MAX_RETRIES", "3")),
    "retry_base_delay": float(os.getenv("TASK_RETRY_BASE_DELAY", "5.0")),
    "retry_backoff": float(os.getenv("TASK_RETRY_BACKOFF", "2.0")),
    "retry_max_delay": float(os.getenv("TASK_RETRY_MAX_DELAY", "60.0")),
}

# Уровень логирования
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FILE = os.getenv("LOG_FILE", "logs/app.log")

# Bootstrap admin identities
SUPREME_ADMIN_ID = os.getenv("SUPREME_ADMIN_ID")
SUPREME_ADMIN_USERNAME = os.getenv("SUPREME_ADMIN_USERNAME")
DEV_ADMIN_ID = os.getenv("DEV_ADMIN_ID")
DEV_ADMIN_USERNAME = os.getenv("DEV_ADMIN_USERNAME")

# Manual link
_manual_url = os.getenv(
    "MANUAL_URL",
    "https://docs.google.com/document/d/1g2cpa4Pzv6NhZ7hL6bLvo26TF0--KxWlqVnoxvDvpss/edit?usp=sharing",
)
MANUAL_URL = (_manual_url or "").strip()
if not MANUAL_URL:
    raise RuntimeError(
        "MANUAL_URL is not configured. Set MANUAL_URL env var with ссылка на мануал."
    )
