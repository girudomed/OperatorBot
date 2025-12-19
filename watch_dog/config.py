# watch_dog/config.py
import os
import logging

# Уровни логирования
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Пути к логам (по умолчанию вне рабочего каталога контейнера)
LOG_DIR = os.getenv("LOG_DIR", "/var/log/operabot")
MAIN_LOG_FILE = "operabot.log"
ERROR_LOG_FILE = "errors.log"

# Настройки ротации
MAX_BYTES = int(os.getenv("LOG_MAX_BYTES", str(500 * 1024 * 1024)))  # 500 MB
BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", "1"))

# Чувствительные ключи для маскировки (из переменных окружения)
SENSITIVE_KEYS = [
    "TELEGRAM_BOT_TOKEN",
    "OPENAI_API_KEY",
    "DB_PASSWORD",
    "ADMIN_PASSWORD"
]
