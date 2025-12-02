# watch_dog/config.py
import os
import logging

# Уровни логирования
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Пути к логам
LOG_DIR = "logs"
MAIN_LOG_FILE = "operabot.log"
ERROR_LOG_FILE = "errors.log"

# Настройки ротации
MAX_BYTES = 10 * 1024 * 1024  # 10 MB
BACKUP_COUNT = 5

# Чувствительные ключи для маскировки (из переменных окружения)
SENSITIVE_KEYS = [
    "TELEGRAM_BOT_TOKEN",
    "OPENAI_API_KEY",
    "DB_PASSWORD",
    "ADMIN_PASSWORD"
]
