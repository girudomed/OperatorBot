# watch_dog/config.py
import os
import logging

# Уровни логирования
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
LOG_CAPTURE_STDOUT = os.getenv("LOG_CAPTURE_STDOUT", "false").lower() in ("1", "true", "yes", "on")

# Пути к логам (по умолчанию вне рабочего каталога контейнера)
LOG_DIR = os.getenv("LOG_DIR", "/var/log/operabot")
MAIN_LOG_FILE = "operabot.log"
ERROR_LOG_FILE = "errors.log"

# Настройки ротации
# 20 MB по умолчанию, чтобы ротация срабатывала заметно раньше и
# в боте не приходили «огромные смешанные» логи.
MAX_BYTES = int(os.getenv("LOG_MAX_BYTES", str(20 * 1024 * 1024)))  # 20 MB
BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", "5"))

# Чувствительные ключи для маскировки (из переменных окружения)
SENSITIVE_KEYS = [
    "TELEGRAM_BOT_TOKEN",
    "OPENAI_API_KEY",
    "DB_PASSWORD",
    "ADMIN_PASSWORD"
]
