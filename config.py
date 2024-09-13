import os
from dotenv import load_dotenv
import logging
from logging.handlers import RotatingFileHandler
import time

# Загрузка переменных окружения из файла .env
load_dotenv()

# Настройка логирования с ротацией файлов
def setup_logging(log_file="logs/logs.log", log_level="INFO"):
    """
    Инициализация системы логирования для проекта.
    Логи пишутся в файл с ротацией и выводятся в консоль.
    """
    log_level = os.getenv("LOG_LEVEL", log_level).upper()
    log_file = os.getenv("LOG_FILE", log_file)

    # Проверка и создание директории для логов
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir)
            print(f"Создана директория для логов: {log_dir}")
        except Exception as e:
            print(f"Ошибка при создании директории для логов: {e}")
            raise

    # Создание логгера с ротацией
    rotating_handler = RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=3)  # 5MB на файл
    rotating_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            rotating_handler,  # Логи в файл с ротацией
            logging.StreamHandler()  # Логи в консоль
        ]
    )
    logger = logging.getLogger(__name__)
    logger.info(f"Логирование инициализировано. Логи сохраняются в файл: {log_file}")
    return logger

# Инициализация логгера
logger = setup_logging()

# Функция для проверки переменных окружения
def check_required_env_vars(required_vars):
    """
    Проверка наличия обязательных переменных окружения.
    Если какая-то переменная отсутствует, поднимается исключение.
    """
    missing_vars = [var for var in required_vars if not os.getenv(var) or os.getenv(var).strip() == ""]
    if missing_vars:
        logger.error(f"Отсутствуют необходимые переменные окружения: {', '.join(missing_vars)}")
        raise EnvironmentError(f"Отсутствуют необходимые переменные окружения: {', '.join(missing_vars)}")

# Переменные, которые обязательно должны быть в окружении
required_env_vars = [
    "OPENAI_API_KEY", "TELEGRAM_BOT_TOKEN", "DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME", "DB_PORT"
]

# Засекаем время загрузки конфигурации
start_time = time.time()

try:
    check_required_env_vars(required_env_vars)
except EnvironmentError as e:
    logger.critical(f"Ошибка при проверке переменных окружения: {e}")
    raise

# Конфигурация OpenAI
openai_api_key = os.getenv("OPENAI_API_KEY")
openai_api_base = os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1")

# Параметры OpenAI Completion
openai_completion_options = {
    "temperature": float(os.getenv("OPENAI_TEMPERATURE", 0.7)),
    "max_tokens": int(os.getenv("OPENAI_MAX_TOKENS", 1000)),
    "top_p": float(os.getenv("OPENAI_TOP_P", 1)),
    "frequency_penalty": float(os.getenv("OPENAI_FREQUENCY_PENALTY", 0)),
    "presence_penalty": float(os.getenv("OPENAI_PRESENCE_PENALTY", 0)),
    "request_timeout": float(os.getenv("OPENAI_REQUEST_TIMEOUT", 60.0)),
}

# Логирование параметров OpenAI для отладки
logger.debug(f"OpenAI Completion options: {openai_completion_options}")

# Конфигурация Telegram
telegram_token = os.getenv('TELEGRAM_BOT_TOKEN')

# Проверка наличия Telegram токена
if not telegram_token:
    logger.error("Telegram Bot Token не найден в переменных окружения.")
    raise EnvironmentError("Telegram Bot Token не найден.")

# Конфигурация базы данных
db_config = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "db": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT")),  # Добавляем значение по умолчанию для порта
}

# Логирование конфигурации базы данных (без пароля для безопасности)
logger.debug(f"Конфигурация базы данных: host={db_config['host']}, user={db_config['user']}, db={db_config['db']}, port={db_config['port']}")

# Проверка конфигурации базы данных
if not all(db_config[key] for key in ["host", "user", "password", "db", "port"]):
    logger.error("Конфигурация базы данных неполная.")
    raise EnvironmentError("Проблема с конфигурацией базы данных.")

# Засекаем время завершения загрузки конфигурации
elapsed_time = time.time() - start_time
logger.info(f"Конфигурация загружена успешно за {elapsed_time:.2f} секунд.")
