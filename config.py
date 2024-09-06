import os
from dotenv import load_dotenv

# Загрузка переменных окружения из файла .env, если он есть
load_dotenv()

from logger_utils import setup_logging

logger = setup_logging()

def some_function():
    logger.info("Функция some_function начала работу.")
    # Логика функции
    try:
        # Некоторый код
        logger.info("Успешное выполнение.")
    except Exception as e:
        logger.error(f"Произошла ошибка: {e}")


# Проверка наличия критически важных переменных окружения
required_env_vars = ["OPENAI_API_KEY", "TELEGRAM_BOT_TOKEN", "DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME"]
missing_vars = [var for var in required_env_vars if os.getenv(var) is None]

if missing_vars:
    raise EnvironmentError(f"Отсутствуют необходимые переменные окружения: {', '.join(missing_vars)}")

# Конфигурация для подключения к OpenAI API
openai_api_key = os.getenv("OPENAI_API_KEY")
openai_api_base = os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1")
telegram_token = os.getenv('TELEGRAM_BOT_TOKEN')

# Конфигурация для подключения к базе данных MySQL
db_config = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", "password"),
    "db": os.getenv("DB_NAME", "call_center_db"),
    "port": int(os.getenv("DB_PORT", 3306)),
}

# Параметры OpenAI Completion
openai_completion_options = {
    "temperature": 0.7,
    "max_tokens": 1000,
    "top_p": 1,
    "frequency_penalty": 0,
    "presence_penalty": 0,
    "request_timeout": 60.0,
}

# Логирование
logging_config = {
    "level": os.getenv("LOG_LEVEL", "INFO"),
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "datefmt": "%Y-%m-%d %H:%M:%S",
}

# Функция для инициализации логирования
def init_logging():
    import logging
    logging.basicConfig(level=logging_config["level"], format=logging_config["format"], datefmt=logging_config["datefmt"])
    logger = logging.getLogger(__name__)
    return logger

# Инициализация логера
logger = init_logging()

# Пример использования logger:
logger.info("Конфигурация загружена успешно")
