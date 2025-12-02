# watch_dog/logger.py
import logging
import os
import sys
from logging.handlers import RotatingFileHandler

from .config import (
    LOG_DIR, MAIN_LOG_FILE, ERROR_LOG_FILE, 
    LOG_LEVEL, LOG_FORMAT, LOG_DATE_FORMAT, 
    MAX_BYTES, BACKUP_COUNT
)
from .filters import SensitiveDataFilter

def setup_watchdog():
    """
    Настраивает глобальную конфигурацию логирования.
    Должна вызываться один раз при старте приложения.
    """
    # Создаем директорию для логов
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

    # Корневой логгер
    root_logger = logging.getLogger()
    root_logger.setLevel(LOG_LEVEL)
    
    # Очищаем существующие хендлеры
    root_logger.handlers = []

    # Форматтер
    formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)
    
    # Фильтр чувствительных данных
    sensitive_filter = SensitiveDataFilter()

    # 1. Console Handler (stdout)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.addFilter(sensitive_filter)
    root_logger.addHandler(console_handler)

    # 2. File Handler (Main Log)
    main_log_path = os.path.join(LOG_DIR, MAIN_LOG_FILE)
    file_handler = RotatingFileHandler(
        main_log_path, 
        maxBytes=MAX_BYTES, 
        backupCount=BACKUP_COUNT, 
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    file_handler.addFilter(sensitive_filter)
    root_logger.addHandler(file_handler)

    # 3. Error File Handler (Errors only)
    error_log_path = os.path.join(LOG_DIR, ERROR_LOG_FILE)
    error_handler = RotatingFileHandler(
        error_log_path, 
        maxBytes=MAX_BYTES, 
        backupCount=BACKUP_COUNT, 
        encoding='utf-8'
    )
    error_handler.setFormatter(formatter)
    error_handler.setLevel(logging.ERROR)
    error_handler.addFilter(sensitive_filter)
    root_logger.addHandler(error_handler)
    
    # Отключаем шумные библиотеки
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("aiomysql").setLevel(logging.INFO)
    logging.getLogger("apscheduler").setLevel(logging.INFO)

    logging.info(f"WatchDog Logger initialized. Level: {LOG_LEVEL}")


def get_watchdog_logger(name: str) -> logging.Logger:
    """
    Возвращает настроенный логгер для модуля.
    """
    return logging.getLogger(name)
