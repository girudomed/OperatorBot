# watch_dog/logger.py
import logging
import os
import sys
import re
from datetime import datetime
from zoneinfo import ZoneInfo
from logging.handlers import RotatingFileHandler

from .config import (
    LOG_DIR, MAIN_LOG_FILE, ERROR_LOG_FILE, 
    LOG_LEVEL, LOG_FORMAT, LOG_DATE_FORMAT, 
    MAX_BYTES, BACKUP_COUNT, LOG_CAPTURE_STDOUT
)
from .filters import SensitiveDataFilter


class _StreamToLogger:
    _reentry_guard = False

    def __init__(self, logger: logging.Logger, level: int):
        self.logger = logger
        self.level = level

    def write(self, buf):
        if not buf:
            return
        if _StreamToLogger._reentry_guard:
            return
        text = buf.rstrip()
        if not text:
            return
        try:
            _StreamToLogger._reentry_guard = True
            self.logger.log(self.level, text)
        finally:
            _StreamToLogger._reentry_guard = False

    def flush(self):
        return


class _TraceTimestampFormatter(logging.Formatter):
    """
    Добавляет timestamp на каждую строку многострочного сообщения/traceback.
    Это упрощает разбор инцидентов в длинных полотнах лога.
    """

    _TS_PREFIX_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")

    def format(self, record: logging.LogRecord) -> str:
        rendered = super().format(record)
        if "\n" not in rendered:
            return rendered

        ts = self.formatTime(record, self.datefmt)
        lines = rendered.splitlines()
        normalized = [lines[0]]
        for line in lines[1:]:
            if not line:
                normalized.append(line)
                continue
            if self._TS_PREFIX_RE.match(line):
                normalized.append(line)
                continue
            normalized.append(f"{ts} | {line}")
        return "\n".join(normalized)


def setup_watchdog():
    """
    Настраивает глобальную конфигурацию логирования.
    Должна вызываться один раз при старте приложения.
    """
    # Создаем директорию для логов
    log_dir = LOG_DIR
    try:
        os.makedirs(log_dir, exist_ok=True)
    except PermissionError:
        fallback_dir = os.path.join(os.getcwd(), "logs")
        os.makedirs(fallback_dir, exist_ok=True)
        log_dir = fallback_dir
        logging.warning(
            "Не удалось создать каталог логов %s, используем fallback %s",
            LOG_DIR,
            fallback_dir,
        )

    # Корневой логгер
    root_logger = logging.getLogger()
    root_logger.setLevel(LOG_LEVEL)
    
    # Очищаем существующие хендлеры
    root_logger.handlers = []

    # Форматтер
    formatter = _TraceTimestampFormatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)
    formatter.converter = lambda *args: datetime.now(ZoneInfo("Europe/Moscow")).timetuple()
    
    # Фильтр чувствительных данных
    sensitive_filter = SensitiveDataFilter()

    # 1. Console Handler (stdout)
    console_handler = logging.StreamHandler(sys.__stdout__)
    console_handler.setFormatter(formatter)
    console_handler.addFilter(sensitive_filter)
    root_logger.addHandler(console_handler)

    # 1.1 Error Console Handler (stderr)
    error_console_handler = logging.StreamHandler(sys.__stderr__)
    error_console_handler.setLevel(logging.ERROR)
    error_console_handler.setFormatter(formatter)
    error_console_handler.addFilter(sensitive_filter)
    root_logger.addHandler(error_console_handler)

    # 2. File Handler (Main Log)
    main_log_path = os.path.join(log_dir, MAIN_LOG_FILE)
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
    error_log_path = os.path.join(log_dir, ERROR_LOG_FILE)
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

    if LOG_CAPTURE_STDOUT:
        sys.stdout = _StreamToLogger(logging.getLogger("stdout"), logging.INFO)
        sys.stderr = _StreamToLogger(logging.getLogger("stderr"), logging.ERROR)
    
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
