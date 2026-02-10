"""
Модуль настройки логирования приложения.

Использует watch_dog для централизованного логирования с маскировкой секретов.
Дополнительно внедряет trace_id через ContextVar, чтобы связывать логи между слоями.
"""

from __future__ import annotations

import contextvars
import logging
import uuid
from typing import Optional

from watch_dog import setup_watchdog, get_watchdog_logger


_TRACE_ID_VAR: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "trace_id",
    default=None,
)


class _TraceIdFilter(logging.Filter):
    """Добавляет trace_id в записи логов, чтобы его не приходилось проставлять вручную."""

    def filter(self, record: logging.LogRecord) -> bool:  # pragma: no cover - простая вставка
        record.trace_id = _TRACE_ID_VAR.get() or "-"
        return True


def is_polling_noise_record(record: logging.LogRecord) -> bool:
    """Определяет шумный polling traceback от PTB Updater."""
    try:
        if not record:
            return False
        logger_name = record.name or ""
        message = (record.getMessage() or "").lower()
    except Exception:
        return False
    if logger_name.startswith("telegram.ext.Updater") and "exception happened while polling for updates." in message:
        return True
    if logger_name.startswith("telegram.ext.Updater") and "self.gen.throw(typ, value, traceback)" in message:
        return True
    if "telegram/ext/_utils/networkloop.py" in message and "self.gen.throw" in message:
        return True
    return False


class _PollingNoiseFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:  # pragma: no cover - простая вставка
        return not is_polling_noise_record(record)


def install_polling_noise_filter() -> None:
    """Устанавливает fail-safe фильтр polling-noise на logger и root handlers."""
    noise_logger = logging.getLogger("telegram.ext.Updater")
    if not any(isinstance(f, _PollingNoiseFilter) for f in noise_logger.filters):
        noise_logger.addFilter(_PollingNoiseFilter())

    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        if not any(isinstance(f, _PollingNoiseFilter) for f in handler.filters):
            handler.addFilter(_PollingNoiseFilter())


def generate_trace_id(prefix: str = "req") -> str:
    """Генерирует короткий trace_id вида req-1a2b3c4d."""
    suffix = uuid.uuid4().hex[:8]
    return f"{prefix}-{suffix}" if prefix else suffix


def bind_trace_id(trace_id: Optional[str] = None) -> contextvars.Token:
    """Устанавливает trace_id в контекст и возвращает token для отката."""
    if trace_id is None:
        trace_id = generate_trace_id()
    return _TRACE_ID_VAR.set(trace_id)


def reset_trace_id(token: contextvars.Token) -> None:
    """Сбрасывает trace_id к предыдущему значению."""
    _TRACE_ID_VAR.reset(token)


def get_trace_id() -> Optional[str]:
    """Возвращает текущий trace_id из контекста."""
    return _TRACE_ID_VAR.get()


def setup_app_logging() -> logging.Logger:
    """
    Настраивает централизованное логирование приложения через watch_dog.
    
    Returns:
        logging.Logger: Настроенный корневой логгер
    """
    setup_watchdog()
    root_logger = logging.getLogger()
    # Гарантируем, что filter не добавляется многократно (watch_dog может пересоздавать root).
    if not any(isinstance(f, _TraceIdFilter) for f in root_logger.filters):
        root_logger.addFilter(_TraceIdFilter())
    install_polling_noise_filter()
    return get_watchdog_logger("app")


# Глобальный логгер для использования в модуле
logger = setup_app_logging()

__all__ = [
    "logger",
    "get_watchdog_logger",
    "setup_app_logging",
    "generate_trace_id",
    "bind_trace_id",
    "reset_trace_id",
    "get_trace_id",
    "is_polling_noise_record",
    "install_polling_noise_filter",
]
