"""
Централизованная обработка ошибок для всего приложения.
Включает глобальные обработчики исключений, декораторы и утилиты.

Все логи идут через централизованную систему watch_dog.
"""

import asyncio
import functools
import sys
import traceback
from typing import Any, Callable, Optional, TypeVar, cast

from watch_dog import get_watchdog_logger

# Используем watch_dog для всех логов
logger = get_watchdog_logger(__name__)

# Type variable для правильной типизации декораторов
F = TypeVar('F', bound=Callable[..., Any])


def setup_global_exception_handlers():
    """
    Настройка глобальных обработчиков исключений для sync и async кода.
    """
    # Обработчик необработанных исключений (sync)
    def handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        
        logger.error(
            "Необработанное исключение в основном потоке",
            exc_info=(exc_type, exc_value, exc_traceback),
            extra={
                "exception_type": exc_type.__name__,
                "exception_message": str(exc_value)
            }
        )
    
    sys.excepthook = handle_exception
    
    # Обработчик необработанных исключений в async задачах
    def handle_async_exception(loop, context):
        exception = context.get("exception")
        message = context.get("message", "Необработанное исключение в async задаче")
        
        if exception:
            logger.error(
                f"Async exception: {message}",
                exc_info=(type(exception), exception, exception.__traceback__),
                extra={
                    "exception_type": type(exception).__name__,
                    "exception_message": str(exception),
                    "context": str(context)
                }
            )
        else:
            logger.error(
                f"Async error: {message}",
                extra={"context": str(context)}
            )
    
    # Устанавливаем обработчик для текущего event loop
    try:
        loop = asyncio.get_event_loop()
        loop.set_exception_handler(handle_async_exception)
    except RuntimeError:
        # Event loop еще не создан, установим позже
        pass
    
    logger.info("Глобальные обработчики исключений установлены")


def log_exceptions(func: F) -> F:
    """
    Декоратор для автоматического логирования исключений в sync функциях.
    
    Пример:
        @log_exceptions
        def my_function():
            raise ValueError("Ошибка")
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(
                f"Ошибка в {func.__module__}.{func.__name__}",
                exc_info=True,
                extra={
                    "function": func.__name__,
                    "module": func.__module__,
                    "exception_type": type(e).__name__,
                    "exception_message": str(e),
                    "args": str(args)[:200],
                    "kwargs": str(kwargs)[:200]
                }
            )
            raise
    
    return cast(F, wrapper)


def log_async_exceptions(func: F) -> F:
    """
    Декоратор для автоматического логирования исключений в async функциях.
    
    Пример:
        @log_async_exceptions
        async def my_async_function():
            raise ValueError("Ошибка")
    """
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except asyncio.CancelledError:
            # CancelledError - это нормальное поведение, не логируем как ошибку
            logger.debug(
                f"Задача отменена: {func.__module__}.{func.__name__}",
                extra={
                    "function": func.__name__,
                    "module": func.__module__
                }
            )
            raise
        except Exception as e:
            logger.error(
                f"Ошибка в async {func.__module__}.{func.__name__}",
                exc_info=True,
                extra={
                    "function": func.__name__,
                    "module": func.__module__,
                    "exception_type": type(e).__name__,
                    "exception_message": str(e),
                    "args": str(args)[:200],
                    "kwargs": str(kwargs)[:200]
                }
            )
            raise
    
    return cast(F, wrapper)


def safe_execute(func: Callable, *args, **kwargs) -> Optional[Any]:
    """
    Безопасное выполнение функции с логированием ошибок.
    Возвращает None в случае ошибки вместо propagation.
    
    Использование:
        result = safe_execute(risky_function, arg1, arg2, key=value)
    """
    try:
        return func(*args, **kwargs)
    except Exception as e:
        logger.error(
            f"Ошибка при выполнении {func.__name__}",
            exc_info=True,
            extra={
                "function": func.__name__,
                "exception_type": type(e).__name__,
                "exception_message": str(e)
            }
        )
        return None


async def safe_async_execute(coro_func: Callable, *args, **kwargs) -> Optional[Any]:
    """
    Безопасное выполнение async функции с логированием ошибок.
    Возвращает None в случае ошибки.
    
    Использование:
        result = await safe_async_execute(async_risky_function, arg1, arg2)
    """
    try:
        return await coro_func(*args, **kwargs)
    except asyncio.CancelledError:
        logger.debug(f"Задача отменена: {coro_func.__name__}")
        return None
    except Exception as e:
        logger.error(
            f"Ошибка при выполнении async {coro_func.__name__}",
            exc_info=True,
            extra={
                "function": coro_func.__name__,
                "exception_type": type(e).__name__,
                "exception_message": str(e)
            }
        )
        return None


class ErrorContext:
    """
    Контекстный менеджер для автоматического логирования ошибок.
    
    Использование:
        with ErrorContext("Инициализация БД"):
            db.connect()
            
        async with ErrorContext("Запрос к API", reraise=False):
            await api.call()
    """
    
    def __init__(self, context_name: str, reraise: bool = True, log_level: str = "error"):
        self.context_name = context_name
        self.reraise = reraise
        self.log_level = log_level
        # Используем watch_dog logger
        self.logger = get_watchdog_logger(__name__)
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, exc_traceback):
        if exc_type is not None:
            log_func = getattr(self.logger, self.log_level)
            log_func(
                f"Ошибка в контексте: {self.context_name}",
                exc_info=(exc_type, exc_value, exc_traceback),
                extra={
                    "context": self.context_name,
                    "exception_type": exc_type.__name__,
                    "exception_message": str(exc_value)
                }
            )
            
            # Если reraise=False, подавляем исключение
            return not self.reraise
        return False
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_value, exc_traceback):
        return self.__exit__(exc_type, exc_value, exc_traceback)


def log_coroutine_exceptions(coro):
    """
    Оборачивает корутину для автоматического логирования исключений.
    
    Использование:
        task = asyncio.create_task(log_coroutine_exceptions(my_coro()))
    """
    async def wrapper():
        try:
            return await coro
        except asyncio.CancelledError:
            logger.debug(f"Корутина отменена: {coro}")
            raise
        except Exception as e:
            logger.error(
                f"Необработанное исключение в корутине",
                exc_info=True,
                extra={
                    "coroutine": str(coro),
                    "exception_type": type(e).__name__,
                    "exception_message": str(e)
                }
            )
            raise
    
    return wrapper()


def format_exception_details(exc: Exception) -> dict:
    """
    Форматирует детали исключения для структурированного логирования.
    
    Возвращает словарь с полной информацией об ошибке.
    """
    tb_lines = traceback.format_exception(type(exc), exc, exc.__traceback__)
    
    return {
        "exception_type": type(exc).__name__,
        "exception_message": str(exc),
        "exception_module": type(exc).__module__,
        "traceback": "".join(tb_lines),
        "traceback_lines": tb_lines,
        "cause": str(exc.__cause__) if exc.__cause__ else None,
        "context": str(exc.__context__) if exc.__context__ else None
    }


# Автоматическая установка обработчиков при импорте модуля
setup_global_exception_handlers()
