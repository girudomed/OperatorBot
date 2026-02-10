# Файл: app/utils/error_handlers.py

"""
Централизованные обработчики ошибок и trace propagation.

Важный контракт:
- финальное логирование и user-notify выполняются на верхнем уровне (app.main.telegram_error_handler);
- декораторы ниже только связывают trace-контекст и пробрасывают исключения.
"""

from __future__ import annotations

import asyncio
import functools
import sys
import traceback
from typing import Any, Awaitable, Callable, Dict, Optional, TypeVar, cast

from app.logging_config import (
    bind_trace_id,
    generate_trace_id,
    get_watchdog_logger,
    reset_trace_id,
)
from app.utils.best_effort import best_effort_async, best_effort_sync

logger = get_watchdog_logger(__name__)
F = TypeVar("F", bound=Callable[..., Any])

_loop_exception_handler: Optional[
    Callable[[asyncio.AbstractEventLoop, Dict[str, Any]], None]
] = None


def setup_global_exception_handlers() -> None:
    """Настройка обработчиков необработанных исключений для sync/async кода."""

    global _loop_exception_handler

    def handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, (KeyboardInterrupt, SystemExit)):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return

        logger.error(
            "Необработанное исключение в основном потоке",
            exc_info=(exc_type, exc_value, exc_traceback),
            extra={
                "exception_type": exc_type.__name__,
                "exception_message": str(exc_value),
            },
        )

    sys.excepthook = handle_exception

    def handle_async_exception(loop, context):
        exception = context.get("exception")
        message = context.get("message", "Необработанное исключение в async задаче")

        if exception:
            if isinstance(exception, asyncio.CancelledError):
                logger.info("Async task cancelled during shutdown: %s", message)
                return
            logger.error(
                "Async exception: %s",
                message,
                exc_info=(type(exception), exception, exception.__traceback__),
                extra={
                    "exception_type": type(exception).__name__,
                    "exception_message": str(exception),
                    "context": str(context),
                },
            )
        else:
            logger.error("Async error: %s", message, extra={"context": str(context)})

    _loop_exception_handler = handle_async_exception

    try:
        loop = asyncio.get_event_loop()
        loop.set_exception_handler(handle_async_exception)
    except RuntimeError as exc:
        logger.debug(
            "Не удалось установить обработчик для текущего event loop: %s",
            exc,
            exc_info=True,
        )

    logger.info("Глобальные обработчики исключений установлены")


def install_loop_exception_handler(loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
    if _loop_exception_handler is None:
        return
    target_loop = loop
    if target_loop is None:
        try:
            target_loop = asyncio.get_event_loop()
        except RuntimeError as exc:
            logger.debug(
                "Не удалось получить event loop для установки обработчика: %s",
                exc,
                exc_info=True,
            )
            return
    target_loop.set_exception_handler(_loop_exception_handler)


def log_exceptions(func: F) -> F:
    """Sync decorator: only trace propagation, no final logging/user notify."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        token = bind_trace_id(generate_trace_id("sync"))
        try:
            return func(*args, **kwargs)
        finally:
            reset_trace_id(token)

    return cast(F, wrapper)


def log_async_exceptions(func: F) -> F:
    """Async decorator: only trace propagation, no final logging/user notify."""

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        token = bind_trace_id(generate_trace_id("tg"))
        try:
            return await func(*args, **kwargs)
        finally:
            reset_trace_id(token)

    return cast(F, wrapper)


def safe_execute(func: Callable, *args, **kwargs) -> Optional[Any]:
    """Legacy explicit best-effort wrapper for sync operations."""

    result = best_effort_sync(
        op_name=f"legacy.safe_execute:{getattr(func, '__name__', 'unknown')}",
        fn=func,
        *args,
        on_error_result=None,
        **kwargs,
    )
    return result.value


async def safe_async_execute(coro_func: Callable, *args, **kwargs) -> Optional[Any]:
    """Legacy explicit best-effort wrapper for async operations."""

    result = await best_effort_async(
        op_name=f"legacy.safe_async_execute:{getattr(coro_func, '__name__', 'unknown')}",
        coro=coro_func(*args, **kwargs),
        on_error_result=None,
    )
    return result.value


async def safe_job(job_name: str, job_func: Callable[[], Awaitable[Any]]) -> None:
    """Безопасный запуск фоновой задачи с trace_id."""

    token = bind_trace_id(generate_trace_id(f"job-{job_name}"))
    logger.info("Старт фоновой задачи", extra={"job_name": job_name})
    try:
        await job_func()
        logger.info("Фоновая задача завершена", extra={"job_name": job_name})
    except asyncio.CancelledError:
        logger.warning("Фоновая задача отменена", extra={"job_name": job_name})
        raise
    except Exception as exc:
        logger.error(
            "Ошибка в фоновой задаче",
            exc_info=True,
            extra={
                "job_name": job_name,
                "exception_type": type(exc).__name__,
                "exception_message": str(exc),
            },
        )
    finally:
        reset_trace_id(token)


class ErrorContext:
    """Контекстный менеджер с явным контрактом подавления через reraise=False."""

    def __init__(self, context_name: str, reraise: bool = True, log_level: str = "error"):
        self.context_name = context_name
        self.reraise = reraise
        self.log_level = log_level
        self.logger = get_watchdog_logger(__name__)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if exc_type is not None:
            log_func = getattr(self.logger, self.log_level)
            log_func(
                "Ошибка в контексте: %s",
                self.context_name,
                exc_info=(exc_type, exc_value, exc_traceback),
                extra={
                    "context": self.context_name,
                    "exception_type": exc_type.__name__,
                    "exception_message": str(exc_value),
                    "suppressed": not self.reraise,
                },
            )
            return not self.reraise
        return False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, exc_traceback):
        return self.__exit__(exc_type, exc_value, exc_traceback)


def log_coroutine_exceptions(coro):
    """Оборачивает корутину и логирует её необработанные исключения."""

    async def wrapper():
        try:
            return await coro
        except asyncio.CancelledError:
            logger.debug("Корутина отменена: %s", coro)
            raise
        except Exception as e:
            logger.error(
                "Необработанное исключение в корутине",
                exc_info=True,
                extra={
                    "coroutine": str(coro),
                    "exception_type": type(e).__name__,
                    "exception_message": str(e),
                },
            )
            raise

    return wrapper()


def format_exception_details(exc: Exception) -> dict:
    tb_lines = traceback.format_exception(type(exc), exc, exc.__traceback__)
    return {
        "exception_type": type(exc).__name__,
        "exception_message": str(exc),
        "exception_module": type(exc).__module__,
        "traceback": "".join(tb_lines),
        "traceback_lines": tb_lines,
        "cause": str(exc.__cause__) if exc.__cause__ else None,
        "context": str(exc.__context__) if exc.__context__ else None,
    }


setup_global_exception_handlers()
