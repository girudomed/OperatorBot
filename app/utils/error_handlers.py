# Файл: app/utils/error_handlers.py

"""
Централизованная обработка ошибок для всего приложения.
Включает глобальные обработчики исключений, декораторы и утилиты.

Все логи идут через централизованную систему watch_dog.
"""

import asyncio
import functools
import sys
import traceback
from itertools import chain
from typing import Any, Callable, Dict, Optional, TypeVar, cast

from telegram import Update
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
    except RuntimeError as exc:
        # Event loop еще не создан, установим позже
        logger.debug(
            "Не удалось установить обработчик для текущего event loop: %s",
            exc,
            exc_info=True,
        )
    
logger.info("Глобальные обработчики исключений установлены")


def _find_update_in_args(args: tuple, kwargs: dict) -> Optional[Update]:
    for value in chain(args, kwargs.values()):
        if isinstance(value, Update):
            return value
    return None


async def _resolve_user_role(handler_instance: Any, update: Optional[Update]) -> Optional[str]:
    if not update or not getattr(update, "effective_user", None):
        return None
    permissions = getattr(handler_instance, "permissions", None)
    if not permissions or not hasattr(permissions, "get_effective_role"):
        return None
    user = update.effective_user
    try:
        return await permissions.get_effective_role(user.id, user.username)
    except Exception as exc:
        logger.debug(
            "Не удалось определить роль пользователя %s: %s",
            user.id,
            exc,
            exc_info=True,
        )
        return None


def _build_update_context(update: Optional[Update]) -> Dict[str, Any]:
    context: Dict[str, Any] = {}
    if not update:
        return context

    user = update.effective_user
    chat = update.effective_chat

    if user:
        context["user_id"] = user.id
        if user.username:
            context["username"] = user.username
        if user.full_name:
            context["full_name"] = user.full_name
    if chat:
        context["chat_id"] = chat.id

    handler_type = "update"
    command_or_callback = None

    if update.callback_query:
        handler_type = "callback_query"
        command_or_callback = update.callback_query.data
    elif update.message:
        text = update.message.text or update.message.caption or ""
        handler_type = "command" if text.startswith("/") else "message"
        command_or_callback = text

    context["handler_type"] = handler_type
    if command_or_callback:
        context["command_or_callback"] = command_or_callback
    return context


def _classify_business_error(error: Exception) -> str:
    message = str(error).lower()
    if "unknown column" in message or "unknown table" in message:
        return "db_schema_error"
    if isinstance(error, PermissionError) or "permission" in message:
        return "permission_error"
    if "timeout" in message:
        return "timeout_error"
    return "unexpected_error"


async def _notify_user_about_error(update: Optional[Update], category: str) -> None:
    if not update:
        return
    default_message = "⚠️ Произошла ошибка. Повторите действие или обратитесь к разработчику."
    if category == "db_schema_error":
        default_message = "⚠️ Ошибка БД, обратитесь к разработчику."
    try:
        if update.callback_query:
            try:
                await update.callback_query.answer(default_message, show_alert=True)
            except Exception as exc:
                logger.debug(
                    "Не удалось отправить alert в callback_query: %s",
                    exc,
                    exc_info=True,
                )
            try:
                await update.callback_query.message.reply_text(default_message)
            except Exception as exc:
                logger.debug(
                    "Не удалось отправить reply_text по callback_query: %s",
                    exc,
                    exc_info=True,
                )
        elif update.message:
            await update.message.reply_text(default_message)
    except Exception as exc:
        logger.warning(
            "Не удалось отправить уведомление об ошибке пользователю: %s",
            exc,
            exc_info=True,
        )


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
            update = _find_update_in_args(args, kwargs)
            context = _build_update_context(update)
            error_category = _classify_business_error(e)
            base_message = f"Ошибка в {func.__module__}.{func.__name__}"
            if context.get("command_or_callback"):
                base_message = (
                    f"Ошибка при выполнении {context.get('handler_type')} "
                    f"{context.get('command_or_callback')}"
                )
            logger.error(
                base_message,
                exc_info=True,
                extra={
                    "function": func.__name__,
                    "module_name": func.__module__,
                    "exception_type": type(e).__name__,
                    "exception_message": str(e),
                    "error_category": error_category,
                    **context,
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
                    "module_name": func.__module__
                }
            )
            raise
        except Exception as e:
            update = _find_update_in_args(args, kwargs)
            context = _build_update_context(update)
            error_category = _classify_business_error(e)
            handler_instance = args[0] if args else None
            if handler_instance:
                role = await _resolve_user_role(handler_instance, update)
                if role:
                    context["user_role"] = role
            base_message = f"Ошибка в async {func.__module__}.{func.__name__}"
            if context.get("command_or_callback"):
                base_message = (
                    f"Ошибка при выполнении {context.get('handler_type')} "
                    f"{context.get('command_or_callback')}"
                )
            logger.error(
                base_message,
                exc_info=True,
                extra={
                    "function": func.__name__,
                    "module_name": func.__module__,
                    "exception_type": type(e).__name__,
                    "exception_message": str(e),
                    "error_category": error_category,
                    **context,
                }
            )
            await _notify_user_about_error(update, error_category)
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
