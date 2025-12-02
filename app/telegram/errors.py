# bot/services/errors.py
import asyncio
import json
import logging
from datetime import datetime
from enum import Enum
from functools import wraps
from typing import Any, Callable, Dict, Optional, Tuple, Type, Union

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, CallbackQuery
from telegram.ext import CallbackContext

from watch_dog import get_watchdog_logger

logger = get_watchdog_logger(__name__)


class ErrorSeverity(Enum):
    """Уровни серьезности ошибок."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class ErrorContext:
    """Контекст ошибки для расширенной обработки."""

    def __init__(
        self,
        error: Exception,
        severity: ErrorSeverity,
        user_id: Union[int, str],
        function_name: str,
        additional_data: Dict[str, Any] = None,
    ):
        self.error = error
        self.severity = severity
        self.user_id = user_id
        self.function_name = function_name
        self.timestamp = datetime.now()
        self.additional_data = additional_data or {}

    def to_dict(self) -> Dict[str, Any]:
        """Преобразование контекста в словарь для логирования."""
        return {
            "error_type": self.error.__class__.__name__,
            "error_message": str(self.error),
            "severity": self.severity.value,
            "user_id": self.user_id,
            "function": self.function_name,
            "timestamp": self.timestamp.isoformat(),
            "additional_data": self.additional_data,
        }


class BotError(Exception):
    """Базовый класс для ошибок бота."""

    def __init__(
        self,
        message: str,
        user_message: str = None,
        severity: ErrorSeverity = ErrorSeverity.ERROR,
        details: Dict[str, Any] = None,
        retry_allowed: bool = True,
    ):
        super().__init__(message)
        self.user_message = user_message or message
        self.severity = severity
        self.details = details or {}
        self.retry_allowed = retry_allowed
        self.timestamp = datetime.now()

    def get_user_message(self, include_details: bool = True) -> str:
        """Формирует сообщение для пользователя."""
        message = self.user_message
        if include_details and self.details:
            message += "\n\nПодробности:\n"
            for key, value in self.details.items():
                message += f"• {key}: {value}\n"
        return message


class RetryableError(BotError):
    """Ошибка, которую можно повторить."""

    def __init__(
        self,
        message: str,
        user_message: str = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        **kwargs,
    ):
        super().__init__(message, user_message, **kwargs)
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.retry_count = 0


class RateLimitError(RetryableError):
    """Ошибка превышения лимита запросов."""

    def __init__(self, message: str, reset_time: datetime = None, **kwargs):
        super().__init__(message, **kwargs)
        self.reset_time = reset_time

    def get_user_message(self, include_details: bool = True) -> str:
        message = super().get_user_message(include_details)
        if self.reset_time:
            wait_time = (self.reset_time - datetime.now()).total_seconds()
            if wait_time > 0:
                message += f"\n\nПопробуйте снова через {int(wait_time)} секунд."
        return message


# Определяем исключения, которые могут возникнуть
class AuthenticationError(BotError): pass
class PermissionError(BotError): pass
class ValidationError(BotError): pass
class DataProcessingError(BotError): pass
class VisualizationError(BotError): pass
class ExternalServiceError(BotError): pass


class ErrorHandler:
    """Класс для централизованной обработки ошибок."""

    def __init__(self, bot_instance):
        self.bot = bot_instance
        self.logger = logger
        self._error_configs = self._get_default_error_configs()
        self._notification_rules = self._get_default_notification_rules()
        self._retry_policies = self._get_default_retry_policies()

    @property
    def error_configs(self) -> Dict[Type[Exception], Dict[str, Any]]:
        return self._error_configs

    @property
    def notification_rules(self) -> Dict[ErrorSeverity, Dict[str, Any]]:
        return self._notification_rules

    @property
    def retry_policies(self) -> Dict[Type[Exception], Dict[str, Any]]:
        return self._retry_policies

    def _get_default_error_configs(self) -> Dict[Type[Exception], Dict[str, Any]]:
        return {
            AuthenticationError: {
                "message": "🔒 Ошибка аутентификации",
                "severity": ErrorSeverity.WARNING,
                "log_level": "warning",
                "retry_count": 0,
                "notify_admin": False,
            },
            PermissionError: {
                "message": "🚫 Недостаточно прав",
                "severity": ErrorSeverity.WARNING,
                "log_level": "warning",
                "retry_count": 0,
                "notify_admin": False,
            },
            ValidationError: {
                "message": "⚠️ Некорректные данные",
                "severity": ErrorSeverity.WARNING,
                "log_level": "warning",
                "retry_count": 0,
                "notify_admin": False,
            },
            DataProcessingError: {
                "message": "🔄 Ошибка обработки данных",
                "severity": ErrorSeverity.ERROR,
                "log_level": "error",
                "retry_count": 2,
                "notify_admin": True,
            },
            VisualizationError: {
                "message": "📊 Ошибка создания графика",
                "severity": ErrorSeverity.ERROR,
                "log_level": "error",
                "retry_count": 1,
                "notify_admin": True,
            },
            RateLimitError: {
                "message": "⏳ Превышен лимит запросов",
                "severity": ErrorSeverity.INFO,
                "log_level": "info",
                "retry_count": 3,
                "retry_delay": 5.0,
                "notify_admin": False,
            },
            ExternalServiceError: {
                "message": "🌐 Ошибка внешнего сервиса",
                "severity": ErrorSeverity.ERROR,
                "log_level": "error",
                "retry_count": 2,
                "notify_admin": True,
            },
        }

    def _get_default_notification_rules(self) -> Dict[ErrorSeverity, Dict[str, Any]]:
        return {
            ErrorSeverity.DEBUG: {"notify_admin": False, "notification_format": "simple"},
            ErrorSeverity.INFO: {"notify_admin": False, "notification_format": "simple"},
            ErrorSeverity.WARNING: {"notify_admin": False, "notification_format": "detailed"},
            ErrorSeverity.ERROR: {"notify_admin": True, "notification_format": "detailed"},
            ErrorSeverity.CRITICAL: {"notify_admin": True, "notification_format": "full"},
        }

    def _get_default_retry_policies(self) -> Dict[Type[Exception], Dict[str, Any]]:
        return {
            RateLimitError: {
                "max_retries": 3,
                "base_delay": 5.0,
                "max_delay": 30.0,
                "exponential_backoff": True,
            },
            DataProcessingError: {
                "max_retries": 2,
                "base_delay": 1.0,
                "max_delay": 5.0,
                "exponential_backoff": False,
            },
            ExternalServiceError: {
                "max_retries": 2,
                "base_delay": 2.0,
                "max_delay": 10.0,
                "exponential_backoff": True,
            },
        }

    def update_error_config(self, error_type: Type[Exception], config: Dict[str, Any]) -> None:
        if error_type in self.error_configs:
            self.error_configs[error_type].update(config)
        else:
            self.error_configs[error_type] = config

    def get_error_config(self, error: Exception) -> Dict[str, Any]:
        error_type = type(error)
        if error_type in self.error_configs:
            return self.error_configs[error_type]
        for err_type, config in self.error_configs.items():
            if isinstance(error, err_type):
                return config
        return {
            "message": "❌ Произошла ошибка",
            "severity": ErrorSeverity.ERROR,
            "log_level": "error",
            "retry_count": 0,
            "notify_admin": True,
        }

    async def handle_error(self, error: Exception, context: Dict[str, Any]) -> Tuple[str, bool]:
        logger.info("Начало обработки ошибки.")
        logger.debug(f"Ошибка: {error}")
        
        try:
            config = self.get_error_config(error)
            severity = config.get("severity", ErrorSeverity.ERROR)
            
            error_context = ErrorContext(
                error=error,
                severity=severity,
                user_id=context.get("user_id", "Unknown"),
                function_name=context.get("function_name", "Unknown"),
                additional_data=context,
            )

            self._log_error(error_context, config)

            if config.get("notify_admin", False) or self.notification_rules[severity]["notify_admin"]:
                await self._notify_admin(error_context)

            user_message = self._format_user_message(error, config)
            return user_message, True

        except Exception as handling_error:
            logger.error("Ошибка при обработке исключения.", exc_info=True)
            return "Произошла непредвиденная ошибка. Попробуйте позже.", False

    def _log_error(self, error_context: ErrorContext, config: Dict[str, Any]) -> None:
        log_level = config["log_level"]
        log_message = json.dumps(error_context.to_dict(), indent=2)
        
        if hasattr(self.logger, log_level):
            getattr(self.logger, log_level)(log_message, exc_info=True)
        else:
            self.logger.error(log_message, exc_info=True)

    async def _notify_admin(self, error_context: ErrorContext) -> None:
        # Здесь должна быть логика уведомления админа
        # Поскольку bot_instance передан, можно использовать его методы
        if hasattr(self.bot, 'notify_admin'):
            await self.bot.notify_admin(f"🚨 Error: {error_context.error}")

    def _format_user_message(self, error: Exception, config: Dict[str, Any]) -> str:
        if isinstance(error, BotError):
            message = error.get_user_message()
        else:
            message = config["message"]

        if isinstance(error, RetryableError):
            message += f"\n\nПопытка {error.retry_count + 1}/{error.max_retries}"

        if isinstance(error, RateLimitError) and error.reset_time:
            wait_time = (error.reset_time - datetime.now()).total_seconds()
            if wait_time > 0:
                message += f"\n\nПопробуйте снова через {int(wait_time)} секунд"

        return message

    def get_retry_policy(self, error: Exception) -> Dict[str, Any]:
        error_type = type(error)
        if error_type in self.retry_policies:
            return self.retry_policies[error_type]
        for err_type, policy in self.retry_policies.items():
            if isinstance(error, err_type):
                return policy
        return {
            "max_retries": 0,
            "base_delay": 1.0,
            "max_delay": 5.0,
            "exponential_backoff": False,
        }

    def calculate_retry_delay(self, policy: Dict[str, Any], retry_count: int) -> float:
        base_delay = policy["base_delay"]
        max_delay = policy["max_delay"]
        if policy["exponential_backoff"]:
            delay = base_delay * (2 ** (retry_count - 1))
        else:
            delay = base_delay * retry_count
        return min(delay, max_delay)

    async def handle_retry(self, error: Exception, retry_count: int, context: Dict[str, Any]) -> Tuple[bool, float]:
        policy = self.get_retry_policy(error)
        max_retries = policy["max_retries"]

        if retry_count >= max_retries:
            return False, 0.0

        delay = self.calculate_retry_delay(policy, retry_count + 1)
        self.logger.info(f"Retry {retry_count + 1}/{max_retries} for {context.get('function_name')}. Waiting {delay:.1f}s")
        return True, delay


def handle_bot_exceptions(func: Callable):
    """
    Декоратор для обработки исключений с использованием ErrorHandler.
    """
    @wraps(func)
    async def wrapper(self, update: Update, context: CallbackContext, *args, **kwargs):
        retry_count = 0
        logger.info(f"Начало выполнения функции {func.__name__}.")

        while True:
            try:
                return await func(self, update, context, *args, **kwargs)

            except Exception as e:
                logger.error(f"Исключение в функции {func.__name__}: {e}", exc_info=True)

                error_context = {
                    "user_id": update.effective_user.id if update and update.effective_user else "Unknown",
                    "chat_id": update.effective_chat.id if update and update.effective_chat else None,
                    "function_name": func.__name__,
                    "command": context.args[0] if context and context.args else None,
                    "retry_count": retry_count,
                }

                # Используем error_handler из self (экземпляр бота)
                if not hasattr(self, 'error_handler'):
                    logger.error("Bot instance has no error_handler attribute!")
                    raise e

                can_retry, delay = await self.error_handler.handle_retry(e, retry_count, error_context)

                if can_retry:
                    retry_count += 1
                    await asyncio.sleep(delay)
                    continue

                user_message, success = await self.error_handler.handle_error(e, error_context)

                if update and update.effective_message:
                    markup = None
                    # Здесь можно добавить логику кнопок повтора
                    await update.effective_message.reply_text(user_message, parse_mode="HTML", reply_markup=markup)
                
                break
    return wrapper
