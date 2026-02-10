from __future__ import annotations

from enum import Enum
from typing import Any, Dict, Optional


class ErrorSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class ErrorCategory(str, Enum):
    VALIDATION = "validation"
    AUTH = "auth"
    PERMISSION = "permission"
    REPOSITORY = "repository"
    INTEGRATION = "integration"
    RATE_LIMIT = "rate_limit"
    UNEXPECTED = "unexpected"


class AppError(Exception):
    """Канонический корневой тип управляемых ошибок приложения."""

    def __init__(
        self,
        message: str,
        *,
        user_message: Optional[str] = None,
        category: ErrorCategory = ErrorCategory.UNEXPECTED,
        retryable: bool = False,
        severity: ErrorSeverity = ErrorSeverity.ERROR,
        user_visible: bool = True,
        alert: bool = False,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.user_message = user_message
        self.category = category
        self.retryable = retryable
        self.severity = severity
        self.user_visible = user_visible
        self.alert = alert
        self.details = details or {}


# Domain errors
class ValidationAppError(AppError):
    def __init__(self, message: str, **kwargs: Any) -> None:
        kwargs.setdefault("category", ErrorCategory.VALIDATION)
        kwargs.setdefault("severity", ErrorSeverity.WARNING)
        kwargs.setdefault("alert", False)
        super().__init__(message, **kwargs)


class AuthorizationAppError(AppError):
    def __init__(self, message: str, **kwargs: Any) -> None:
        kwargs.setdefault("category", ErrorCategory.AUTH)
        kwargs.setdefault("severity", ErrorSeverity.WARNING)
        kwargs.setdefault("alert", False)
        super().__init__(message, **kwargs)


class AccessDeniedAppError(AppError):
    def __init__(self, message: str, **kwargs: Any) -> None:
        kwargs.setdefault("category", ErrorCategory.PERMISSION)
        kwargs.setdefault("severity", ErrorSeverity.WARNING)
        kwargs.setdefault("alert", False)
        super().__init__(message, **kwargs)


# Integration and repository errors
class IntegrationError(AppError):
    def __init__(self, message: str, **kwargs: Any) -> None:
        kwargs.setdefault("category", ErrorCategory.INTEGRATION)
        kwargs.setdefault("severity", ErrorSeverity.ERROR)
        kwargs.setdefault("alert", True)
        super().__init__(message, **kwargs)


class RepositoryError(AppError):
    def __init__(self, message: str, **kwargs: Any) -> None:
        kwargs.setdefault("category", ErrorCategory.REPOSITORY)
        kwargs.setdefault("severity", ErrorSeverity.ERROR)
        kwargs.setdefault("alert", True)
        super().__init__(message, **kwargs)


class DatabaseIntegrationError(RepositoryError):
    pass


class OpenAIIntegrationError(IntegrationError):
    pass


class YandexDiskIntegrationError(IntegrationError):
    pass


class TelegramIntegrationError(IntegrationError):
    pass


class CacheIntegrationError(IntegrationError):
    pass


class RateLimitAppError(AppError):
    def __init__(self, message: str, **kwargs: Any) -> None:
        kwargs.setdefault("category", ErrorCategory.RATE_LIMIT)
        kwargs.setdefault("severity", ErrorSeverity.INFO)
        kwargs.setdefault("retryable", True)
        kwargs.setdefault("alert", False)
        super().__init__(message, **kwargs)


# Backward compatibility alias
BotError = AppError
