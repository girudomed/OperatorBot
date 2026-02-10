from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Type

from app.errors import (
    AppError,
    CacheIntegrationError,
    DatabaseIntegrationError,
    ErrorSeverity,
    OpenAIIntegrationError,
    RateLimitAppError,
    YandexDiskIntegrationError,
)


@dataclass(frozen=True)
class RetryConfig:
    enabled: bool
    max_retries: int
    base_delay: float
    max_delay: float
    exponential_backoff: bool


@dataclass(frozen=True)
class ErrorPolicy:
    retry: RetryConfig
    log_level: str
    default_user_message: str
    alert: bool


_DEFAULT_RETRY = RetryConfig(
    enabled=False,
    max_retries=0,
    base_delay=0.0,
    max_delay=0.0,
    exponential_backoff=False,
)


_POLICY_MAP: Dict[Type[AppError], ErrorPolicy] = {
    DatabaseIntegrationError: ErrorPolicy(
        retry=RetryConfig(True, 3, 0.5, 5.0, True),
        log_level="error",
        default_user_message="Ошибка доступа к базе. Повторите позже.",
        alert=True,
    ),
    OpenAIIntegrationError: ErrorPolicy(
        retry=RetryConfig(True, 3, 1.0, 8.0, True),
        log_level="warning",
        default_user_message="Сервис рекомендаций временно недоступен.",
        alert=False,
    ),
    YandexDiskIntegrationError: ErrorPolicy(
        retry=RetryConfig(True, 2, 1.0, 5.0, True),
        log_level="warning",
        default_user_message="Не удалось получить запись звонка из внешнего сервиса.",
        alert=False,
    ),
    CacheIntegrationError: ErrorPolicy(
        retry=RetryConfig(False, 0, 0.0, 0.0, False),
        log_level="warning",
        default_user_message="",
        alert=False,
    ),
    RateLimitAppError: ErrorPolicy(
        retry=RetryConfig(True, 3, 2.0, 20.0, True),
        log_level="info",
        default_user_message="Превышен лимит запросов. Попробуйте позже.",
        alert=False,
    ),
}


def get_policy(exc: Exception) -> ErrorPolicy:
    if isinstance(exc, AppError):
        for err_type, policy in _POLICY_MAP.items():
            if isinstance(exc, err_type):
                return policy
        log_level = "error"
        if exc.severity == ErrorSeverity.WARNING:
            log_level = "warning"
        elif exc.severity == ErrorSeverity.INFO:
            log_level = "info"
        elif exc.severity == ErrorSeverity.CRITICAL:
            log_level = "critical"
        return ErrorPolicy(
            retry=_DEFAULT_RETRY,
            log_level=log_level,
            default_user_message=exc.user_message or "Произошла ошибка. Повторите позже.",
            alert=exc.alert,
        )

    return ErrorPolicy(
        retry=_DEFAULT_RETRY,
        log_level="error",
        default_user_message="Команда временно недоступна. Попробуйте позже.",
        alert=True,
    )


def is_retryable(exc: Exception) -> bool:
    if isinstance(exc, AppError):
        return bool(exc.retryable)
    return False


def get_retry_config(exc: Exception) -> RetryConfig:
    if isinstance(exc, AppError):
        if not exc.retryable:
            return _DEFAULT_RETRY
        policy = get_policy(exc)
        return policy.retry
    return _DEFAULT_RETRY


def resolve_user_message(exc: Exception) -> str:
    if isinstance(exc, AppError):
        if exc.user_visible and exc.user_message:
            return exc.user_message
        return get_policy(exc).default_user_message
    return get_policy(exc).default_user_message


def should_alert(exc: Exception) -> bool:
    if isinstance(exc, AppError):
        return exc.alert or get_policy(exc).alert
    return True
