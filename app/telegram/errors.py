"""Совместимый слой ошибок Telegram-модуля.

Канонические типы определены в app.errors.
"""

from app.errors import (
    AccessDeniedAppError,
    AppError,
    AuthorizationAppError,
    BotError,
    CacheIntegrationError,
    DatabaseIntegrationError,
    ErrorCategory,
    ErrorSeverity,
    IntegrationError,
    OpenAIIntegrationError,
    RateLimitAppError,
    RepositoryError,
    TelegramIntegrationError,
    ValidationAppError,
    YandexDiskIntegrationError,
)

# Backward-friendly aliases for old names.
RetryableError = IntegrationError
RateLimitError = RateLimitAppError
AuthenticationError = AuthorizationAppError
ValidationError = ValidationAppError
DataProcessingError = RepositoryError
VisualizationError = IntegrationError
ExternalServiceError = IntegrationError
PermissionDeniedError = AccessDeniedAppError

__all__ = [
    "AppError",
    "BotError",
    "ErrorSeverity",
    "ErrorCategory",
    "IntegrationError",
    "RepositoryError",
    "DatabaseIntegrationError",
    "OpenAIIntegrationError",
    "YandexDiskIntegrationError",
    "TelegramIntegrationError",
    "CacheIntegrationError",
    "RateLimitAppError",
    "ValidationAppError",
    "AuthorizationAppError",
    "AccessDeniedAppError",
    "RetryableError",
    "RateLimitError",
    "AuthenticationError",
    "ValidationError",
    "DataProcessingError",
    "VisualizationError",
    "ExternalServiceError",
    "PermissionDeniedError",
]
