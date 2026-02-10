# Error Contract (Core Path)

## Цель
Единый контракт управляемых ошибок для `core path`:
- единый корневой тип `AppError`;
- типовая маршрутизация (без строковых эвристик);
- один верхнеуровневый обработчик финального stacktrace + user-notify;
- явный best-effort только через `best_effort_*` helper.

## Слой ответственности
1. Интеграции/репозитории:
- перехватывают только low-level исключения SDK/драйверов;
- конвертируют в `AppError`-наследники;
- сохраняют причину через `raise NewError(...) from exc`.

2. Сервисы:
- не скрывают unknown bug exceptions;
- используют retry только если `retryable=True`.

3. Верхний уровень (`app/main.py::telegram_error_handler`):
- финальное логирование инцидента;
- маппинг в user message;
- дедуп user-notify по `(trace_id, update_id)`.

## Классы ошибок
- `AppError` — корневой тип
- `ValidationAppError`
- `AuthorizationAppError`
- `AccessDeniedAppError`
- `IntegrationError`
- `RepositoryError`
- `DatabaseIntegrationError`
- `OpenAIIntegrationError`
- `YandexDiskIntegrationError`
- `TelegramIntegrationError`
- `CacheIntegrationError`
- `RateLimitAppError`

Совместимость: `BotError = AppError`.

## Политика
- `retryable` управляет правом retry.
- `error_policy.get_retry_config()` задает backoff.
- `error_policy.resolve_user_message()` возвращает user-facing текст.
- `error_policy.should_alert()` определяет алерт.

## Best effort
Разрешен только через:
- `best_effort_async(...)`
- `best_effort_sync(...)`

Каждый best-effort логирует событие `event=best_effort` с `status`, `operation`, `trace_id`.

## Пример обязательного raise-from
```python
try:
    await client.call(...)
except httpx.HTTPError as exc:
    raise IntegrationError("External API failed", retryable=True) from exc
```
