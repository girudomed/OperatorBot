# Обработка ошибок (актуальная модель)

## Ключевые правила
- Канонический корень управляемых ошибок: `AppError`.
- Классификация и маршрутизация только по типам/атрибутам (`retryable`, `severity`, `alert`, `user_visible`).
- Строковые эвристики (`"timeout" in str(exc)`) не используются как механизм маршрутизации в core path.
- Unknown исключения (баги) не маскируются.
- Нижние уровни не выполняют финальное stacktrace-логирование и не отправляют user-уведомления.
- Верхний уровень (`telegram_error_handler`) отвечает за финальное логирование, user-notify и дедуп.

## Retry
- Retry разрешен только если `retryable=True`.
- Политика retry определяется через `app/error_policy.py`.

## Best-effort
- Silent-fail допускается только через `best_effort_*` helper.
- Для best-effort обязательно структурированное событие в логах (`event=best_effort`, `status`, `operation`, `trace_id`).

## Где смотреть
- Контракт ошибок: `docs/ERROR_CONTRACT.md`
- Классы ошибок: `app/errors.py`
- Политика: `app/error_policy.py`
- Best-effort helper: `app/utils/best_effort.py`
