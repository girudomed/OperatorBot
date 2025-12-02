# Система обработки ошибок Operabot

## Обзор

Полная система централизованного логирования и обработки ошибок, включающая:

1. **Глобальные обработчики исключений** (sync и async)
2. **Декораторы** для автоматического логирования
3. **Контекстные менеджеры** для безопасного выполнения
4. **Утилиты** для работы с корутинами

## 🎯 Основные возможности

### 1. Автоматическая установка

Обработчики устанавливаются автоматически при запуске приложения:

```python
# В app/main.py
from app.utils.error_handlers import setup_global_exception_handlers

setup_global_exception_handlers()
```

Это устанавливает:
- `sys.excepthook` для необработанных sync исключений
- `loop.set_exception_handler` для async задач

### 2. Декораторы

#### Для async функций

```python
from app.utils.error_handlers import log_async_exceptions

@log_async_exceptions
async def my_service_method(self, user_id: int):
    # Любые исключения будут залогированы через logger.error
    result = await self.repo.get_data(user_id)
    return result
```

#### Для sync функций

```python
from app.utils.error_handlers import log_exceptions

@log_exceptions
def process_data(data):
    # Ошибки автоматически логируются
    return transform(data)
```

**Что логируется:**
- Полный traceback через `exc_info=True`
- Имя функции и модуля
- Тип исключения
- Сообщение об ошибке
- Аргументы функции (первые 200 символов)

### 3. Контекстные менеджеры

#### С propagation ошибки

```python
from app.utils.error_handlers import ErrorContext

async def initialize_service():
    # Ошибка будет залогирована И пробросится дальше
    async with ErrorContext("Инициализация БД"):
        await db.connect()
        await db.migrate()
```

#### Без propagation (подавление ошибки)

```python
# Ошибка логируется, но не прерывает выполнение
async with ErrorContext("Отправка уведомления", reraise=False):
    await send_notification(user_id)

# Код продолжит выполнение даже если send_notification упадет
```

#### С разными уровнями логирования

```python
# Логируем как warning вместо error
with ErrorContext("Опциональная операция", log_level="warning"):
    optional_task()
```

### 4. Безопасное выполнение

#### Для sync кода

```python
from app.utils.error_handlers import safe_execute

# Вернет None при ошибке вместо exception
result = safe_execute(risky_function, arg1, arg2, key=value)

if result is not None:
    process(result)
```

#### Для async кода

```python
from app.utils.error_handlers import safe_async_execute

result = await safe_async_execute(async_risky_function, user_id)

if result is not None:
    await process(result)
```

### 5. Обработка корутин

```python
from app.utils.error_handlers import log_coroutine_exceptions

# При создании задачи
coro = fetch_data(user_id)
task = asyncio.create_task(log_coroutine_exceptions(coro))

# Любые ошибки в корутине будут залогированы
```

### 6. Форматирование деталей ошибки

```python
from app.utils.error_handlers import format_exception_details

try:
    risky_operation()
except Exception as e:
    details = format_exception_details(e)
    # details содержит:
    # - exception_type
    # - exception_message
    # - traceback (полный текст)
    # - traceback_lines (список)
    # - cause, context
    
    logger.error("Детали ошибки", extra=details)
```

## 📝 Примеры использования

### Пример 1: Telegram хендлер

```python
from app.utils.error_handlers import log_async_exceptions, ErrorContext

class ReportHandler:
    @log_async_exceptions
    async def handle_command(self, update: Update, context: CallbackContext):
        user_id = update.effective_user.id
        
        # Критичная операция - пробрасываем ошибку
        async with ErrorContext("Проверка прав доступа"):
            await self.check_permissions(user_id)
        
        # Некритичная - продолжаем даже при ошибке
        async with ErrorContext("Отправка аналитики", reraise=False):
            await self.send_analytics(user_id)
        
        return await self.generate_report(user_id)
```

### Пример 2: Сервис

```python
from app.utils.error_handlers import log_async_exceptions

class DataService:
    @log_async_exceptions
    async def fetch_and_process(self, query: str):
        # Все ошибки автоматически логируются
        raw_data = await self.repo.fetch(query)
        processed = self.process(raw_data)
        await self.repo.save(processed)
        return processed
```

### Пример 3: Воркер очереди

```python
from app.utils.error_handlers import ErrorContext

async def task_worker(queue):
    while True:
        task = await queue.get()
        
        # Ошибки в задаче не остановят воркер
        async with ErrorContext(f"Обработка задачи {task['id']}", reraise=False):
            await process_task(task)
        
        queue.task_done()
```

## 🔍 Поиск ошибок в логах

Все ошибки логируются через `logger.error()`, что позволяет легко искать:

```bash
# Поиск всех ошибок
grep "ERROR" logs/app.log

# Поиск конкретного типа ошибки
grep "ValueError" logs/app.log

# Поиск ошибок в конкретном модуле
grep "app.services.reports" logs/app.log | grep ERROR
```

## ⚙️ Конфигурация

### Уровни логирования

По умолчанию все ошибки логируются как `ERROR`. Можно изменить:

```python
async with ErrorContext("Операция", log_level="warning"):
    # Будет залогировано как WARNING
    pass
```

### Подавление CancelledError

`asyncio.CancelledError` автоматически логируется как `DEBUG`, а не `ERROR`, так как это ожидаемое поведение при отмене задач.

## 🎯 Best Practices

1. **Используйте декораторы** для всех public методов сервисов
2. **ErrorContext с reraise=True** для критичных операций
3. **ErrorContext с reraise=False** для некритичных (аналитика, уведомления)
4. **safe_execute** для операций, где None - валидный результат при ошибке
5. **log_coroutine_exceptions** при создании фоновых задач через `create_task`

## 📊 Что логируется автоматически

При каждой ошибке:

```
ERROR - Ошибка в async app.services.reports.generate_report
Traceback (most recent call last):
  File "app/services/reports.py", line 50, in generate_report
    metrics = await self.metrics_service.calculate_operator_metrics(...)
  ...
ValueError: Invalid operator ID

Extra fields:
  - function: generate_report
  - module: app.services.reports
  - exception_type: ValueError
  - exception_message: Invalid operator ID
  - args: (123,)
  - kwargs: {'period': 'daily'}
```

## 🚨 Обработка критических ошибок

Для критических ошибок, требующих немедленной остановки:

```python
try:
    critical_operation()
except CriticalError as e:
    logger.critical("Критическая ошибка!", exc_info=True)
    # Отправить в Sentry/мониторинг
    raise SystemExit(1)
```

## ✅ Проверка

После внедрения:

1. ✅ Все async методы сервисов с `@log_async_exceptions`
2. ✅ Глобальные обработчики в `app/main.py`
3. ✅ ErrorContext в критичных местах
4. ✅ Тесты на логирование ошибок

---

**Контакты**: После внедрения все ошибки будут доступны через `grep ERROR` в логах!
