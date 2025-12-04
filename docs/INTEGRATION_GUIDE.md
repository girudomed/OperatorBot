# Integration Guide: ML Analytics System

## Шаг 1: Применение Миграций

```bash
cd /Users/vitalyefimov/Projects/operabot

# Применить миграции
mysql -u YOUR_USER -p YOUR_DATABASE < migrations/003_ml_analytics.sql
mysql -u YOUR_USER -p YOUR_DATABASE < migrations/004_extended_roles.sql

# Проверить
mysql -u YOUR_USER -p YOUR_DATABASE -e "SHOW TABLES LIKE 'operator_%'"
mysql -u YOUR_USER -p YOUR_DATABASE -e "DESCRIBE call_scores" | grep ml_
```

## Шаг 2: Регистрация Handlers в main.py

Добавьте в `app/main.py`:

```python
from app.telegram.handlers.dashboard import DashboardHandler
from app.telegram.handlers.transcripts import TranscriptHandler
from app.telegram.handlers.dev_messages import DevMessagesHandler

def setup_handlers(application, db_manager):
    """Регистрация всех handlers."""
    
    # ... существующие handlers ...
    
    # Новые ML Analytics handlers
    dashboard_handler = DashboardHandler(db_manager)
    transcript_handler = TranscriptHandler(db_manager)
    dev_messages_handler = DevMessagesHandler(db_manager)
    
    # Регистрируем handlers
    for handler in dashboard_handler.get_handlers():
        application.add_handler(handler)
    
    for handler in transcript_handler.get_handlers():
        application.add_handler(handler)
    
    for handler in dev_messages_handler.get_handlers():
        application.add_handler(handler)
```

## Шаг 3: Обновление ReportService (опционально)

Если используете старый `ReportService`, он уже обновлен с интеграцией новых компонентов.
Убедитесь что в `app/services/reports.py` есть импорты:

```python
from app.db.repositories.analytics import AnalyticsRepository
from app.services.recommendations import RecommendationsService
```

## Шаг 4: Переменные Окружения

Проверьте наличие в `.env` или конфигурации:

```bash
# Для dev messages handler
DEV_ADMIN_IDS=123456789,987654321
SUPREME_ADMIN_IDS=111111111

# Для LLM (если используете)
OPENAI_API_KEY=sk-...
```

## Шаг 5: Тестирование

```bash
# Запустить бота
python -m app.main

# В Telegram проверить команды:
/dashboard          # Должен показать меню дашборда
/transcript 12345   # Расшифровка звонка (замените на реальный ID)
/message_dev Тест   # Отправить сообщение разработчику
/report daily       # Отчет за день (для операторов)
```

## Шаг 6: Проверка Прав Доступа

Убедитесь что пользователи правильно назначены в таблице `UsersTelegaBot`:

```sql
-- Проверить роли
SELECT user_id, username, role_id, operator_name, extension 
FROM UsersTelegaBot 
WHERE status = 'approved';

-- role_id:
-- 1 = Оператор
-- 2 = Администратор
-- 3 = Маркетолог
-- 4 = ЗавРег
-- 5 = СТ Админ
-- 6 = Руководство
-- 7 = SuperAdmin
-- 8 = Dev
```

## Шаг 7: Оптимизация (опционально)

### Включить кеширование дашборда

В `DashboardHandler` добавьте использование `DashboardCacheService`:

```python
from app.services.dashboard_cache import DashboardCacheService

class DashboardHandler:
    def __init__(self, db_manager: DatabaseManager):
        # ...
        self.cache_service = DashboardCacheService(db_manager)
    
    async def _show_single_dashboard(self, ...):
        # Попробовать получить из кеша
        cached = await self.cache_service.get_cached_dashboard(
            operator_name, period, period_start, period_end
        )
        
        if cached:
            dashboard = cached
        else:
            # Получить свежие данные
            dashboard = await self.analytics_repo.get_live_dashboard_single(...)
            # Сохранить в кеш
            await self.cache_service.save_dashboard_cache(dashboard)
```

### Настроить cleanup задачу

Добавьте cron или фоновую задачу для очистки старого кеша:

```python
# В scheduler или отдельном скрипте
async def cleanup_old_cache():
    cache_service = DashboardCacheService(db_manager)
    await cache_service.cleanup_old_cache(days=7)
```

## Troubleshooting

### Проблема: "Нет данных для оператора"

**Решение:** Проверьте что:
1. У пользователя заполнено поле `operator_name` в `UsersTelegaBot`
2. Это имя совпадает с `called_info` или `caller_info` в `call_scores`

```sql
-- Проверить совпадение
SELECT DISTINCT called_info FROM call_scores WHERE call_type = 'принятый' LIMIT 10;
SELECT operator_name FROM UsersTelegaBot WHERE operator_name IS NOT NULL;
```

### Проблема: "У вас нет прав"

**Решение:** Проверьте role_id пользователя:

```sql
UPDATE UsersTelegaBot 
SET role_id = 2  -- Администратор
WHERE user_id = YOUR_TELEGRAM_ID;
```

### Проблема: Кеш не обновляется

**Решение:** Инвалидируйте кеш вручную:

```sql
DELETE FROM operator_dashboards 
WHERE operator_name = 'Иванова А.И.';
```

Или через код:
```python
await cache_service.invalidate_cache(operator_name="Иванова А.И.")
```

## Production Checklist

- [ ] Применены обе миграции (003, 004)
- [ ] Зарегистрированы все handlers в main.py
- [ ] Проверены переменные окружения
- [ ] Назначены роли пользователям
- [ ] Протестированы команды: /dashboard, /transcript, /message_dev
- [ ] Проверена работа permissions (оператор видит только своё)
- [ ] Настроен cleanup для кеша (опционально)
- [ ] Проверена интеграция с ReportService
- [ ] Логи не содержат критических ошибок

## Дополнительная Документация

- **Roles:** см. `migrations/004_extended_roles.sql` - таблица `roles_reference`
- **Permissions:** см. `app/services/permissions.py` - класс `PermissionChecker`
- **Dashboard Metrics:** см. `МЛ_РАСЧЕТЫ` - формулы расчета метрик
- **Recommendations:** см. `app/services/recommendations.py` - логика генерации

---

**Готово!** Система полностью интегрирована и готова к работе.
