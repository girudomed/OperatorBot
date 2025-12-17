# LM Аналитическая Система

> Система расчета метрик для звонков OperatorBot

## Быстрый старт

### 1. Миграция БД

```bash
mysql -u username -p database < db/migrations/001_create_lm_value_table.sql
```

### 2. Использование

```python
from app.db.repositories.lm_repository import LMRepository
from app.services.lm_service import LMService

# Расчет метрик для звонка
lm_service = LMService(lm_repo)
count = await lm_service.calculate_all_metrics(
    history_id=123,
    call_history=call_data,
    call_score=score_data
)
```

### 3. Worker для batch обработки

```python
from app.workers.lm_calculator_worker import LMCalculatorWorker

worker = LMCalculatorWorker(db_manager)
await worker.process_recent_calls(hours_back=24)
```

## Что рассчитывается

**18 метрик** в 6 категориях:

1. **Операционные** - скорость, эффективность, нагрузка
2. **Конверсионные** - конверсия, потери, cross-sell
3. **Качество** - чек-лист, оценка, риск скрипта
4. **Риски** - отток, жалобы, follow-up
5. **Прогнозы** - вероятности событий
6. **Вспомогательные** - версия, профиль

## Документация

- 📖 [Подробная документация](docs/LM_ДОКУМЕНТАЦИЯ.md) - полное руководство
- 📋 [Краткий справочник](docs/LM_СПРАВОЧНИК.md) - быстрый доступ к метрикам
- 🔬 [Walkthrough](../.gemini/antigravity/brain/cffda447-86d4-408d-b2dd-5f5e5c75f7ed/walkthrough.md) - обзор реализации

## Структура

```
app/
├── db/
│   ├── models.py                 # TypedDict для метрик
│   └── repositories/
│       └── lm_repository.py      # Операции с БД
├── services/
│   └── lm_service.py            # Расчет метрик
└── workers/
    └── lm_calculator_worker.py  # Фоновая обработка

db/migrations/
└── 001_create_lm_value_table.sql

docs/
├── LM_ДОКУМЕНТАЦИЯ.md           # Полное руководство
└──  LM_СПРАВОЧНИК.md             # Краткая справка

tests/
├── unit/
│   ├── test_lm_service.py
│   └── test_lm_repository.py
└── test_lm_integration.py
```

## Примеры

### Получить метрики звонка

```python
metrics = await lm_repo.get_lm_values_by_call(history_id=123)
for m in metrics:
    print(f"{m['metric_code']}: {m['value_numeric']}")
```

### SQL: Звонки с высоким риском

```sql
SELECT lv.history_id, ch.caller_number, lv.value_label
FROM lm_value lv
JOIN call_history ch ON ch.history_id = lv.history_id
WHERE lv.metric_code = 'churn_risk_level'
  AND lv.value_label = 'high'
  AND lv.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY);
```

### Агрегация

```python
stats = await lm_repo.get_metric_statistics(
    metric_code='conversion_score',
    start_date=week_ago,
    end_date=now
)
print(f"Средняя конверсия: {stats['avg_value']}")
```

## Типизация

Все метрики строго типизированы:

```python
from app.db.models import (
    LMOperationalMetric,
    LMConversionMetric,
    LMQualityMetric,
    LMRiskMetric,
    LMForecastMetric,
    LMAuxiliaryMetric
)
```

## Тестирование

```bash
# Unit тесты
pytest tests/unit/test_lm_service.py -v
pytest tests/unit/test_lm_repository.py -v

# Интеграционные
pytest tests/test_lm_integration.py -v
```

## Развертывание

1. **Миграция БД** - создать таблицу `lm_value`
2. **Backfill** - заполнить исторические данные
3. **Cron** - настроить автоматический расчет

Подробно: см. [Руководство по развертыванию](docs/LM_ДОКУМЕНТАЦИЯ.md#руководство-по-развертыванию)

## Производительность

- Batch сохранение метрик
- Индексированные запросы
- Асинхронные операции
- Поддержка больших объемов

## Поддержка

Вопросы и предложения:
- Код: `app/services/lm_service.py`
- Тесты: `tests/unit/test_lm_service.py`
- Doc: `docs/LM_ДОКУМЕНТАЦИЯ.md`
