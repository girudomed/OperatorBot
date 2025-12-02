# Краткий Справочник LM Метрик

## Быстрый доступ

### Операционные метрики (operational)

| Код метрики | Диапазон | Описание |
|-------------|----------|----------|
| `response_speed_score` | 0-100 | Скорость ответа на звонок. 85 = принят, 20 = пропущен |
| `talk_time_efficiency` | 0-100 | Эффективность времени разговора. Выше = продуктивнее |
| `queue_impact_index` | 0-100 | Нагрузка на систему. Выше = больше нагрузка |

### Конверсионные метрики (conversion)

| Код метрики | Диапазон | Описание |
|-------------|----------|----------|
| `conversion_score` | 0-100 | Вероятность конверсии. 100 = запись, 50 = лид, 10-20 = низкий потенциал |
| `lost_opportunity_score` | 0-100 | Ценность потери. 80 = целевой потерян, 0 = конвертирован |
| `cross_sell_potential` | 0-100 | Потенциал допродажи. 70 = записался, 40 = интерес, 10 = низкий |

### Метрики качества (quality)

| Код метрики | Диапазон | Описание |
|-------------|----------|----------|
| `checklist_coverage_ratio` | 0-100 | Процент выполнения чек-листа. (items/10)*100 |
| `normalized_call_score` | 0-100 | Нормализованная оценка качества. Унифицированная шкала |
| `script_risk_index` | 0-100 | Риск отклонения от скрипта. Выше = больше риск |

### Метрики рисков (risk)

| Код метрики | Значения | Описание |
|-------------|----------|----------|
| `churn_risk_level` | low/medium/high + 0-100 | Риск оттока. 90 = жалоба, 70 = отмена, 10 = записался |
| `complaint_risk_flag` | true/false + 0-100 | Риск жалобы. true при score >= 50 |
| `followup_needed_flag` | true/false + 0/1 | Нужен ли follow-up. true для лидов без записи |

### Прогнозные метрики (forecast)

| Код метрики | Диапазон | Описание |
|-------------|----------|----------|
| `conversion_prob_forecast` | 0-1 | Вероятность будущей конверсии. 1.0 = конвертирован, 0.35 = лид, 0.05 = низкая |
| `second_call_prob` | 0-1 | Вероятность повторного звонка. 0.60 = навигация, 0.15 = записался |
| `complaint_prob` | 0-1 | Вероятность жалобы. 1.0 = есть, 0.40 = низкое качество, 0.05 = норма |

### Вспомогательные метрики (aux)

| Код метрики | Тип | Описание |
|-------------|-----|----------|
| `lm_version_tag` | string | Версия LM (lm_v1, lm_v2,...) |
| `calc_profile` | string | Профиль расчета (default_v1, night_shift_v1, weekend_v1) |

---

## Таблица приоритетов

### Высокий приоритет - требуют немедленного действия

- `churn_risk_level` = 'high' (score >= 70)
- `complaint_risk_flag` = 'true' (score >= 50)
- `followup_needed_flag` = 'true'
- `script_risk_index` >= 70

### Средний приоритет - требуют мониторинга

- `churn_risk_level` = 'medium' (score 40-69)
- `conversion_score` < 30 (низкая конверсия)
- `lost_opportunity_score` >= 60 (значительная потеря)
- `script_risk_index` 40-69

### Низкий приоритет - для аналитики

- `cross_sell_potential` >= 60 (возможности допродажи)
- `queue_impact_index` >= 80 (планирование мощностей)
- Все прогнозные метрики для планирования

---

## Частые запросы

### Python: Получить метрики звонка

```python
from app.db.repositories.lm_repository import LMRepository

lm_repo = LMRepository(db_manager)
metrics = await lm_repo.get_lm_values_by_call(history_id=123)

# Найти конкретную метрику
conversion = next((m for m in metrics if m['metric_code'] == 'conversion_score'), None)
if conversion:
    print(f"Конверсия: {conversion['value_numeric']}")
```

### SQL: Звонки требующие внимания

```sql
SELECT DISTINCT lv.history_id, ch.caller_number
FROM lm_value lv
JOIN call_history ch ON ch.history_id = lv.history_id
WHERE (
    (lv.metric_code = 'churn_risk_level' AND lv.value_label = 'high')
    OR (lv.metric_code = 'complaint_risk_flag' AND lv.value_label = 'true')
    OR (lv.metric_code = 'followup_needed_flag' AND lv.value_label = 'true')
)
AND lv.created_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR);
```

### Python Worker: Batch расчет

```python
from app.workers.lm_calculator_worker import LMCalculatorWorker

worker = LMCalculatorWorker(db_manager)

# Последние 24 часа
await worker.process_recent_calls(hours_back=24)

# Конкретные звонки
await worker.process_specific_calls(history_ids=[123, 124, 125])
```

---

## Формулы быстрого расчета метрик

### Операционные

```
response_speed_score = 85 если talk_duration > 0 иначе 20

talk_time_efficiency = min(100, talk_duration/3) если > 30s
                      talk_duration*2 если <= 30s

queue_impact_index = min(100, (talk_duration/300)*100)
```

### Конверсии

```
conversion_score = 100 если outcome='record'
                  50 если outcome='lead_no_record'
                  20 если категория 'Навигация'
                  10 иначе

lost_opportunity = 80 если is_target=1 И outcome!='record'
                  0 если is_target=1 И outcome='record'
                  20 иначе

cross_sell = 70 если outcome='record'
            40 если есть requested_service
            10 иначе
```

### Качество

```
checklist_coverage = min(100, (number_checklist/10)*100)

normalized_score = call_score*10 если call_score <= 10
                  call_score иначе
                  (ограничено 0-100)

script_risk = 80 если score <= 3
             50 если score <= 5
             30 если score <= 7
             10 иначе
             (+20 если категория 'Жалоба' или 'Отмена')
```

### Риски

```
churn_risk = 90 (high) если категория='Жалоба'
            70 (high) если категория='Отмена записи'
            60 (medium) если outcome='refusal'
            50 (medium) если есть refusal_reason
            10 (low) если outcome='record'
            30 (low) иначе

complaint_risk = 100 если категория='Жалоба'
                60 если call_score < 3
                40 если категория='Отмена записи'
                10 иначе
                flag=true если >= 50

followup_needed = true если outcome='lead_no_record'
                 true если категория in ['Жалоба', 'Лид (без записи)']
             false иначе
```

### Прогнозы

```
conversion_prob = 1.0 если outcome='record'
                 0.35 если outcome='lead_no_record'
                 0.20 если is_target=1
                 0.05 иначе

second_call_prob = 0.60 если категория 'Навигация'
                  0.45 если outcome='lead_no_record'
                  0.15 если outcome='record'
                  0.25 иначе

complaint_prob = 1.0 если категория='Жалоба'
                0.40 если call_score < 3
                0.25 если категория='Отмена записи'
                0.05 иначе
```

---

## Диагностика и troubleshooting

### Проверить покрытие метриками

```sql
SELECT 
    COUNT(DISTINCT history_id) as calls_with_lm,
    (SELECT COUNT(*) FROM call_history WHERE call_date >= DATE_SUB(NOW(), INTERVAL 7 DAY)) as total,
    ROUND(COUNT(DISTINCT history_id) / (SELECT COUNT(*) FROM call_history WHERE call_date >= DATE_SUB(NOW(), INTERVAL 7 DAY)) * 100, 1) as coverage_pct
FROM lm_value
WHERE created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY);
```

### Найти звонки без метрик

```sql
SELECT ch.history_id, ch.call_date, ch.called_info
FROM call_history ch
LEFT JOIN lm_value lv ON lv.history_id = ch.history_id
WHERE ch.call_date >= DATE_SUB(NOW(), INTERVAL 7 DAY)
  AND lv.id IS NULL
LIMIT 20;
```

### Проверить распределение метрик

```sql
SELECT 
    metric_code,
    COUNT(*) as count,
    AVG(value_numeric) as avg_value,
    MIN(value_numeric) as min_value,
    MAX(value_numeric) as max_value
FROM lm_value
WHERE metric_group = 'conversion'
  AND created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
GROUP BY metric_code;
```

---

## Рекомендации по использованию

### В отчетах для менеджмента

Используйте:
- `conversion_score` - эффективность конверсии
- `lost_opportunity_score` - упущенная выгода
- `churn_risk_level` - распределение рисков
- Прогнозы (`conversion_prob_forecast`, `second_call_prob`)

### Для операторов (личные отчеты)

Используйте:
- `normalized_call_score` - качество работы
- `checklist_coverage_ratio` - соблюдение процедур
- `script_risk_index` - проблемные звонки для review

### Для планирования работы

Используйте:
- `followup_needed_flag` - список для дозвонов
- `churn_risk_level` = 'high' - retention работа
- `complaint_risk_flag` = true - превентивная работа
- `queue_impact_index` - планирование нагрузки

### Для аналитики и ML

Используйте:
- Все прогнозные метрики для обучения моделей
- Исторические данные из `lm_value` как features
- Сравнение `lm_version` для A/B тестов
- `calc_profile` для сегментации данных

---

*Полная документация: [LM_ДОКУМЕНТАЦИЯ.md](LM_ДОКУМЕНТАЦИЯ.md)*
