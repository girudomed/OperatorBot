# Документация LM Аналитической Системы

## Оглавление
1. [Общий обзор](#общий-обзор)
2. [Архитектура LM](#архитектура-lm)
3. [Категории метрик](#категории-метрик)
4. [Подробное описание метрик](#подробное-описание-метрик)
5. [Использование API](#использование-api)
6. [Примеры запросов](#примеры-запросов)
7. [Руководство по развертыванию](#руководство-по-развертыванию)

---

## Общий обзор

**LM (Learning/Logic Model)** — это аналитический слой системы OperatorBot, который трансформирует сырые данные о звонках в actionable метрики для принятия управленческих решений.

### Что делает LM?

- **Статистический анализ**: вычисляет нормализованные метрики по звонкам, операторам, услугам
- **Прогнозирование**: предсказывает конверсию, повторные звонки, риски
- **Оценка качества**: оценивает работу операторов и соблюдение скриптов
- **Управление рисками**: выявляет риски оттока клиентов и жалоб

### Ключевые особенности

- ✅ **18 метрик** в 6 категориях
- ✅ **Строгая типизация** с TypedDict для каждой группы метрик
- ✅ **Rule-based расчет** с возможностью перехода на ML
- ✅ **Batch обработка** через фоновый worker
- ✅ **Интеграция** с существующими сервисами

---

## Архитектура LM

```
┌─────────────────┐
│  call_history   │
│  call_scores    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   LM Service    │ ← Расчет метрик
│  (lm_service.py)│
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   lm_value      │ ← Хранение результатов
│  (таблица БД)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Metrics Service │ ← Агрегация и отчеты
│ Weekly Quality  │
└─────────────────┘
```

### Компоненты системы

1. **База данных** (`lm_value`):
   - Хранит вычисленные метрики
   - Foreign keys к `call_history` и `call_scores`
   - Индексы для быстрых запросов

2. **LM Service** (`app/services/lm_service.py`):
   - Расчет всех категорий метрик
   - Валидация диапазонов значений
   - Batch операции

3. **LM Repository** (`app/db/repositories/lm_repository.py`):
   - CRUD операции с `lm_value`
   - Агрегация и статистика
   - Batch сохранение

4. **LM Worker** (`app/workers/lm_calculator_worker.py`):
   - Фоновая обработка звонков
   - Backfill исторических данных
   - Scheduled задачи

---

## Категории метрик

LM вычисляет метрики в 6 категориях:

| Категория | metric_group | Метрик | Назначение |
|-----------|--------------|--------|------------|
| **Операционные** | `operational` | 3 | Нагрузка, скорость, эффективность |
| **Конверсионные** | `conversion` | 3 | Бизнес-метрики, конверсия, потери |
| **Качество** | `quality` | 3 | Оценка работы операторов |
| **Риски** | `risk` | 3 | Риск оттока, жалобы, «Нужно перезвонить» |
| **Прогнозы** | `forecast` | 3 | Вероятности будущих событий |
| **Вспомогательные** | `aux` | 2+ | Метаданные LM |

---

## Матрица ответственности, логики и отчетов

> Колонки: `Ответственный блок` — кто поддерживает формулу и данные; `Поверхности` — где пользователь сталкивается с метрикой (дашборды/отчеты/боты). Все вычисления живут в `app/services/lm_service.py` и исполняются `LM Worker` при записи новой строки `call_history`.

### Operational

| metric_code | Расчет / входы | Ответственный блок | Поверхности |
|:-----------:|----------------|--------------------|-------------|
| `response_speed_score` | `_calculate_response_speed`<br>Вход: `call_history.talk_duration` (бинарное 85/20) | LM Service → `calculate_operational_metrics`<br>Ops Analytics — SLA | Daily Telegram digest «Операционный блок»<br>Grafana борд `LM Ops` |
| `talk_time_efficiency` | `_calculate_talk_efficiency`<br>Вход: `talk_duration` (≤30 сек → ×2, >30 → /3) | LM Service<br>LM Platform поддерживает формулы | Weekly Ops dashboard<br>API `metrics_service.get_lm_enhanced_metrics` |
| `queue_impact_index` | `_calculate_queue_impact`<br>Вход: `talk_duration` (нормализация к 300 сек) | LM Service<br>NOC наблюдает нагрузку | Alerting бот «Очередь»<br>Grafana «Load» |

### Conversion

| metric_code | Расчет / входы | Ответственный блок | Поверхности |
|:-----------:|----------------|--------------------|-------------|
| `conversion_score` | `_calculate_conversion_score`<br>`call_scores.outcome`/`call_category` → 10/20/50/100 | LM Service + команда продаж (справочник категорий) | Telegram отчёты по операторам<br>CRM-виджет «Воронка» |
| `lost_opportunity_score` | `_calculate_lost_opportunity`<br>`call_scores.is_target` + факт записи | LM Service<br>Retention squad сверяет целевые звонки | Ретаргет-выгрузки<br>Презентация «Lost Leads» |
| `cross_sell_potential` | `_calculate_cross_sell_potential`<br>`outcome` + `requested_service` | LM Service<br>Продукт допродаж | Outbound-бот кампаний<br>Web-дэшборд «Upsell» |

### Quality

| metric_code | Расчет / входы | Ответственный блок | Поверхности |
|:-----------:|----------------|--------------------|-------------|
| `checklist_coverage_ratio` | `_calculate_checklist_coverage`<br>`call_scores.number_checklist` → 0..100 | LM Service<br>QA-служба ведёт чек-лист | Weekly quality бот<br>Отчёт «Checklist heatmap» |
| `normalized_call_score` | `_calculate_normalized_score`<br>`call_scores.call_score` → шкала 0..100 | LM Service<br>QA отвечает за первичный скор | Telegram weekly quality report<br>PowerBI «Quality overview» |
| `script_risk_index` | `_calculate_script_risk`<br>`call_score` пороги + категории «Жалоба/Отмена» | LM Service<br>QA реагирует на превышения | Alert «Script risk» в боте супервайзера<br>QA backlog |

### Risk

| metric_code | Расчет / входы | Ответственный блок | Поверхности |
|:-----------:|----------------|--------------------|-------------|
| `churn_risk_level` | `_calculate_churn_risk`<br>`call_category`, `outcome`, `refusal_reason` → score+label | LM Service<br>Retention squad обрабатывает high-risk | Telegram алерт «Риск оттока»<br>CRM лейбл `LM_churn_high` |
| `complaint_risk_flag` | `_calculate_complaint_risk`<br>`call_category` + `call_score` → флаг/скор | LM Service<br>Служба качества | Бот жалоб<br>Ежедневный отчёт «Complaint watch» |
| `followup_needed_flag` | `_calculate_followup_needed`<br>`outcome` + лидовые категории | LM Service<br>Отдел обзвона | Task-бот «Нужно перезвонить»<br>Вкладка «Задачи» |

### Forecast

| metric_code | Расчет / входы | Ответственный блок | Поверхности |
|:-----------:|----------------|--------------------|-------------|
| `conversion_prob_forecast` | `_forecast_conversion_probability`<br>Эвристика 0.05–1.0 (`outcome`, `is_target`) | LM Service<br>Продуктовая аналитика валидирует коэффициенты | Модуль планирования продаж<br>Отчёт «Pipeline forecast» |
| `second_call_prob` | `_forecast_second_call_probability`<br>`call_category`/`outcome` → 0.15–0.60 | LM Service<br>Workforce management | Планировщик очереди<br>Telegram «нагрузка завтра» |
| `complaint_prob` | `_forecast_complaint_probability`<br>`call_category`/`call_score` → 0.05–1.0 | LM Service<br>Служба качества | QA приоритизация<br>Alert «Complaint probability» |

### Auxiliary

| metric_code | Расчет / входы | Ответственный блок | Поверхности |
|:-----------:|----------------|--------------------|-------------|
| `lm_version_tag` | `calculate_auxiliary_metrics`<br>Берёт `LMService.lm_version` | LM Platform team | API (`lm_value`), логи LM Worker, отчёты A/B |
| `calc_profile` | `_determine_calc_profile`<br>`call_history.call_date` → `default/night_shift/weekend` | LM Platform team | Диагностика пересчётов<br>Grafana фильтры «Context» |

---

## Подробное описание метрик

### 1. Операционные метрики (`operational`)

#### 1.1. `response_speed_score`

**Описание**: Скаляр 0‑100, вычисляется только из `call_history.talk_duration`. Любое значение >0 трактуется как факт взятого звонка и фиксируется как 85, нулевое/отрицательное время — как 20.

**Формула**:
```python
if talk_duration > 0:
    score = 85.0  # Звонок принят
else:
    score = 20.0  # Звонок пропущен
```

**Диапазон**: 0-100 (выше = лучше)

**Использование**: Быстрый CAT-индекс “ответили или нет” без привязки к SLA. Значение используют в мониторинге очереди и при алертах.

**Пример**: `talk_duration=0` → 20; `talk_duration=45` → 85.

---

#### 1.2. `talk_time_efficiency`

**Описание**: Скаляр 0‑100, вход: `call_history.talk_duration` (секунды). До 30 секунд рассчитывается линейный отклик `duration*2`, далее — нормализация `duration/3` с верхней границей 100.

**Формула**:
```python
if talk_duration > 30:
    efficiency = min(100.0, talk_duration / 3.0)
else:
    efficiency = talk_duration * 2.0
```

**Диапазон**: 0-100 (выше = продуктивнее)

**Использование**: Простая оценка “длинный vs короткий разговор” для операционных срезов. Используется при triage звонков на “краткие/длинные”.

**Пример**: `talk_duration=150` → 50; `talk_duration=20` → 40.

---

#### 1.3. `queue_impact_index`

**Описание**: Скаляр 0‑100, вход — `call_history.talk_duration`. Формула нормализует длительность к пяти минутам (`300 cек = 100`).

**Формула**:
```python
impact = min(100.0, (talk_duration / 300) * 100)  # 5 минут = 100
```

**Диапазон**: 0-100 (выше = больше нагрузка)

**Использование**: Мгновенный прокси нагрузки на линию для планировщика смен.

**Пример**: `talk_duration=75` → 25; `talk_duration=420` → 100 (обрезано).

---

### 2. Конверсионные метрики (`conversion`)

#### 2.1. `conversion_score`

**Описание**: Категориальный скор 10/20/50/100. Используются поля `call_scores.outcome` и `call_scores.call_category`: прямые записи и категории “Запись” дают 100, лиды без записи — 50, инфо/навигация — 20, всё остальное — 10.

**Формула**:
```python
if outcome == 'record' or category == 'Запись на услугу (успешная)':
    score = 100.0
elif outcome == 'lead_no_record' or category == 'Лид (без записи)':
    score = 50.0
elif category in ['Навигация', 'Информационный']:
    score = 20.0
else:
    score = 10.0
```

**Диапазон**: 0-100 (выше = выше вероятность)

**Использование**: Однозначный признак статуса лида для отчётов и алертов, без вероятностной модели. Не привязан к суммам чека.

**Пример**: `outcome='record'` → 100; `call_category='Навигация'` → 20.

---

#### 2.2. `lost_opportunity_score`

**Описание**: Бинарный скор (0/80/20) на входах `call_scores.is_target` и `call_scores.outcome`. Если целевой звонок не сконвертировался — 80, если целевой и записали — 0, нецелевые звонки — 20.

**Формула**:
```python
if is_target == 1 and outcome != 'record':
    score = 80.0  # Целевой звонок потерян
elif is_target == 1:
    score = 0.0   # Целевой звонок конвертирован
else:
    score = 20.0  # Нецелевой звонок
```

**Диапазон**: 0-100 (выше = больше потеря)

**Использование**: Быстрая сегментация потерянных целевых лидов для досозвона/retarget.

---

#### 2.3. `cross_sell_potential`

**Описание**: Эвристический скор (10/40/70) на входах `call_scores.outcome` и `call_scores.requested_service`. Фиксирует факт интереса к услугам.

**Формула**:
```python
if outcome == 'record':
    potential = 70.0  # Уже записался - высокий потенциал
elif requested_service:
    potential = 40.0  # Проявил интерес
else:
    potential = 10.0  # Низкий потенциал
```

**Диапазон**: 0-100

**Использование**: Источник сегмента для outbound-кампаний по апсейлу.

---

### 3. Метрики Качества (`quality`)

#### 3.1. `checklist_coverage_ratio`

**Описание**: Нормализованный процент выполнения чек-листа. Единственный вход — `call_scores.number_checklist`. Отсутствие значения интерпретируем как 50.

**Формула**:
```python
if number_checklist is None:
    coverage = 50.0  # Default
else:
    # Предполагаем максимум 10 пунктов
    coverage = min(100.0, (number_checklist / 10.0) * 100)
```

**Диапазон**: 0-100 (выше = лучше покрытие)

**Использование**: Метрика “процент покрытых пунктов” для ревью операторов.

---

#### 3.2. `normalized_call_score`

**Описание**: Нормализация `call_scores.call_score` в диапазон 0‑100. Если исходная шкала 0‑10, домножаем на 10, иначе ограничиваем 0..100.

**Формула**:
```python
if call_score <= 10:
    normalized = call_score * 10.0  # Шкала 0-10 → 0-100
else:
    normalized = call_score          # Уже 0-100

return max(0.0, min(100.0, normalized))
```

**Диапазон**: 0-100 (выше = выше качество)

**Использование**: Нормализация гетерогенных источников `call_score` перед агрегацией и ML.

---

#### 3.3. `script_risk_index`

**Описание**: Эвристический риск 10-100. Используются `call_scores.call_score` и `call_scores.call_category`. Низкий скор и категории “Жалоба/Отмена” повышают риск.

**Формула**:
```python
if score <= 3:
    risk = 80.0
elif score <= 5:
    risk = 50.0
elif score <= 7:
    risk = 30.0
else:
    risk = 10.0

if category in ['Жалоба', 'Отмена записи']:
    risk = min(100.0, risk + 20.0)  # Повышаем риск
```

**Диапазон**: 0-100 (выше = выше риск)

**Использование**: Фильтр на звонки, требующие прослушки супервайзером.

---

### 4. Метрики Рисков (`risk`)

#### 4.1. `churn_risk_level`

**Описание**: Категориальный скор “low/medium/high” плюс числовая оценка 0‑100. Входы: `call_scores.call_category`, `call_scores.outcome`, `call_scores.refusal_reason`.

**Формула**:
```python
if category == 'Жалоба':
    score = 90.0  # level = 'high'
elif category == 'Отмена записи':
    score = 70.0  # level = 'high'
elif outcome in ['refusal', 'no_interest']:
    score = 60.0  # level = 'medium'
elif refusal_reason:
    score = 50.0  # level = 'medium'
elif outcome == 'record':
    score = 10.0  # level = 'low'
else:
    score = 30.0  # level = 'low'

# Определение уровня:
# score >= 70 → 'high'
# score >= 40 → 'medium'
# score < 40 → 'low'
```

**Значения**: 
- `value_label`: 'low' | 'medium' | 'high'
- `value_numeric`: 0-100

**Использование**: Маршрутизация кейсов в retention/качество и настройка алертов.

---

#### 4.2. `complaint_risk_flag`

**Описание**: Пороговый индикатор жалобы с числовым риском. Исп. `call_scores.call_category` и `call_scores.call_score`. Жалоба → 100, низкое качество → 60, отмена → 40, иначе 10; флаг `score>=50`.

**Формула**:
```python
if category == 'Жалоба':
    score = 100.0
elif call_score < 3:
    score = 60.0
elif category == 'Отмена записи':
    score = 40.0
else:
    score = 10.0

flag = score >= 50.0  # true/false
```

**Значения**:
- `value_label`: 'true' | 'false'
- `value_numeric`: 0-100

**Использование**: Автопометка звонков в системах качества/CRM.

---

#### 4.3. `followup_needed_flag`

**Описание**: Булев признак, построенный на `call_scores.outcome` и `call_scores.call_category`. `lead_no_record` и категории “Жалоба/Лид” переводят флаг в `true`.

**Формула**:
```python
if outcome == 'lead_no_record':
    return True
elif category in ['Жалоба', 'Лид (без записи)']:
    return True
else:
    return False
```

**Значения**:
- `value_label`: 'true' | 'false'
- `value_numeric`: 1.0 | 0.0

**Использование**: Триггер для постановки задач в CRM/таск-трекере без ручной разметки.

---

### 5. Прогнозные метрики (`forecast`)

#### 5.1. `conversion_prob_forecast`

**Описание**: Детерминированная вероятность 0.05–1.0. Используются `call_scores.outcome` и `call_scores.is_target`. Значения не ML, а простая эвристика.

**Формула**:
```python
if outcome == 'record':
    prob = 1.0    # Уже конвертирован
elif outcome == 'lead_no_record':
    prob = 0.35   # 35% шанс конверсии позже
elif is_target == 1:
    prob = 0.20   # 20% шанс
else:
    prob = 0.05   # 5% шанс
```

**Диапазон**: 0.0-1.0 (вероятность)

**Использование**: Upper-bound прогноз числа записей при batch-планировании.

---

#### 5.2. `second_call_prob`

**Описание**: Эвристика 0.15–0.60 по `call_scores.call_category` и `call_scores.outcome`. Навигация/инфо и незавершённые лиды дают повышенную вероятность.

**Формула**:
```python
if category in ['Навигация', 'Информационный']:
    prob = 0.60  # 60% позвонят снова
elif outcome == 'lead_no_record':
    prob = 0.45  # 45%
elif outcome == 'record':
    prob = 0.15  # 15%
else:
    prob = 0.25  # 25%
```

**Диапазон**: 0.0-1.0

**Использование**: Грубая оценка входящей нагрузки на ближайшие слоты расписания.

---

#### 5.3. `complaint_prob`

**Описание**: Эвристика 0.05–1.0 по `call_scores.call_category` и `call_scores.call_score`. Жалоба — 1.0, низкий скор — 0.4, отмена — 0.25, прочее — 0.05.

**Формула**:
```python
if category == 'Жалоба':
    prob = 1.0   # Уже жалоба
elif call_score < 3:
    prob = 0.40  # 40%
elif category == 'Отмена записи':
    prob = 0.25  # 25%
else:
    prob = 0.05  # 5%
```

**Диапазон**: 0.0-1.0

**Использование**: Флаг для приоритизации исходящих контактов от retention/QA.

---

### 6. Вспомогательные метрики (`aux`)

#### 6.1. `lm_version_tag`

**Описание**: Строковый тег из `LMService.lm_version`, сохраняется вместе с каждой записью.

**Значение**: Строка (например, 'lm_v1', 'lm_v2')

**Использование**: Диагностика регрессий между версиями правил и аудит batch-пересчётов.

---

#### 6.2. `calc_profile`

**Описание**: Значение из набора `default_v1/night_shift_v1/weekend_v1`, вычисляется по `call_history.call_date`. Помогает отслеживать, какое правило применялось.

**Формула**:
```python
if hour >= 22 or hour < 6:
    profile = 'night_shift_v1'
elif weekday >= 5:  # Суббота/Воскресенье
    profile = 'weekend_v1'
else:
    profile = 'default_v1'
```

**Значения**: 'default_v1' | 'night_shift_v1' | 'weekend_v1'

**Использование**: Маркёр для последующего сравнения метрик между сменами и праздничными днями.

---

## Использование API

### Расчет метрик для звонка

```python
from app.db.manager import DatabaseManager
from app.db.repositories.lm_repository import LMRepository
from app.services.lm_service import LMService

# Инициализация
db_manager = DatabaseManager(DB_CONFIG)
lm_repo = LMRepository(db_manager)
lm_service = LMService(lm_repo, lm_version="lm_v1")

# Расчет всех метрик
saved_count = await lm_service.calculate_all_metrics(
    history_id=12345,
    call_history=call_history_data,
    call_score=call_score_data,
    calc_source="manual_calculation"
)

print(f"Сохранено {saved_count} метрик")
```

### Получение метрик звонка

```python
# Получить все метрики звонка
metrics = await lm_repo.get_lm_values_by_call(history_id=12345)

for metric in metrics:
    print(f"{metric['metric_code']}: {metric['value_numeric']}")
```

### Агрегация метрик

```python
from datetime import datetime, timedelta

# Статистика по conversion_score за неделю
stats = await lm_repo.get_metric_statistics(
    metric_code='conversion_score',
    start_date=datetime.now() - timedelta(days=7),
    end_date=datetime.now()
)

print(f"Среднее: {stats['avg_value']}")
print(f"Минимум: {stats['min_value']}")
print(f"Максимум: {stats['max_value']}")
print(f"Звонков: {stats['count']}")
```

### Использование Worker

```python
from app.workers.lm_calculator_worker import LMCalculatorWorker

worker = LMCalculatorWorker(db_manager)

# Обработать звонки за последние 24 часа
processed = await worker.process_recent_calls(
    hours_back=24,
    batch_size=100,
    skip_existing=True
)

print(f"Обработано {processed} звонков")
```

---

## Примеры запросов

### SQL: Найти звонки с высоким риском оттока

```sql
SELECT 
    lv.history_id,
    ch.caller_number,
    ch.call_date,
    lv.value_label as risk_level,
    lv.value_numeric as risk_score
FROM lm_value lv
JOIN call_history ch ON ch.history_id = lv.history_id
WHERE lv.metric_code = 'churn_risk_level'
  AND lv.value_label = 'high'
  AND lv.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
ORDER BY lv.value_numeric DESC
LIMIT 50;
```

### SQL: Средняя конверсия по операторам

```sql
SELECT 
    SUBSTRING_INDEX(ch.called_info, ' ', 1) as operator_ext,
    AVG(lv.value_numeric) as avg_conversion,
    COUNT(*) as calls_count
FROM lm_value lv
JOIN call_history ch ON ch.history_id = lv.history_id
WHERE lv.metric_code = 'conversion_score'
  AND lv.created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)
GROUP BY operator_ext
ORDER BY avg_conversion DESC;
```

### SQL: Распределение рисков по дням

```sql
SELECT 
    DATE(lv.created_at) as date,
    lv.value_label as risk_level,
    COUNT(*) as count
FROM lm_value lv
WHERE lv.metric_code = 'churn_risk_level'
  AND lv.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
GROUP BY DATE(lv.created_at), lv.value_label
ORDER BY date DESC, risk_level;
```

---

## Руководство по развертыванию

### 1. Миграция БД

```bash
# Применить миграцию
mysql -u username -p database_name < db/migrations/001_create_lm_value_table.sql

# Проверить создание таблицы
mysql -u username -p database_name -e "DESCRIBE lm_value;"
```

###  Backfill исторических данных

```python
from datetime import datetime, timedelta
from app.workers.lm_calculator_worker import LMCalculatorWorker

# Инициализация
db_manager = DatabaseManager(DB_CONFIG)
await db_manager.initialize()
worker = LMCalculatorWorker(db_manager)

# Backfill за последние 30 дней
await worker.backfill_all_calls(
    start_date=datetime.now() - timedelta(days=30),
    end_date=datetime.now(),
    batch_size=500
)
```

### 3. Настройка Cron для автоматического расчета

```bash
# /etc/cron.d/lm_calculator
# Запуск каждый час
0 * * * * cd /path/to/operabot && /usr/bin/python3 app/workers/lm_calculator_worker.py >> /var/log/lm_worker.log 2>&1
```

### 4. Мониторинг

Проверяйте покрытие метриками:

```sql
-- Процент звонков с LM метриками
SELECT 
    (SELECT COUNT(DISTINCT history_id) FROM lm_value) as calls_with_lm,
    (SELECT COUNT(*) FROM call_history WHERE call_date >= DATE_SUB(NOW(), INTERVAL 7 DAY)) as total_calls,
    ROUND((SELECT COUNT(DISTINCT history_id) FROM lm_value) / 
          (SELECT COUNT(*) FROM call_history WHERE call_date >= DATE_SUB(NOW(), INTERVAL 7 DAY)) * 100, 2) as coverage_percent;
```

---

## Техническая поддержка

Для вопросов по LM системе:
- Код: `app/services/lm_service.py`
- Тесты: `tests/unit/test_lm_service.py`
- Документация: Этот файл

Поддержка: команда разработки OperatorBot
