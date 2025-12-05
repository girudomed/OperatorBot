# КРИТИЧНАЯ НАХОДКА: Несоответствие Миграций и Кода

## Дата: 04.12.2024

---

## Проблема

Согласно документу `правки_без_миграций прошлых`, **миграции НЕ были применены к БД**.

Однако код (который мы создали ранее) **ожидает наличие** таблиц из миграции `003_ml_analytics.sql`:

### Миграция 003 Создает:

1. **ML поля в call_scores:**
   - `ml_p_record` (вероятность записи)
   - `ml_score_pred` (прогноз оценки)
   - `ml_p_complaint` (риск жалобы)
   - `ml_updated_at` (время прогноза)

2. **Таблица operator_dashboards** (кеш дашбордов)
3. **Таблица operator_recommendations** (LLM рекомендации)

### Код Использует Эти Таблицы:

**`app/services/dashboard_cache.py`:**
- Строка 40: `SELECT FROM operator_dashboards`
- Строка 71: `INSERT INTO operator_dashboards`
- 6 других мест

**`app/db/repositories/analytics.py`:**
- Строка 523: `INSERT INTO operator_recommendations`
- Строка 547: `SELECT FROM operator_recommendations`

---

## Текущее Состояние БД

### ✅ Существуют:
- `call_scores` (БЕЗ ml_* полей)
- `users` (Mango справочник)
- `UsersTelegaBot` (роли Telegram)
- `roles_reference` (справочник ролей)
- `admin_action_logs`
- `RolesTelegaBot`

### ❌ НЕ Существуют:
- ML поля в `call_scores`
- Таблица `operator_dashboards`
- Таблица `operator_recommendations`
- Таблица `call_analytics` (если планировалась)

---

## Варианты Решения

### Вариант 1: Применить Миграцию 003 ✅ РЕКОМЕНДУЕТСЯ

**Действия:**
```bash
mysql -u USER -p DB < migrations/003_ml_analytics.sql
```

**Плюсы:**
- Код будет работать сразу
- Кеширование дашбордов будет работать
- LLM рекомендации будут сохраняться

**Минусы:**
- Добавляет поля в call_scores (могут быть NULL)
- Новые таблицы (но это норм)

---

### Вариант 2: Отключить Кеширование и Рекомендации

**Действия:**
1. Удалить/закомментировать `dashboard_cache.py`
2. Убрать методы работы с `operator_recommendations` из `analytics.py`
3. Dashboard будет работать "на лету" из `call_scores`
4. Рекомендации не будут сохраняться

**Плюсы:**
- Не меняем БД

**Минусы:**
- Медленнее (каждый раз запрос к call_scores)
- Нет сохранения рекомендаций

---

### Вариант 3: Условное Использование (С Проверками)

**Действия:**
1. Добавить try-except в dashboard_cache.py
2. Если таблица не существует - работать без кеша
3. То же для recommendations

**Плюсы:**
- Код работает с БД и без миграций

**Минусы:**
- Усложняет код
- Нужны проверки везде

---

## Рекомендация

**ПРИМЕНИТЬ МИГРАЦИЮ 003**

**Почему:**
1. Миграция не ломает существующие данные
2. Добавляет только новые колонки (NULL допустимы)
3. Новые таблицы не влияют на старые
4. Все индексы полезны для производительности
5. Код уже написан под эти таблицы

**Что НЕ произойдет:**
- Не потеряются данные
- Старые запросы продолжат работать
- call_scores останется совместимым

**Что ДОБАВИТСЯ:**
- ML поля (пока NULL, заполнятся потом)
- Кеш дашбордов (ускорит работу)
- Таблица рекомендаций (LLM сможет писать туда)

---

## Что Делать ЕСЛИ НЕ Применять Миграцию 003

### Критичные Изменения в Коде:

**1. Отключить DashboardCacheService**

В `app/telegram/handlers/dashboard.py`:
```python
# Было:
self.cache_service = DashboardCacheService(db_manager)

# Стало:
self.cache_service = None  # Отключен
```

И в `_show_single_dashboard`:
```python
# Убрать все обращения к cache_service
```

**2. Убрать методы из analytics.py**

Удалить/закомментировать:
- `save_operator_recommendations()`
- `get_operator_recommendations()`

**3. Recommendations Service**

В `app/services/recommendations.py`:
- Убрать сохранение в БД
- Генерировать "на лету" каждый раз

---

## Текущие Файлы Требующие Исправления

### Если НЕ применяем миграцию 003:

1. ❌ `app/services/dashboard_cache.py` - весь файл (не будет работать)
2. ❌ `app/telegram/handlers/dashboard.py` - убрать cache_service
3. ❌ `app/db/repositories/analytics.py` - убрать 2 метода
4. ❌ `app/services/recommendations.py` - убрать сохранение в БД

### Если ПРИМЕНЯЕМ миграцию 003:

✅ Все файлы работают как есть!

---

## Мое Предложение

**ПРИМЕНИТЬ МИГРАЦИЮ 003 СЕЙЧАС**

Команда:
```bash
cd /Users/vitalyefimov/Projects/operabot
mysql -u YOUR_USER -p YOUR_DB < migrations/003_ml_analytics.sql
```

После этого продолжить исправления по остальным задачам из `правки_без_миграций прошлых`.

---

**Вопрос к Пользователю:**

Применить миграцию 003 или работать без кеширования/рекомендаций?
