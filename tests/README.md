# Tests Documentation

## Структура тестов

```
tests/
  __init__.py
  test_db_module.py          # Тесты БД (нормализация, периоды)
  test_operator_data.py      # Тесты работы с данными операторов
  test_metrics_calculator.py # Тесты расчёта метрик
  README.md                  # Эта документация
```

## Установка зависимостей для тестов

```bash
pip install pytest pytest-asyncio
```

## Запуск тестов

### Все тесты
```bash
pytest tests/ -v
```

### Конкретный файл
```bash
pytest tests/test_db_module.py -v
```

### Конкретный тест
```bash
pytest tests/test_operator_data.py::TestDateUtils::test_validate_and_format_date_from_string -v
```

### С покрытием (если установлен pytest-cov)
```bash
pytest tests/ --cov=. --cov-report=html
```

## Покрытие тестами

### test_db_module.py (4 теста)
- ✅ Нормализация телефонов (8→7, только цифры)
- ✅ Нормализация extension (только алфавит+цифры)
- ✅ Валидация дат (формат YYYY-MM-DD)
- ✅ Парсинг периодов (daily, weekly, и т.д.)

### test_operator_data.py (11 тестов)
- ✅ Преобразование строки в datetime
- ✅ Преобразование date в datetime
- ✅ Обработка уже существующего datetime
- ✅ Обработка некорректного формата даты
- ✅ Обработка некорректного типа
- ✅ Форматирование для MySQL TIMESTAMP
- ✅ Форматирование для MySQL DATETIME
- ✅ Обработка неподдерживаемого типа MySQL
- ✅ Обработка некорректного input в format_date
- ✅ Проверка требования метода acquire в инициализации
- ✅ Успешная инициализация OperatorData

### test_metrics_calculator.py (10 тестов)
- ✅ Инициализация MetricsCalculator
- ✅ Расчёт средней оценки звонков
- ✅ Расчёт средней длительности
- ✅ Подсчёт общей длительности
- ✅ Подсчёт общей длительности с фильтром по категории
- ✅ Подсчёт успешных записей на услуги
- ✅ Подсчёт пропущенных звонков
- ✅ Расчёт конверсии в запись
- ✅ Расчёт доли отмен
- ✅ Обработка пустых данных во всех методах

**Всего: 25 unit-тестов**

## Требуется доработка

### Интеграционные тесты
- [ ] Тесты очереди задач (с реальным asyncio)
- [ ] Тесты взаимодействия с БД (с тестовой БД)

### E2E тесты
- [ ] Тесты Telegram-команд (с моками bot API)
- [ ] Тесты генерации отчётов

### CI/CD
- [ ] Настроить GitHub Actions / GitLab CI
- [ ] Автоматический запуск тестов на каждый PR
- [ ] Проверка покрытия кода

## Примеры использования

### Пример запуска с выводом
```bash
$ pytest tests/test_operator_data.py -v

tests/test_operator_data.py::TestDateUtils::test_validate_and_format_date_from_string PASSED
tests/test_operator_data.py::TestDateUtils::test_validate_and_format_date_from_date PASSED
...
======================== 11 passed in 0.05s =========================
```

### Пример с фильтрацией
```bash
# Только тесты, содержащие "normalize"
pytest tests/ -k "normalize" -v

# Только тесты, НЕ содержащие "mysql"
pytest tests/ -k "not mysql" -v
```

## Добавление новых тестов

1. Создайте новый файл `test_<module_name>.py`
2. Импортируйте pytest: `import pytest`
3. Создайте классы тестов: `class TestFeatureName:`
4. Добавьте методы тестов: `def test_specific_behavior(self):`
5. Используйте фикстуры для переиспользуемых данных
6. Запустите тесты: `pytest tests/test_<module_name>.py -v`

## Полезные команды

```bash
# Показать все доступные фикстуры
pytest --fixtures

# Пропустить медленные тесты (если помечены @pytest.mark.slow)
pytest -m "not slow"

# Остановиться на первой ошибке
pytest -x

# Показать локальные переменные при ошибке
pytest -l

# Запустить последние упавшие тесты
pytest --lf
```
