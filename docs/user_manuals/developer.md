# Руководство для Разработчика

Цель: понятные и короткие инструкции — как быстро поднять проект, где править логику оценок и что проверять при неисправностях.

---

## Быстрый старт локально (5–10 минут)
1. Клонируйте репозиторий и перейдите в папку проекта:
   - git clone ... && cd operabot
2. Установите зависимости:
   - pip install -r requirements.txt
3. Создайте копию файла окружения и заполните:
   - cp .env.example .env
   - Заполните TELEGRAM_TOKEN, DB_*, SUPREME_ADMIN_IDS, REDIS_URL (если есть).
4. Запустите Redis для разработки (локально):
   - docker run -d --name operabot_redis -p 6379:6379 redis:7-alpine
   - В .env укажите: REDIS_URL=redis://localhost:6379/0
5. Примените миграции в базу (MySQL):
   - mysql -u user -p dbname < migrations/run_migrations.sql
6. Запустите бота:
   - python -m app.main
7. Откройте Telegram → /start от пользователя с SUPREME_ADMIN_ID, затем /admin для доступа к админке.

---

## Структура кода — что где лежит (коротко)
- app/main.py — точка входа (запуск бота).
- app/config.py — читается .env.
- app/telegram/ — все Telegram-хендлеры и UI.
  - app/telegram/handlers/* — обработчики команд и меню.
  - app/telegram/ui/* — экраны и клавиатуры.
- app/services/ — бизнес-логика.
  - lm_service.py — расчёт/парсинг LM-метрик.
  - lm_rules.py — правила, пороги и тексты (главное место для правок).
- app/db/repositories/ — доступ к данным (LM, users, admin и т.д.).
- app/workers/ — фоновые задачи (lm_calculator_worker.py).
- docs/ — документация, включая мануалы и runbooks.

---

## LM: где менять правила и пороги
1. Файл: `app/services/lm_rules.py`
   - Здесь задаются тексты "Почему в списке" и пороги для светофоров.
   - Примеры: complaint_risk_threshold, followup_needed критерии.

2. Как добавить простое правило (пример)
```python
# в app/services/lm_rules.py
EVIDENCE_RULES.append({
    "code": "too_many_silences",
    "condition": lambda call: call.get("silence_secs", 0) > 20,
    "label": "Оператор молчал долго",
    "advice": "Попросите оператора сократить паузы, проведите тренинг"
})
```
После добавления — перезапустите worker/бот (чтобы новые правила применились к новым расчётам).

3. Где парсится результат:
   - `app/services/lm_service.py` — функции `_parse_result_subscores` и `calculate_*`. Меняйте осторожно; добавляйте тесты.

---

## Как добавить новую метрику LM
1. Добавьте код метрики и логику расчёта в lm_service.py.
2. Зарегистрируйте метрику в месте, где формируется список метрик (см. lm_repository).
3. Напишите unit-тест (tests/unit/*) для новой метрики.
4. Запустите тесты: `pytest tests/unit/test_lm_service.py -q`

---

## Запуск фоновых задач
- Для пересчёта LM:
  - `python -m app.workers.lm_calculator_worker` — пример запуска worker (в зависимости от способа инвокации).
- В проде worker запускается через процесс-менеджер (systemd, supervisor, docker-compose). Смотрите docker-compose.yml в корне.

---

## Тесты и проверка
- Запустить все тесты:
  - pytest -q
- Запустить тесты по модулю:
  - pytest tests/unit/test_lm_service.py -q
- Писать тесты: новые тесты добавляйте рядом в tests/unit/ с понятными именами.

---

## Логирование и отладка
- Логи настраиваются в `app/logging_config.py`.
- Ошибки по обработке команд и хэндлеров логируются и обычно видны в STDOUT при запуске.
- Для проблем с admin callback (длинные callback_data) проверьте Redis — system использует сохранение mappings в Redis.

---

## Частые ошибки и как их диагностировать
1. Бот "не отвечает" на команды
   - Проверьте, запущен ли процесс `python -m app.main`.
   - Проверьте TELEGRAM_TOKEN в .env.
2. Кнопки админки не работают / некорректны
   - Проверьте REDIS_URL и доступность Redis.
   - Проверьте что admin callbacks пишутся в Redis (ошибки в логах).
3. LM-метрики не считаются
   - Проверьте статус worker'ов и их логи.
   - Убедитесь, что в таблице `call_history` есть новые записи и worker обрабатывает их.
4. Миграции не применились
   - Проверьте права MySQL-пользователя и примените migrations/run_migrations.sql вручную.

---

## Runbook — поднять Redis в dev (коротко)
1. Локально (Docker):
   - docker run -d --name operabot_redis -p 6379:6379 redis:7-alpine
2. Проверка:
   - redis-cli PING  -> PONG
3. В .env установить:
   - REDIS_URL=redis://localhost:6379/0
4. Перезапустить бота:
   - pkill -f "python -m app.main" || true
   - python -m app.main

---

## Как формировать тикет в dev (что указывать)
- Коротко:
  - Заголовок: [LM|bug] Короткое описание
  - Тело:
    - ID звонка (history_id), дата/время
    - Что вы увидели (что не так)
    - Скрин/транскрипт (если есть)
    - Ожидаемое поведение
    - Приоритет (низкий/средний/высокий)
- Пример:
  - ID 12345, 2025-12-22 11:12 — complaint incorrectly triggered due to wrong transcription. Please check rule complaint_risk.

---

## Полезные команды (CLI)
- Запустить тесты: `pytest -q`
- Запустить бот: `python -m app.main`
- Применить миграции: `mysql -u user -p dbname < migrations/run_migrations.sql`
- Старт Redis локально: `docker run -d --name operabot_redis -p 6379:6379 redis:7-alpine`

---

Конец руководства. Если нужно — могу сделать отдельный файл runbook с расширенными командами для деплоя и rollback.
