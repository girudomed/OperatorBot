# OperaBot — Telegram-бот для контроля качества операторов

## 🏗️ Архитектура

```
app/
├── config.py              # Конфигурация и переменные окружения
├── main.py                # Точка входа
├── logging_config.py      # WatchDog логирование
│
├── db/                    # Слой данных
│   ├── manager.py         # DatabaseManager (aiomysql pool)
│   └── repositories/      # Репозитории
│       ├── admin.py       # AdminRepository (UsersTelegaBot)
│       ├── operators.py   # OperatorRepository (users + Mango)
│       ├── lm_repository.py  # LM метрики
│       └── users.py       # UserRepository
│
├── ml/                    # ML слой
│   ├── models.py          # CallScorer, ChurnPredictor, UpsellRecommender
│   └── pipeline.py        # MLPipeline
│
├── services/              # Бизнес-логика
│   ├── lm_service.py      # Расчёт LM метрик
│   ├── metrics_service.py # Метрики качества
│   ├── dashboard_cache.py # Кеширование дашбордов
│   └── permissions.py     # Проверка прав
│
├── telegram/              # Telegram-хендлеры
│   ├── handlers/
│   │   ├── admin_panel.py    # /admin
│   │   ├── admin_users.py    # Управление пользователями
│   │   ├── admin_admins.py   # Управление администраторами
│   │   ├── admin_lm.py       # ML аналитика
│   │   └── admin_stats.py    # Статистика
│   └── bot.py             # Инициализация бота
│
└── workers/               # Фоновые задачи
    ├── lm_calculator_worker.py  # Расчёт LM метрик
    └── task_worker.py     # Очередь задач
```

## 🗄️ База данных

| Таблица | Назначение |
|---------|------------|
| `UsersTelegaBot` | Пользователи бота (роли, статусы) |
| `users` | Операторы Mango (extension, sip_id) |
| `call_history` | История звонков |
| `call_scores` | Оценки звонков |
| `lm_value` | ML метрики |
| `operator_dashboards` | Кеш дашбордов |
| `admin_action_logs` | Логи админ-действий |

## 🚀 Запуск

```bash
# Установка зависимостей
pip install -r requirements.txt

# Настройка окружения
cp .env.example .env
# Заполнить .env

# Запуск бота
python -m app.main
```

## 📋 Переменные окружения

```env
TELEGRAM_TOKEN=...
DB_HOST=localhost
DB_USER=operabot
DB_PASSWORD=...
DB_NAME=operabot
SUPREME_ADMIN_IDS=123456789
DEV_ADMIN_IDS=987654321
```

## 🔧 Документация

- [Админ-панель](docs/ADMIN_PANEL.md)
- [LM метрики](docs/LM_СПРАВОЧНИК.md)
- [WatchDog логирование](docs/WATCHDOG_INTEGRATION.md)
