# Testing Checklist for New Services

## Дата: 04.12.2024

---

## ✅ Syntax Validation

**Компиляция файлов:**
- ✅ `app/db/repositories/roles.py`
- ✅ `app/services/admin_logger.py`
- ✅ `app/services/dashboard_cache.py`
- ✅ `app/telegram/handlers/sync_analytics.py`
- ✅ `app/telegram/handlers/start.py`
- ✅ `app/telegram/handlers/help.py`
- ✅ `app/telegram/utils/keyboard_builder.py`
- ⏳ `app/db/repositories/call_analytics_repo.py`
- ⏳ `app/services/call_analytics_sync.py`

---

## 📋 Automated Tests

**Тестовый скрипт:** `tests/test_new_services.py`

### Тесты:

#### TEST 1: RolesRepository
- [ ] Import успешен
- [ ] `get_role_by_id()` работает
- [ ] `get_all_roles()` возвращает список
- [ ] `check_permission()` корректно проверяет
- [ ] `get_user_permissions()` возвращает dict

#### TEST 2: AdminActionLogger
- [ ] Import успешен
- [ ] `log_action()` записывает в БД
- [ ] `get_recent_logs()` извлекает логи
- [ ] Specific methods работают (approval, role_change)

#### TEST 3: CallAnalyticsRepository
- [ ] Import успешен
- [ ] `get_call_count()` возвращает число
- [ ] `get_operators_list()` возвращает список
- [ ] `get_aggregated_metrics()` возвращает метрики

#### TEST 4: CallAnalyticsSyncService
- [ ] Import успешен
- [ ] `get_sync_status()` показывает статус
- [ ] `sync_new()` синхронизирует данные

#### TEST 5: DashboardCacheService
- [ ] Import успешен
- [ ] `save_dashboard_cache()` сохраняет
- [ ] `get_cached_dashboard()` читает
- [ ] `invalidate_cache()` очищает

#### TEST 6: AnalyticsRepository (Updated)
- [ ] `call_analytics_repo` интегрирован
- [ ] `save_operator_recommendations()` работает
- [ ] `get_operator_recommendations()` читает

#### TEST 7: Handlers Import
- [ ] SyncAnalyticsHandler импортируется
- [ ] StartHandler импортируется
- [ ] HelpHandler импортируется

#### TEST 8: KeyboardBuilder
- [ ] Import успешен
- [ ] `build_main_keyboard()` создает клавиатуру
- [ ] `build_reports_menu()` создает меню
- [ ] Other menus создаются

---

## 🔍 Manual Testing

### 1. Database Connectivity
```bash
# Проверить подключение к БД
psql -U user -d database -c "SELECT 1;"
```

### 2. Синхронизация
```bash
# В боте:
/sync_analytics status
/sync_analytics  # инкрементальная
```

**Ожидаемый результат:**
- Показывает статус синхронизации
- Синхронизирует новые звонки
- Логи в watchdog

### 3. Start Command
```bash
/start
```

**Ожидаемый результат:**
- Короткое сообщение по роли
- Reply клавиатура с кнопками
- Разные тексты для разных ролей

### 4. Help Command
```bash
/help
```

**Ожидаемый результат:**
- Блочная структура
- Разделы в зависимости от прав
- Краткое содержание

### 5. Dashboard
```bash
# Нажать кнопку "📊 Отчёты" или "📊 Моя статистика"
```

**Ожидаемый результат:**
- Быстрая загрузка (кеш работает)
- Кнопка "Обновить" инвалидирует кеш
- Данные из call_analytics

---

## 🐛 Known Issues to Check

### Potential Bugs:

1. **Import Errors:**
   - [ ] Все imports правильные
   - [ ] Нет circular imports
   - [ ] Пути к модулям корректны

2. **Database:**
   - [ ] Таблицы существуют (roles_reference, operator_dashboards, etc)
   - [ ] ML поля в call_scores существуют
   - [ ] call_analytics синхронизирована

3. **Permissions:**
   - [ ] Supreme/Dev admin корректно определяются
   - [ ] Роли из roles_reference работают
   - [ ] can_* флаги правильно проверяются

4. **Caching:**
   - [ ] TTL 5 минут работает
   - [ ] Invalidation очищает кеш
   - [ ] UPSERT не создает дубликатов

5. **Logging:**
   - [ ] Все логи с префиксами [SERVICE]
   - [ ] Exception tracebacks полные
   - [ ] Уровни логирования правильные

---

## 🚨 Critical Paths to Test

### Path 1: Новый пользователь
1. /start → незарегистрирован
2. /register
3. Админ одобряет
4. /start → видит клавиатуру

### Path 2: Оператор смотрит статистику
1. Оператор: /start
2. Нажимает "📊 Моя статистика"
3. Видит свои метрики
4. Данные из call_analytics

### Path 3: Админ управляет пользователями
1. Админ: /start
2. "👥 Пользователи и роли"
3. "Ожидают одобрения"
4. Одобряет → логируется в admin_action_logs

### Path 4: SuperAdmin синхронизирует
1. SuperAdmin: /sync_analytics status
2. Видит статус
3. /sync_analytics full (если нужно)
4. Проверяет логи

---

## 📊 Performance Metrics

**До изменений:**
- Dashboard load: ~2-5сек (call_scores direct)
- Агрегации: медленные GROUP BY

**После изменений (ожидаемо):**
- Dashboard load: ~0.2-0.5сек (cache hit)
- Dashboard load: ~0.5-1сек (call_analytics, cache miss)
- Агрегации: ~10-100x быстрее

**Проверить:**
- [ ] Dashboard быстрее
- [ ] Кеш работает
- [ ] call_analytics быстрее call_scores

---

## 🔧 Edge Cases

### Edge Case 1: call_analytics пуст
- analytics.py должен fallback на call_scores  
- [ ] Fallback работает
- [ ] Логи предупреждают

### Edge Case 2: Пользователь без роли
- [ ] Используется default (role_id=1)
- [ ] Не крашится

### Edge Case 3: TTL истёк
- [ ] get_cached_dashboard возвращает None
- [ ] Пересчет происходит
- [ ] Новый кеш сохраняется

### Edge Case 4: Concurrent updates
- [ ] UPSERT не создает дубликатов
- [ ] Last write wins

---

## 📝 Next Steps

1. ✅ Запустить `tests/test_new_services.py`
2. Проверить каждый failed test
3. Добавить missing exception handling
4. Улучшить логирование где нужно
5. Fix bugs
6. Re-test
7. Deploy

---

**Тестировщик:** Antigravity AI + User  
**Статус:** In Progress 🔍
