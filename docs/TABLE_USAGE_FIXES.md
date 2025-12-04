# Table Usage Fix Summary

## КРИТИЧНО: users vs UsersTelegaBot

### Правильное Разделение:

**Таблица `users` (Mango Phone System):**
- Содержит: extension, protocol, outgoing_number, sip_id, etc.
- Назначение: Справочник телефонных пользователей Mango
- НЕ содержит: role_id, status, approved_by, blocked_at
- Используется: Для связки с call_scores через extension

**Таблица `UsersTelegaBot` (Telegram Bot Users):**
- Содержит: role_id, status, approved_by, blocked_at, operator_name, extension
- Назначение: Telegram пользователи бота с ролями и правами
- Связь с users: Через поля operator_name и extension (для join)

---

## Исправленные Файлы:

### 1. ✅ app/db/repositories/users.py
**Статус:** Полностью переписан
**Изменения:**
- Все запросы переведены на `UsersTelegaBot`
- Добавлено comprehensive logging
- Методы для linking с операторами Mango
- `register_telegram_user()` вместо `register_user_if_not_exists()`
- `get_user_by_telegram_id()` вместо `get_user_by_id()`
- Новые методы: `link_operator()`, `approve_user()`, `block_user()`

### 2. ✅ app/telegram/middlewares/permissions.py
**Статус:** Исправлено
**Изменения:**
- Строка 81: `FROM users` → `FROM UsersTelegaBot`
- Строка 119: `FROM users` → `FROM UsersTelegaBot`
- Обновлены комментарии/docstrings

### 3. ✅ app/telegram/handlers/dev_messages.py  
**Статус:** Исправлено (пользователем)
**Изменения:**
- Строка 226: `FROM users` → `FROM UsersTelegaBot` ✅

---

## Файлы Требующие Внимания:

### ⚠️ Сомнительные (Нужно Проверить):

#### app/db/repositories/operators.py
**Статус:** Частично правильно
- Строки 234, 259, 275, 283, 291, etc. используют `users`
- **НО:** Это правильно! Operators repository работает с Mango данными
- Проверить: Убедиться что не запрашивает role_id из users

#### app/db/repositories/admin.py
**Статус:** КРИТИЧНО - требует полной переработки
- ~35 использований `FROM users`
- Много запросов к role_id, status, approved_by
- **Нужно:** Переписать все на `UsersTelegaBot`

#### app/telegram/middlewares/permissions_legacy.py
**Статус:** Требует проверки
- Legacy файл, возможно не используется
- Если используется - заменить на UsersTelegaBot

#### app/telegram/handlers/admin_*.py
**Статус:** Требуют проверки каждый:
- `admin_admins.py` - строка 472
- `admin_users.py` - строка 292  
- `admin_commands.py` - строки 396, 409

#### app/services/notifications.py
**Статус:** Проверить
- Строка 116: `SELECT chat_id FROM users`
- Возможно правильно если chat_id в users для Mango

#### app/db/setup.py  
**Статус:** Проверить
- Строки 45, 60: role_id и password из users
- Вероятно неправильно

---

## План Дальнейших Действий:

### Приоритет 1 (Критично):
1. ✅ `app/db/repositories/users.py` - DONE
2. ✅ `app/telegram/middlewares/permissions.py` - DONE  
3. ❌ `app/db/repositories/admin.py` - TODO
4. ❌ `app/db/setup.py` - TODO

### Приоритет 2 (Важно):
5. ❌ `app/telegram/handlers/admin_admins.py` - TODO
6. ❌ `app/telegram/handlers/admin_users.py` - TODO
7. ❌ `app/telegram/handlers/admin_commands.py` - TODO

### Приоритет 3 (Проверить):
8. ❓ `app/db/repositories/operators.py` - вероятно OK
9. ❓ `app/services/notifications.py` - вероятно OK
10. ❓ `app/telegram/middlewares/permissions_legacy.py` - если используется

---

## Принципы Join между Таблицами:

### Как правильно связывать:

```sql
-- Получить Telegram юзера с данными Mango оператора:
SELECT 
    ut.user_id,
    ut.username,
    ut.role_id,
    ut.status,
    u.extension,
    u.full_name as mango_name,
    u.protocol
FROM UsersTelegaBot ut
LEFT JOIN users u ON ut.operator_name = u.full_name 
    AND ut.extension = u.extension
WHERE ut.user_id = %s
```

### Что НЕЛЬЗЯ делать:

```sql
-- ❌ НЕПРАВИЛЬНО: запрос role_id из users
SELECT role_id FROM users WHERE user_id = %s

-- ✅ ПРАВИЛЬНО: запрос role_id из UsersTelegaBot  
SELECT role_id FROM UsersTelegaBot WHERE user_id = %s
```

---

## Проверочный Список:

Для каждого файла с `FROM users` проверить:

- [ ] Запрашивает ли role_id? → Переделать на UsersTelegaBot
- [ ] Запрашивает ли status? → Переделать на UsersTelegaBot
- [ ] Запрашивает ли approved_by? → Переделать на UsersTelegaBot
- [ ] Запрашивает ли blocked_at? → Переделать на UsersTelegaBot
- [ ] Запрашивает extension/protocol? → OK, это users (Mango)
- [ ] Запрашивает chat_id? → Проверить откуда это поле

---

**Дата:** 04.12.2024  
**Статус:** В процессе (2/10+ файлов исправлено)
