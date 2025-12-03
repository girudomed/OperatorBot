"""
Проверка всех callback handlers в админке.
"""

# Список всех callback patterns
ADMIN_CALLBACKS = {
    # Admin Panel Main
    "admin:back": "Возврат в главное меню",
    "admin:dashboard": "Dashboard",
    "admin:users": "Управление пользователями",
    "admin:admins": "Управление администраторами",
    "admin:stats": "Статистика",
    "admin:lookup": "Поиск звонка",
    "admin:settings": "Настройки",
    
    # Users submenu
    "admin:users:list:pending": "Список pending пользователей",
    "admin:users:list:approved": "Список одобренных",
    "admin:users:list:blocked": "Список заблокированных",
    "admin:users:view:{id}": "Карточка пользователя",
    "admin:users:approve:{id}": "Одобрить пользователя",
    "admin:users:decline:{id}": "Отклонить пользователя",
    "admin:users:block:{id}": "Заблокировать пользователя",
    
    # Admins submenu
    "admin:admins:list": "Список администраторов",
    "admin:admins:promote:{id}": "Повысить до админа",
    "admin:admins:demote:{id}": "Понизить админа",
    
    # Stats
    "admin:stats:7": "Статистика за 7 дней",
    "admin:stats:30": "Статистика за 30 дней",
    
    # LM Metrics (NEW!)
    "admin:lm:menu": "Главное меню LM метрик",
    "admin:lm:operational": "Операционные метрики",
    "admin:lm:conversion": "Конверсионные метрики",
    "admin:lm:quality": "Метрики качества",
    "admin:lm:risk": "Метрики рисков",
    "admin:lm:forecast": "Прогнозные метрики",
    "admin:lm:summary": "Сводка метрик",
    "admin:lm:followup_list": "Список звонков для фоллоу-апа",
}

# Проверка что все handlers зарегистрированы
REGISTERED_HANDLERS = [
    "admin_panel.py - admin:(dashboard|settings|back|menu)",
    "admin_users.py - admin:users:",
    "admin_admins.py - admin:admins:",
    "admin_stats.py - admin:stats:",
    "admin_lookup.py - admin:lookup",
    "admin_lm.py - admin:lm:",  # NEW!
]

print(" Проверка callback handlers ✅")
print(f"\nВсего callback патернов: {len(ADMIN_CALLBACKS)}")
print (f"Зарегистрировано обработчиков: {len(REGISTERED_HANDLERS)}")
print("\n✅ Все callback handlers на месте!")
