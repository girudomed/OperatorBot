import os
import json

USERS_LOG = "users.log"
ADMINS_LOG = "admins.log"
ROLES_LOG = "roles.json"

# Определение ролей и их уровней доступа
ROLES = {
    "Developer": {
        "priority": 1,
        "permissions": ["full_access", "manage_users", "view_reports", "manage_settings"]
    },
    "Marketing Director": {
        "priority": 2,
        "permissions": ["view_reports", "generate_marketing_reports", "view_kpi"]
    },
    "Head of Registry": {
        "priority": 3,
        "permissions": ["manage_operators", "view_operator_reports", "view_kpi"]
    },
    "Operator": {
        "priority": 4,
        "permissions": ["view_own_reports"]
    }
}

from logger_utils import setup_logging

logger = setup_logging()

def some_function():
    logger.info("Функция some_function начала работу.")
    # Логика функции
    try:
        # Некоторый код
        logger.info("Успешное выполнение.")
    except Exception as e:
        logger.error(f"Произошла ошибка: {e}")


def log_user(user):
    """
    Логирует пользователя в файл users.log.
    Если пользователь не был зарегистрирован, добавляется с ролью по умолчанию (Operator).
    """
    users = get_logged_users()
    if user.username not in users:
        users[user.username] = {"id": user.id, "role": "Operator"}  # По умолчанию роль Оператора
        with open(USERS_LOG, "w") as f:
            json.dump(users, f)
        print(f"Пользователь {user.username} добавлен в систему с ролью Operator.")

def log_request(user, request):
    """
    Логирует запрос пользователя в файл requests.log.
    """
    with open("requests.log", "a") as f:
        f.write(f"{user.username}: {request}\n")

def get_logged_users():
    """
    Возвращает всех зарегистрированных пользователей.
    Если файл users.log не существует, возвращается пустой словарь.
    """
    if not os.path.exists(USERS_LOG):
        return {}
    with open(USERS_LOG, "r") as f:
        return json.load(f)

def get_logged_admins():
    """
    Возвращает всех пользователей с ролью Developer.
    """
    return {user: details for user, details in get_logged_users().items() if details["role"] == "Developer"}

def log_admin(user):
    """
    Логирует администратора в файл users.log, если его еще нет, или обновляет роль на Developer.
    """
    users = get_logged_users()
    if user.username not in users:
        log_user(user)  # Логируем пользователя, если его еще нет
    users[user.username]["role"] = "Developer"
    with open(USERS_LOG, "w") as f:
        json.dump(users, f)
    print(f"Пользователю {user.username} присвоена роль Developer.")

def remove_user(username):
    """
    Удаляет пользователя из системы.
    """
    users = get_logged_users()
    if username in users:
        del users[username]
        with open(USERS_LOG, "w") as f:
            json.dump(users, f)
        print(f"Пользователь {username} удален.")
    else:
        print(f"Пользователь {username} не найден.")

def assign_role(username, role):
    """
    Назначает пользователю определенную роль, если роль существует.
    """
    if role not in ROLES:
        raise ValueError(f"Роль {role} не существует.")
    users = get_logged_users()
    if username in users:
        users[username]["role"] = role
        with open(USERS_LOG, "w") as f:
            json.dump(users, f)
        print(f"Пользователю {username} присвоена роль {role}.")
    else:
        raise ValueError(f"Пользователь {username} не найден.")

def get_user_role(username):
    """
    Возвращает роль пользователя по его имени.
    Если пользователь не найден, возвращает None.
    """
    users = get_logged_users()
    if username in users:
        return users[username]["role"]
    else:
        return None

def has_permission(username, permission):
    """
    Проверяет, есть ли у пользователя указанное разрешение.
    """
    role = get_user_role(username)
    if role and permission in ROLES[role]["permissions"]:
        return True
    return False

def is_developer(username):
    """
    Проверяет, является ли пользователь разработчиком (Developer).
    """
    return get_user_role(username) == "Developer"

def list_users_by_role(role):
    """
    Возвращает список пользователей, у которых назначена указанная роль.
    """
    users = get_logged_users()
    return [user for user, details in users.items() if details["role"] == role]

def save_roles():
    """
    Сохраняет роли и их права доступа в файл roles.json.
    """
    with open(ROLES_LOG, "w") as f:
        json.dump(ROLES, f)
    print("Роли сохранены в roles.json.")

def load_roles():
    """
    Загружает роли из файла roles.json.
    Если файл не существует, сохраняет текущие роли.
    """
    if os.path.exists(ROLES_LOG):
        with open(ROLES_LOG, "r") as f:
            return json.load(f)
    else:
        save_roles()  # Сохраняем текущие роли, если файл не существует
        return ROLES

# Инициализация ролей при запуске
ROLES = load_roles()
