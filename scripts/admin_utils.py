import os
import json
import time  # Для замера времени
from app.logging_config import get_watchdog_logger
from app.db.setup import get_user_role

# Настройка логирования
logger = get_watchdog_logger(__name__)

USERS_LOG = "users.log"
ROLES_LOG = "roles.json"
OPERATORS_LOG = "operators.log"
ADMINS_LOG = "admins.log"

# Загружаем роли из файла roles.json или сохраняем текущие роли, если файл не существует
def load_roles():
    start_time = time.time()
    if os.path.exists(ROLES_LOG):
        with open(ROLES_LOG, "r") as f:
            roles = json.load(f)
            elapsed_time = time.time() - start_time
            logger.info(f"[КРОТ]: Роли загружены из {ROLES_LOG} (Время выполнения: {elapsed_time:.4f} сек).")
            return roles
    else:
        save_roles()  # Сохраняем текущие роли, если файл не существует
        return ROLES

# Сохраняем роли в файл roles.json
def save_roles():
    start_time = time.time()
    with open(ROLES_LOG, "w") as f:
        json.dump(ROLES, f)
    elapsed_time = time.time() - start_time
    logger.info(f"[КРОТ]: Роли сохранены в {ROLES_LOG} (Время выполнения: {elapsed_time:.4f} сек).")

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

# Логирование действий оператора и администратора
def log_action(username, role, action):
    """
    Логирование действий пользователя в зависимости от его роли.
    Операторы и администраторы имеют разные лог-файлы.
    """
    start_time = time.time()
    log_file = OPERATORS_LOG if role == "Operator" else ADMINS_LOG
    with open(log_file, "a") as f:
        f.write(f"{time.ctime()} - {username} ({role}): {action}\n")
    elapsed_time = time.time() - start_time
    logger.info(f"[КРОТ]: Действие пользователя {username} ({role}) записано в {log_file} (Время выполнения: {elapsed_time:.4f} сек).")

# Функции для работы с пользователями
def log_user(user):
    """
    Логирует пользователя в файл users.log.
    Если пользователь не был зарегистрирован, добавляется с ролью по умолчанию (Operator).
    """
    start_time = time.time()
    users = get_logged_users()
    if user.username not in users:
        users[user.username] = {"id": user.id, "role": "Operator"}
        with open(USERS_LOG, "w") as f:
            json.dump(users, f)
        elapsed_time = time.time() - start_time
        logger.info(f"[КРОТ]: Пользователь {user.username} добавлен в систему с ролью Operator (Время выполнения: {elapsed_time:.4f} сек).")
        log_action(user.username, "Operator", "Пользователь зарегистрирован")

def log_request(user, request):
    """
    Логирует запрос пользователя в файл requests.log.
    """
    start_time = time.time()
    with open("requests.log", "a") as f:
        f.write(f"{time.ctime()} - {user.username}: {request}\n")
    elapsed_time = time.time() - start_time
    logger.info(f"[КРОТ]: Запрос от {user.username}: {request} (Время выполнения: {elapsed_time:.4f} сек).")
    log_action(user.username, get_user_role(user.username), f"Запрос: {request}")

def get_logged_users():
    """
    Возвращает всех зарегистрированных пользователей.
    Если файл users.log не существует, возвращается пустой словарь.
    """
    start_time = time.time()
    if not os.path.exists(USERS_LOG):
        elapsed_time = time.time() - start_time
        logger.info(f"[КРОТ]: Файл {USERS_LOG} не найден. Возвращен пустой список пользователей (Время выполнения: {elapsed_time:.4f} сек).")
        return {}
    with open(USERS_LOG, "r") as f:
        users = json.load(f)
        elapsed_time = time.time() - start_time
        logger.info(f"[КРОТ]: Пользователи загружены из {USERS_LOG} (Время выполнения: {elapsed_time:.4f} сек).")
        return users

def get_logged_admins():
    """
    Возвращает всех пользователей с ролью Developer.
    """
    return {user: details for user, details in get_logged_users().items() if details["role"] == "Developer"}

def log_admin(user):
    """
    Логирует администратора в файл users.log, если его еще нет, или обновляет роль на Developer.
    """
    start_time = time.time()
    users = get_logged_users()
    if user.username not in users:
        log_user(user)
    users[user.username]["role"] = "Developer"
    with open(USERS_LOG, "w") as f:
        json.dump(users, f)
    elapsed_time = time.time() - start_time
    logger.info(f"[КРОТ]: Пользователю {user.username} присвоена роль Developer (Время выполнения: {elapsed_time:.4f} сек).")
    log_action(user.username, "Developer", "Пользователь назначен администратором")

def remove_user(username):
    """
    Удаляет пользователя из системы.
    """
    start_time = time.time()
    users = get_logged_users()
    if username in users:
        del users[username]
        with open(USERS_LOG, "w") as f:
            json.dump(users, f)
        elapsed_time = time.time() - start_time
        logger.info(f"[КРОТ]: Пользователь {username} удален из системы (Время выполнения: {elapsed_time:.4f} сек).")
        log_action(username, "Unknown", "Пользователь удален")
    else:
        logger.warning(f"[КРОТ]: Пользователь {username} не найден.")

def assign_role(username, role):
    """
    Назначает пользователю определенную роль, если роль существует.
    """
    if role not in ROLES:
        raise ValueError(f"Роль {role} не существует.")
    start_time = time.time()
    users = get_logged_users()
    if username in users:
        users[username]["role"] = role
        with open(USERS_LOG, "w") as f:
            json.dump(users, f)
        elapsed_time = time.time() - start_time
        logger.info(f"[КРОТ]: Пользователю {username} присвоена роль {role} (Время выполнения: {elapsed_time:.4f} сек).")
        log_action(username, role, f"Пользователю назначена роль {role}")
    else:
        raise ValueError(f"Пользователь {username} не найден.")

def get_user_role(username):
    """
    Возвращает роль пользователя по его имени.
    Если пользователь не найден, возвращает None.
    """
    return get_logged_users().get(username, {}).get("role", None)

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
    return [user for user, details in get_logged_users().items() if details["role"] == role]

# Инициализация ролей при запуске
ROLES = load_roles()
