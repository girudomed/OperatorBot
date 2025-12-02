import logging
import random
import string
import time  # Для замера времени
import secrets
import bcrypt

from app.db.connection import execute_query

# Настройка логирования
log_handler = logging.FileHandler('logs.log')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log_handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(log_handler)

# Функция для создания таблиц
async def create_tables():
    raise RuntimeError(
        "create_tables больше не используется. Выполняйте миграции вне приложения."
    )

# Генерация пароля
def generate_password(length=12):
    characters = string.ascii_letters + string.digits + string.punctuation
    return ''.join(secrets.choice(characters) for _ in range(length))

def hash_password(password):
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(password.encode(), salt).decode('utf-8')

# Добавление пользователя
async def add_user(user_id, username, full_name, role_name="Operator"):
    logger.error(
        "add_user устарел. Управление пользователями должно происходить через бота."
    )

# Получение роли пользователя
async def get_user_role(user_id):
    logger.info(f"Получение роли для пользователя с user_id: {user_id}")
    query = """
    SELECT R.role_name FROM UsersTelegaBot U
    JOIN RolesTelegaBot R ON U.role_id = R.id
    WHERE U.user_id = %s
    """
    try:
        user_role = await execute_query(query, (user_id,), fetchone=True)
        return user_role['role_name'] if user_role else None
    except Exception as e:
        logger.error(f"Ошибка при получении роли пользователя: {e}")
        return None

# Получение пароля пользователя
async def get_user_password(user_id):
    logger.info(f"Получение пароля для пользователя с user_id: {user_id}")
    query = """
    SELECT password FROM UsersTelegaBot WHERE user_id = %s
    """
    try:
        user_password = await execute_query(query, (user_id,), fetchone=True)
        return user_password['password'] if user_password else None
    except Exception as e:
        logger.error(f"Ошибка при получении пароля пользователя: {e}")
        return None

# Получение пароля роли
async def get_role_password(role_name):
    logger.info(f"Получение пароля для роли: {role_name}")
    query = """
    SELECT role_password FROM RolesTelegaBot WHERE role_name = %s
    """
    try:
        role_password = await execute_query(query, (role_name,), fetchone=True)
        return role_password['role_password'] if role_password else None
    except Exception as e:
        logger.error(f"Ошибка при получении пароля роли: {e}")
        return None

# Точка входа
if __name__ == "__main__":
    raise SystemExit("db_setup.py предназначен только для инспекции/миграций. Запустите миграции вручную.")
