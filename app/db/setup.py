import time  # Для замера времени

from app.logging_config import get_watchdog_logger

from app.db.connection import execute_query
from app.core.roles import role_name_from_id

logger = get_watchdog_logger(__name__)

# Функция для создания таблиц
async def create_tables():
    raise RuntimeError(
        "create_tables больше не используется. Выполняйте миграции вне приложения."
    )

# Добавление пользователя
async def add_user(user_id, username, full_name, role_name="Operator"):
    logger.error(
        "add_user устарел. Управление пользователями должно происходить через бота."
    )

# Получение роли пользователя
async def get_user_role(user_id):
    logger.info(f"Получение роли для пользователя с user_id: {user_id}")
    query = """
    SELECT role_id FROM UsersTelegaBot WHERE user_id = %s
    """
    try:
        user_role = await execute_query(query, (user_id,), fetchone=True)
        if not user_role:
            return None
        return role_name_from_id(user_role.get('role_id'))
    except Exception as e:
        logger.error(f"Ошибка при получении роли пользователя: {e}")
        return None

# Точка входа
if __name__ == "__main__":
    raise SystemExit("db_setup.py предназначен только для инспекции/миграций. Запустите миграции вручную.")
