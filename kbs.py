import json
import logging
from db_setup import get_user_role, create_async_connection
from keyboard_utils import create_kb_for_role, default_kb

# Настройка логирования
logger = logging.getLogger(__name__)

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


# Функция для загрузки ролей из файла roles.json
def load_roles():
    try:
        with open('roles.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Ошибка при загрузке roles.json: {e}")
        return {}

# Загружаем роли из файла roles.json
roles = load_roles()

# Асинхронная функция для получения роли пользователя
async def get_user_role_async(user_telegram_id: int):
    try:
        connection = await create_async_connection()
        if not connection:
            logger.error("Ошибка подключения к базе данных")
            return None
        role = await get_user_role(connection, user_telegram_id)
        await connection.ensure_closed()
        return role
    except Exception as e:
        logger.error(f"Ошибка при получении роли пользователя {user_telegram_id}: {e}")
        return None

# Функция для создания основной клавиатуры в зависимости от уровня доступа пользователя
async def main_kb(user_telegram_id: int):
    role = await get_user_role_async(user_telegram_id)
    if not role:
        logger.error(f"Не удалось получить роль для пользователя с ID {user_telegram_id}")
        return default_kb()  # Возвращаем клавиатуру по умолчанию, если роль не найдена
    
    return create_kb_for_role(role)

# Функция для создания клавиатуры домашней страницы в зависимости от уровня доступа пользователя
async def home_page_kb(user_telegram_id: int):
    role = await get_user_role_async(user_telegram_id)
    if not role:
        logger.error(f"Не удалось получить роль для пользователя с ID {user_telegram_id}")
        return default_kb()  # Возвращаем клавиатуру по умолчанию, если роль не найдена

    return create_kb_for_role(role, home_page=True)
