import logging
from db_setup import create_async_connection, add_user, create_tables, get_user_role, get_user_password
from logger_utils import setup_logging

# Настройка логирования
logger = setup_logging()

def some_function():
    """Пример функции с логированием."""
    logger.info("Функция some_function начала работу.")
    try:
        # Логика функции
        logger.info("Успешное выполнение.")
    except Exception as e:
        logger.error(f"Произошла ошибка: {e}")

# Функция для регистрации пользователя, если он еще не зарегистрирован
async def register_user_if_not_exists(update, context):
    """
    Регистрирует пользователя в базе данных, если он не существует.
    """
    try:
        user_id = update.message.from_user.id
        username = update.message.from_user.username
        full_name = update.message.from_user.full_name or "Без имени"  # Обрабатываем возможное отсутствие полного имени

        connection = await create_async_connection()
        if not connection:
            await update.message.reply_text("Ошибка подключения к базе данных.")
            return

        # Проверяем, существует ли пользователь в базе данных
        existing_role = await get_user_role(connection, user_id)
        if existing_role:
            logger.info(f"Пользователь {user_id} уже зарегистрирован с ролью {existing_role}.")
            await update.message.reply_text(f"Вы уже зарегистрированы с ролью '{existing_role}'.")
        else:
            # Если пользователя нет в базе, добавляем его с базовой ролью
            await add_user(connection, user_id=user_id, username=username, full_name=full_name, role_name="Operator")
            await update.message.reply_text(f"Вы успешно зарегистрированы как 'Operator'.")
            logger.info(f"Новый пользователь {user_id} успешно зарегистрирован.")
        
        await connection.ensure_closed()

    except Exception as e:
        logger.error(f"Ошибка при регистрации пользователя: {e}")
        await update.message.reply_text("Произошла ошибка при регистрации. Пожалуйста, попробуйте позже.")

# Функция для получения роли пользователя
async def get_user_role_from_db(user_id):
    """
    Получает роль пользователя из базы данных.
    """
    try:
        connection = await create_async_connection()
        if not connection:
            logger.error("Ошибка подключения к базе данных при получении роли пользователя.")
            return None

        role = await get_user_role(connection, user_id)
        await connection.ensure_closed()
        return role
    except Exception as e:
        logger.error(f"Ошибка при получении роли пользователя: {e}")
        return None

# Функция для добавления пользователя
async def add_user_to_db(user_id, username, full_name, role_name="Operator"):
    """
    Добавляет пользователя в базу данных.
    """
    try:
        connection = await create_async_connection()
        if not connection:
            logger.error("Ошибка подключения к базе данных при добавлении пользователя.")
            return False

        await add_user(connection, user_id=user_id, username=username, full_name=full_name, role_name=role_name)
        await connection.ensure_closed()
        logger.info(f"Пользователь {user_id} добавлен с ролью {role_name}.")
        return True
    except Exception as e:
        logger.error(f"Ошибка при добавлении пользователя: {e}")
        return False

# Функция для получения пароля пользователя
async def get_user_password_from_db(user_id):
    """
    Получает пароль пользователя из базы данных.
    """
    try:
        connection = await create_async_connection()
        if not connection:
            logger.error("Ошибка подключения к базе данных при получении пароля пользователя.")
            return None

        password = await get_user_password(connection, user_id)
        await connection.ensure_closed()
        return password
    except Exception as e:
        logger.error(f"Ошибка при получении пароля пользователя: {e}")
        return None

# Функция для создания таблиц, если они не существуют
async def ensure_tables_exist():
    """
    Проверяет и создает необходимые таблицы в базе данных, если их не существует.
    """
    try:
        connection = await create_async_connection()
        if not connection:
            logger.error("Ошибка подключения к базе данных при создании таблиц.")
            return False

        # Вызывается create_tables из db_setup, которая создаёт таблицы, если их нет
        await create_tables(connection)
        await connection.ensure_closed()
        logger.info("Таблицы проверены и созданы, если их не было.")
        return True
    except Exception as e:
        logger.error(f"Ошибка при создании таблиц: {e}")
        return False
