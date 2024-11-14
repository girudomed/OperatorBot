import logging
from db_setup import create_async_connection, add_user, create_tables, get_user_role, get_user_password
from logger_utils import setup_logging
import time  # Для замера времени

# Настройка логирования
logger = setup_logging()

# Функция для регистрации пользователя, если он еще не зарегистрирован
async def register_user_if_not_exists(update, context):
    """
    Регистрирует пользователя в базе данных, если он не существует.
    """
    user_id = update.message.from_user.id
    username = update.message.from_user.username or "unknown"
    full_name = update.message.from_user.full_name or "Без имени"

    try:
        async with create_async_connection() as connection:
            start_time = time.time()

            # Проверяем, существует ли пользователь в базе данных
            existing_role = await get_user_role(connection, user_id)
            elapsed_time = time.time() - start_time
            logger.info(f"[КРОТ]: Получена роль для пользователя {user_id}: {existing_role} (Время выполнения: {elapsed_time:.4f} сек)")
            
            if existing_role:
                await update.message.reply_text(f"Вы уже зарегистрированы с ролью '{existing_role}'.")
            else:
                # Если пользователя нет в базе, добавляем его с базовой ролью
                await add_user(connection, user_id=user_id, username=username, full_name=full_name, role_name="Operator")
                await update.message.reply_text(f"Вы успешно зарегистрированы как 'Operator'.")
                logger.info(f"[КРОТ]: Новый пользователь {user_id} зарегистрирован.")
    except Exception as e:
        logger.error(f"[КРОТ]: Ошибка при регистрации пользователя: {e}")
        await update.message.reply_text("Произошла ошибка при регистрации. Пожалуйста, попробуйте позже.")

# Функция для получения роли пользователя
async def get_user_role_from_db(user_id):
    """
    Получает роль пользователя из базы данных.
    """
    try:
        async with create_async_connection() as connection:
            start_time = time.time()
            role = await get_user_role(connection, user_id)
            elapsed_time = time.time() - start_time
            logger.info(f"[КРОТ]: Роль пользователя с ID {user_id}: {role} (Время выполнения: {elapsed_time:.4f} сек)")
            return role
    except Exception as e:
        logger.error(f"[КРОТ]: Ошибка при получении роли пользователя: {e}")
        return None

# Функция для добавления пользователя
async def add_user_to_db(user_id, username, full_name, role_name="Operator"):
    """
    Добавляет пользователя в базу данных.
    """
    try:
        async with create_async_connection() as connection:
            start_time = time.time()
            await add_user(connection, user_id=user_id, username=username, full_name=full_name, role_name=role_name)
            elapsed_time = time.time() - start_time
            logger.info(f"[КРОТ]: Пользователь {user_id} добавлен с ролью {role_name} (Время выполнения: {elapsed_time:.4f} сек).")
            return True
    except Exception as e:
        logger.error(f"[КРОТ]: Ошибка при добавлении пользователя: {e}")
        return False

# Функция для получения пароля пользователя
async def get_user_password_from_db(user_id):
    """
    Получает пароль пользователя из базы данных.
    """
    try:
        async with create_async_connection() as connection:
            start_time = time.time()
            password = await get_user_password(connection, user_id)
            elapsed_time = time.time() - start_time
            if password:
                logger.info(f"[КРОТ]: Пароль для пользователя с ID {user_id} успешно получен (Время выполнения: {elapsed_time:.4f} сек).")
            else:
                logger.warning(f"[КРОТ]: Пароль для пользователя с ID {user_id} не найден (Время выполнения: {elapsed_time:.4f} сек).")
            return password
    except Exception as e:
        logger.error(f"[КРОТ]: Ошибка при получении пароля пользователя: {e}")
        return None

# Функция для создания таблиц, если они не существуют
async def ensure_tables_exist():
    """
    Проверяет и создает необходимые таблицы в базе данных, если их не существует.
    """
    try:
        async with create_async_connection() as connection:
            start_time = time.time()
            await create_tables(connection)
            elapsed_time = time.time() - start_time
            logger.info(f"[КРОТ]: Таблицы проверены и созданы (Время выполнения: {elapsed_time:.4f} сек).")
            return True
    except Exception as e:
        logger.error(f"[КРОТ]: Ошибка при создании таблиц: {e}")
        return False
