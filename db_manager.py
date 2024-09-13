import aiomysql
import logging
import time  # Для замера времени выполнения
from logger_utils import setup_logging

# Настройка логирования
logger = setup_logging()

# Функция для получения роли пользователя по его username
async def get_user_role(connection, username):
    """
    Получает роль пользователя по его имени пользователя (username).
    
    :param connection: Объект подключения к базе данных.
    :param username: Имя пользователя.
    :return: Название роли пользователя или None, если пользователь не найден.
    """
    if not username:
        logger.warning("Получен пустой username для запроса роли пользователя.")
        return None

    try:
        async with connection.cursor() as cursor:
            query = """
            SELECT roles.role_name 
            FROM users 
            JOIN roles ON users.role_id = roles.id 
            WHERE users.username = %s
            """
            start_time = time.time()  # Начало замера времени
            await cursor.execute(query, (username,))
            result = await cursor.fetchone()
            elapsed_time = time.time() - start_time  # Конец замера времени
            if result:
                logger.info(f"Роль пользователя '{username}' успешно получена: {result['role_name']} "
                            f"(Время выполнения: {elapsed_time:.4f} сек)")
                return result['role_name']
            else:
                logger.warning(f"Пользователь с username '{username}' не найден (Время выполнения: {elapsed_time:.4f} сек).")
                return None
    except aiomysql.Error as e:
        logger.error(f"Ошибка при получении роли пользователя: {e}")
        return None
    finally:
        if connection:
            await connection.ensure_closed()  # Обязательно закрываем соединение

# Функция для проверки прав доступа
async def check_permission(connection, role_name, permission):
    """
    Проверяет наличие определенного разрешения для указанной роли.
    
    :param connection: Объект подключения к базе данных.
    :param role_name: Название роли пользователя.
    :param permission: Название проверяемого разрешения.
    :return: True, если разрешение существует для этой роли, иначе False.
    """
    if not role_name or not permission:
        logger.warning("Получены пустые значения для проверки прав доступа.")
        return False

    try:
        async with connection.cursor() as cursor:
            query = """
            SELECT 1 
            FROM permissions 
            JOIN roles ON permissions.role_id = roles.id 
            WHERE roles.role_name = %s AND permissions.permission_name = %s
            """
            start_time = time.time()  # Начало замера времени
            await cursor.execute(query, (role_name, permission))
            result = await cursor.fetchone()
            elapsed_time = time.time() - start_time  # Конец замера времени
            if result:
                logger.info(f"Доступ для роли '{role_name}' с правом '{permission}' подтвержден "
                            f"(Время выполнения: {elapsed_time:.4f} сек).")
                return True
            else:
                logger.warning(f"Роль '{role_name}' не имеет права '{permission}' "
                               f"(Время выполнения: {elapsed_time:.4f} сек).")
                return False
    except aiomysql.Error as e:
        logger.error(f"Ошибка при проверке прав доступа: {e}")
        return False
    finally:
        if connection:
            await connection.ensure_closed()  # Обязательно закрываем соединение
