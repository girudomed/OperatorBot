import aiomysql
import logging

from logger_utils import setup_logging

# Пример функции для получения роли пользователя по его username
async def get_user_role(connection, username):
    """
    Получает роль пользователя по его имени пользователя (username).
    
    :param connection: Объект подключения к базе данных.
    :param username: Имя пользователя.
    :return: Название роли пользователя или None, если пользователь не найден.
    """
    if not username:
        logging.warning("Получен пустой username для запроса роли пользователя.")
        return None

    try:
        async with connection.cursor() as cursor:
            query = """
            SELECT roles.role_name 
            FROM users 
            JOIN roles ON users.role_id = roles.id 
            WHERE users.username = %s
            """
            await cursor.execute(query, (username,))
            result = await cursor.fetchone()
            if result:
                logging.info(f"Роль пользователя '{username}' успешно получена: {result['role_name']}")
                return result['role_name']
            else:
                logging.warning(f"Пользователь с username '{username}' не найден.")
                return None
    except aiomysql.Error as e:
        logging.error(f"Ошибка при получении роли пользователя: {e}")
        return None
    
    from logger_utils import setup_logging

logger = setup_logging()

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


def some_function():
    logger.info("Функция some_function начала работу.")
    # Логика функции
    try:
        # Некоторый код
        logger.info("Успешное выполнение.")
    except Exception as e:
        logger.error(f"Произошла ошибка: {e}")


# Пример функции для проверки прав доступа
async def check_permission(connection, role_name, permission):
    """
    Проверяет наличие определенного разрешения для указанной роли.
    
    :param connection: Объект подключения к базе данных.
    :param role_name: Название роли пользователя.
    :param permission: Название проверяемого разрешения.
    :return: True, если разрешение существует для этой роли, иначе False.
    """
    if not role_name or not permission:
        logging.warning("Получены пустые значения для проверки прав доступа.")
        return False

    try:
        async with connection.cursor() as cursor:
            query = """
            SELECT 1 
            FROM permissions 
            JOIN roles ON permissions.role_id = roles.id 
            WHERE roles.role_name = %s AND permissions.permission_name = %s
            """
            await cursor.execute(query, (role_name, permission))
            result = await cursor.fetchone()
            if result:
                logging.info(f"Доступ для роли '{role_name}' с правом '{permission}' подтвержден.")
                return True
            else:
                logging.warning(f"Роль '{role_name}' не имеет права '{permission}'.")
                return False
    except aiomysql.Error as e:
        logging.error(f"Ошибка при проверке прав доступа: {e}")
        return False
