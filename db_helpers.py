import aiomysql
import logging
import os
from dotenv import load_dotenv

# Загрузка переменных окружения из .env
load_dotenv()

# Получение данных для подключения к базе данных из .env
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_PORT = int(os.getenv("DB_PORT"))

async def create_async_connection():
    """Создание асинхронного соединения с базой данных."""
    try:
        connection = await aiomysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            db=DB_NAME,
            port=DB_PORT,
            cursorclass=aiomysql.DictCursor,
            autocommit=True
        )
        logging.info("Успешное подключение к базе данных")
        return connection
    except aiomysql.Error as e:
        logging.error(f"Ошибка подключения к базе данных: {e}")
        raise

async def execute_async_query(connection, query, params=None, retries=3):
    """Выполнение асинхронного запроса к базе данных с попытками повторного выполнения."""
    for attempt in range(retries):
        async with connection.cursor() as cursor:
            try:
                await cursor.execute(query, params)
                result = await cursor.fetchall()
                logging.info(f"Запрос выполнен успешно. Получено {len(result)} записей.")
                return result
            except aiomysql.Error as e:
                logging.error(f"Ошибка выполнения запроса: {query}, ошибка: {e}")
                if e.args[0] in (2013, 2006):  # Ошибки подключения MySQL
                    logging.info("Попытка повторного подключения...")
                    await connection.ensure_closed()
                    connection = await create_async_connection()
                    if connection is None:
                        return None
                else:
                    return None
    return None

async def register_user_if_not_exists(connection, user_id, username, full_name):
    """Регистрация пользователя, если он не существует в базе данных."""
    try:
        query = "SELECT * FROM UsersTelegaBot WHERE user_id = %s"
        result = await execute_async_query(connection, query, (user_id,))
        if not result:
            # Если пользователь не существует, добавляем его
            query = "INSERT INTO UsersTelegaBot (user_id, username, full_name) VALUES (%s, %s, %s)"
            await execute_async_query(connection, query, (user_id, username, full_name))
            logging.info(f"Пользователь {username} зарегистрирован.")
        else:
            logging.info(f"Пользователь {username} уже существует.")
    except Exception as e:
        logging.error(f"Ошибка при регистрации пользователя: {e}")

async def get_user_role(connection, user_id):
    """Получение роли пользователя из базы данных."""
    try:
        query = """
        SELECT R.role_name 
        FROM UsersTelegaBot U
        JOIN RolesTelegaBot R ON U.role_id = R.id
        WHERE U.user_id = %s
        """
        result = await execute_async_query(connection, query, (user_id,))
        if result:
            return result[0]['role_name']
        logging.error(f"Роль пользователя с user_id {user_id} не найдена.")
        return None
    except Exception as e:
        logging.error(f"Ошибка при получении роли пользователя: {e}")
        return None

async def add_user(connection, user_id, username, full_name, role_name):
    """Добавление нового пользователя с указанной ролью в базу данных."""
    try:
        # Получение role_id для указанной роли
        query_role_id = "SELECT id FROM RolesTelegaBot WHERE role_name = %s"
        role_result = await execute_async_query(connection, query_role_id, (role_name,))
        if role_result:
            role_id = role_result[0]['id']
            query_add_user = """
            INSERT INTO UsersTelegaBot (user_id, username, full_name, role_id)
            VALUES (%s, %s, %s, %s)
            """
            await execute_async_query(connection, query_add_user, (user_id, username, full_name, role_id))
            logging.info(f"Пользователь {username} добавлен с ролью {role_name}.")
        else:
            logging.error(f"Роль {role_name} не найдена в базе данных.")
    except Exception as e:
        logging.error(f"Ошибка при добавлении пользователя: {e}")

async def get_user_password(connection, user_id):
    """Получение пароля пользователя."""
    try:
        query = "SELECT password FROM UsersTelegaBot WHERE user_id = %s"
        result = await execute_async_query(connection, query, (user_id,))
        if result:
            return result[0]['password']
        logging.error(f"Пароль для пользователя с user_id {user_id} не найден.")
        return None
    except Exception as e:
        logging.error(f"Ошибка при получении пароля пользователя: {e}")
        return None
