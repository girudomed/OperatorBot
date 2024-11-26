import logging
import random
import string
import time  # Для замера времени
from dotenv import load_dotenv
from db_helpers import create_async_connection, execute_query, close_connection  # Исправлено на корректный импорт
import os
import secrets
import bcrypt

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
log_handler = logging.FileHandler('logs.log')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log_handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(log_handler)

# Проверка на наличие всех обязательных переменных окружения для базы данных
required_db_vars = ["DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME", "DB_PORT"]
missing_vars = [var for var in required_db_vars if not os.getenv(var)]
if missing_vars:
    logger.critical(f"Отсутствуют необходимые переменные окружения для базы данных: {', '.join(missing_vars)}")
    raise EnvironmentError(f"Отсутствуют переменные окружения: {', '.join(missing_vars)}")

# Функция для создания таблиц
async def create_tables():
    logger.info("Проверка и создание необходимых таблиц...")
    try:
        connection = await create_async_connection()
        async with connection.cursor() as cursor:
            start_time = time.time()  # Начало замера времени
            
            # Создание таблицы пользователей
            logger.info("Создание таблицы UsersTelegaBot, если она не существует...")
            await cursor.execute("""
            CREATE TABLE IF NOT EXISTS UsersTelegaBot (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id BIGINT UNIQUE,
                username VARCHAR(255),
                full_name VARCHAR(255),
                role_id INT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
            """)
            logger.info("Таблица UsersTelegaBot проверена и создана, если её не было.")

            # Создание таблицы ролей
            logger.info("Создание таблицы RolesTelegaBot, если она не существует...")
            await cursor.execute("""
            CREATE TABLE IF NOT EXISTS RolesTelegaBot (
                id INT AUTO_INCREMENT PRIMARY KEY,
                role_name VARCHAR(255) UNIQUE,
                role_password VARCHAR(255)
            )
            """)
            logger.info("Таблица RolesTelegaBot проверена и создана, если её не было.")

            # Создание таблицы разрешений
            logger.info("Создание таблицы PermissionsTelegaBot, если она не существует...")
            await cursor.execute("""
            CREATE TABLE IF NOT EXISTS PermissionsTelegaBot (
                id INT AUTO_INCREMENT PRIMARY KEY,
                role_id INT,
                permission VARCHAR(255),
                FOREIGN KEY (role_id) REFERENCES RolesTelegaBot(id)
            )
            """)
            logger.info("Таблица PermissionsTelegaBot проверена и создана, если её не было.")

            # Добавление ролей по умолчанию с генерацией паролей
            logger.info("Добавление ролей по умолчанию и генерация паролей...")
            default_roles = ['Operator', 'Admin', 'SuperAdmin', 'Developer', 'Head of Registry', 'Founder', 'Marketing Director']
            for role in default_roles:
                password = generate_password()
                hashed_password = hash_password(password)  # Хеширование пароля перед сохранением
                await cursor.execute("""
                INSERT IGNORE INTO RolesTelegaBot (role_name, role_password) 
                VALUES (%s, %s)
                """, (role, hashed_password))
            logger.info("Роли по умолчанию добавлены (если они не существовали).")

            # Добавление разрешений по умолчанию
            logger.info("Добавление разрешений по умолчанию...")
            await cursor.execute("""
            INSERT IGNORE INTO PermissionsTelegaBot (role_id, permission)
            SELECT id, 'view_reports' FROM RolesTelegaBot WHERE role_name = 'Operator'
            UNION ALL
            SELECT id, 'manage_users' FROM RolesTelegaBot WHERE role_name = 'Admin'
            UNION ALL
            SELECT id, 'full_access' FROM RolesTelegaBot WHERE role_name = 'SuperAdmin'
            UNION ALL
            SELECT id, 'full_access' FROM RolesTelegaBot WHERE role_name = 'Developer'
            UNION ALL
            SELECT id, 'manage_operators' FROM RolesTelegaBot WHERE role_name = 'Head of Registry'
            UNION ALL
            SELECT id, 'full_access' FROM RolesTelegaBot WHERE role_name = 'Founder'
            UNION ALL
            SELECT id, 'view_marketing_reports' FROM RolesTelegaBot WHERE role_name = 'Marketing Director'
            """)
            logger.info("Разрешения по умолчанию добавлены.")

            await connection.commit()
            elapsed_time = time.time() - start_time  # Конец замера времени
            logger.info(f"Все таблицы успешно созданы, роли и разрешения добавлены (Время выполнения: {elapsed_time:.4f} сек).")
    except Exception as e:
        logger.error(f"Ошибка при создании таблиц или добавлении ролей: {e}")
        await connection.rollback()
    finally:
        await close_connection()

# Генерация пароля
def generate_password(length=12):
    characters = string.ascii_letters + string.digits + string.punctuation
    return ''.join(secrets.choice(characters) for _ in range(length))

def hash_password(password):
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(password.encode(), salt).decode('utf-8')

# Добавление пользователя
async def add_user(user_id, username, full_name, role_name="Operator"):
    logger.info(f"Попытка добавления пользователя: {username}")
    try:
        connection = await create_async_connection()
        async with connection.cursor() as cursor:
            start_time = time.time()  # Начало замера времени

            # Получение role_id для роли пользователя
            query_role_id = "SELECT id FROM RolesTelegaBot WHERE role_name = %s"
            await cursor.execute(query_role_id, (role_name,))
            role_id = await cursor.fetchone()
            if role_id:
                role_id = role_id['id']
            else:
                logger.error(f"Роль '{role_name}' не найдена в базе данных.")
                return

            # Добавление пользователя
            insert_user_query = """
            INSERT INTO UsersTelegaBot (user_id, username, full_name, role_id)
            VALUES (%s, %s, %s, %s)
            """
            await cursor.execute(insert_user_query, (user_id, username, full_name, role_id))
            await connection.commit()
            elapsed_time = time.time() - start_time  # Конец замера времени
            logger.info(f"Пользователь {username} успешно добавлен с ролью {role_name} (Время выполнения: {elapsed_time:.4f} сек).")
    except Exception as e:
        logger.error(f"Произошла ошибка при добавлении пользователя: {e}")
        await connection.rollback()
    finally:
        await close_connection()

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
    import asyncio

    async def main():
        # Создание таблиц
        await create_tables()

        # Пример добавления пользователя
        await add_user(user_id=123456789, username="testuser", full_name="Test User", role_name="Admin")

        # Пример получения роли пользователя
        role = await get_user_role(123456789)
        print(f"Роль пользователя: {role}")

        # Закрытие соединений после завершения работы
        await close_connection()

    asyncio.run(main())
