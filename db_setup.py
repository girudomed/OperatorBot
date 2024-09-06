import logging
import aiomysql
import random
import string
from dotenv import load_dotenv
import os
from aiogram.types import KeyboardButton, ReplyKeyboardMarkup
from db_helpers import create_async_connection, execute_async_query


# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
log_handler = logging.FileHandler('logs.log')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log_handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(log_handler)

# Функция для создания таблиц
async def create_tables(connection):
    logger.info("Проверка и создание необходимых таблиц...")
    async with connection.cursor() as cursor:
        try:
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
                await cursor.execute("""
                INSERT IGNORE INTO RolesTelegaBot (role_name, role_password) 
                VALUES (%s, %s)
                """, (role, password))
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
            logger.info("Все таблицы успешно созданы или уже существуют, роли, пароли и разрешения добавлены.")
        except Exception as e:
            logger.error(f"Ошибка при создании таблиц или добавлении ролей: {e}")
            await connection.rollback()

# Генерация пароля
def generate_password(length=12):
    characters = string.ascii_letters + string.digits + string.punctuation
    return ''.join(random.choice(characters) for _ in range(length))

# Добавление пользователя
async def add_user(connection, user_id, username, full_name, role_name="Operator"):
    logger.info(f"Попытка добавления пользователя: {username}")
    async with connection.cursor() as cursor:
        try:
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
            logger.info(f"Пользователь {username} успешно добавлен с ролью {role_name}.")
        except Exception as e:
            logger.error(f"Произошла ошибка при добавлении пользователя: {e}")
            await connection.rollback()

# Получение роли пользователя
async def get_user_role(connection, user_id):
    logger.info(f"Получение роли для пользователя с user_id: {user_id}")
    query = """
    SELECT R.role_name FROM UsersTelegaBot U
    JOIN RolesTelegaBot R ON U.role_id = R.id
    WHERE U.user_id = %s
    """
    user_role = await execute_async_query(connection, query, (user_id,))
    if user_role:
        return user_role[0]['role_name']
    else:
        return None

# Получение пароля пользователя
async def get_user_password(connection, user_id):
    logger.info(f"Получение пароля для пользователя с user_id: {user_id}")
    query = """
    SELECT password FROM UsersTelegaBot WHERE user_id = %s
    """
    async with connection.cursor() as cursor:
        await cursor.execute(query, (user_id,))
        result = await cursor.fetchone()
        if result:
            return result['password']
        return None

# Получение пароля роли
async def get_role_password(connection, role_name):
    logger.info(f"Получение пароля для роли: {role_name}")
    query = """
    SELECT role_password FROM RolesTelegaBot WHERE role_name = %s
    """
    role_password = await execute_async_query(connection, query, (role_name,))
    if role_password:
        return role_password[0]['role_password']
    else:
        return None

# Регистрация пользователя
async def register_user_if_not_exists(update, context):
    user_id = update.message.from_user.id
    username = update.message.from_user.username
    full_name = update.message.from_user.full_name or "Не указано"

    connection = await create_async_connection()
    if not connection:
        await update.message.reply_text("Ошибка подключения к базе данных. Попробуйте позже.")
        return

    # Проверка, существует ли пользователь в базе данных
    role = await get_user_role(connection, user_id)
    if role:
        await update.message.reply_text(f"Добро пожаловать, {full_name}! Ваша роль: {role}")
    else:
        # Создание таблиц, если их нет
        await create_tables(connection)

        # Запрос на роль пользователя
        await update.message.reply_text(
            "Привет! Похоже, вы здесь впервые. Пожалуйста, выберите вашу роль.",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="Developer")],
                    [KeyboardButton(text="Head of Registry")],
                    [KeyboardButton(text="Founder")],
                    [KeyboardButton(text="Marketing Director")],
                    [KeyboardButton(text="Operator")]
                ],
                resize_keyboard=True,
                one_time_keyboard=True
            )
        )
        context.user_data['awaiting_role_selection'] = True
        context.user_data['user_id'] = user_id
        context.user_data['username'] = username
        context.user_data['full_name'] = full_name

# Выбор роли
async def role_selection_response(update, context):
    user_id = context.user_data.get('user_id')
    username = context.user_data.get('username')
    full_name = context.user_data.get('full_name')
    role_name = update.message.text.strip()
    valid_roles = ["Developer", "Head of Registry", "Founder", "Marketing Director", "Operator"]

    if role_name in valid_roles:
        connection = await create_async_connection()
        if not connection:
            await update.message.reply_text("Ошибка подключения к базе данных. Попробуйте позже.")
            return

        await add_user(connection, user_id, username, full_name, role_name=role_name)
        await update.message.reply_text(f"Вы успешно зарегистрированы как {role_name}.")
        context.user_data.clear()
        await connection.ensure_closed()
    else:
        keyboard = [
            [KeyboardButton(text="Developer")],
            [KeyboardButton(text="Head of Registry")],
            [KeyboardButton(text="Founder")],
            [KeyboardButton(text="Marketing Director")],
            [KeyboardButton(text="Operator")]
        ]

        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)

        await update.message.reply_text(
            "Неверная роль. Пожалуйста, выберите из следующих ролей.",
            reply_markup=reply_markup
        )

# Обработка пароля
async def password_response(update, context):
    entered_password = update.message.text.strip()
    user_id = context.user_data['user_id']

    connection = await create_async_connection()
    if not connection:
        await update.message.reply_text("Ошибка подключения к базе данных. Попробуйте позже.")
        return

    stored_password = await get_user_password(connection, user_id)
    if stored_password and entered_password == stored_password:
        context.user_data['awaiting_password'] = False
        await update.message.reply_text("Вы успешно вошли в систему.")
    else:
        await update.message.reply_text("Неверный пароль. Пожалуйста, попробуйте снова.")
    
    context.user_data.clear()
    await connection.ensure_closed()

# Точка входа
if __name__ == "__main__":
    import asyncio

    async def main():
        connection = await create_async_connection()
        if connection:
            await create_tables(connection)

            # Пример добавления пользователя
            await add_user(connection, user_id=123456789, username="testuser", full_name="Test User", role_name="Admin")

            # Пример получения роли пользователя
            role = await get_user_role(connection, 123456789)
            print(f"Роль пользователя: {role}")

            await connection.ensure_closed()

    asyncio.run(main())
