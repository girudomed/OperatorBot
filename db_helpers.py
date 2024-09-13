import asyncio
import time
import aiomysql
import logging
import os
from dotenv import load_dotenv
from logger_utils import setup_logging

# Загрузка переменных окружения из .env
load_dotenv()

# Инициализация логирования
logger = setup_logging()

# Конфигурация базы данных из переменных окружения
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "db": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT")),
}

# Функция для создания асинхронного подключения с ретраями
async def create_async_connection(retries=3, delay=5):
    """Создание асинхронного соединения с базой данных с повторными попытками."""
    for attempt in range(retries):
        try:
            connection = await aiomysql.connect(
                host=DB_CONFIG["host"],
                user=DB_CONFIG["user"],
                password=DB_CONFIG["password"],
                db=DB_CONFIG["db"],
                port=DB_CONFIG["port"],
                cursorclass=aiomysql.DictCursor,
                autocommit=True
            )
            logger.info("Успешное подключение к базе данных.")
            return connection
        except aiomysql.Error as e:
            logger.error(f"Ошибка подключения к базе данных: {e}")
            if attempt < retries - 1:
                logger.info(f"Повторная попытка подключения через {delay} секунд...")
                await asyncio.sleep(delay)
            else:
                logger.error("Все попытки подключения к базе данных исчерпаны.")
                raise

# Утилитарная функция для выполнения запросов с логированием и замером времени
async def execute_query_with_logging(connection, query, params=None, retries=3, log_time=True):
    """
    Универсальная функция для выполнения запросов с ретраями и логированием.

    :param connection: Соединение с базой данных.
    :param query: SQL-запрос для выполнения.
    :param params: Параметры для SQL-запроса.
    :param retries: Количество повторных попыток выполнения запроса.
    :param log_time: Нужно ли логировать время выполнения.
    """
    for attempt in range(retries):
        try:
            async with connection.cursor() as cursor:
                start_time = time.time() if log_time else None
                await cursor.execute(query, params)
                result = await cursor.fetchall()

                if log_time:
                    elapsed_time = time.time() - start_time
                    logger.info(f"Запрос выполнен за {elapsed_time:.4f} сек. Получено {len(result)} записей.")
                return result
        except aiomysql.Error as e:
            logger.error(f"Ошибка выполнения запроса: {query}, параметры: {params}, ошибка: {e}")
            if e.args[0] in (2013, 2006):  # Ошибки потери соединения MySQL
                logger.warning("Потеря соединения. Попытка повторного подключения...")
                await connection.ensure_closed()
                connection = await create_async_connection()
                if connection is None:
                    logger.error("Повторное подключение не удалось.")
                    return None
            else:
                logger.error(f"Запрос провалился окончательно: {e}")
                return None
    return None

# === Новый функционал для работы с таблицей reports === #

# Сохранение отчета в таблицу reports
async def save_report_to_db(user_id, report_text):
    """Сохранение сгенерированного отчета в таблицу reports."""
    connection = None
    try:
        connection = await create_async_connection()
        query = """
        INSERT INTO reports (user_id, report_text, report_date)
        VALUES (%s, %s, CURRENT_DATE)
        """
        await execute_query_with_logging(connection, query, (user_id, report_text))
        logger.info(f"Отчет для пользователя с user_id {user_id} успешно сохранен в базе данных.")
    except Exception as e:
        logger.error(f"Ошибка при сохранении отчета для пользователя с user_id {user_id}: {e}")
    finally:
        if connection:
            await connection.ensure_closed()

# Получение всех отчетов за текущий день
async def get_reports_for_today():
    """Получение всех отчетов за текущий день."""
    connection = None
    try:
        connection = await create_async_connection()
        query = """
        SELECT * FROM reports WHERE report_date = CURRENT_DATE
        """
        result = await execute_query_with_logging(connection, query)
        if result:
            logger.info(f"Получено {len(result)} отчетов за сегодняшний день.")
        else:
            logger.info("Отчеты за текущий день не найдены.")
        return result
    except Exception as e:
        logger.error(f"Ошибка при получении отчетов за текущий день: {e}")
        return None
    finally:
        if connection:
            await connection.ensure_closed()

# Получение отчетов по user_id за определенную дату
async def get_reports_by_user_and_date(user_id, report_date):
    """Получение отчетов для пользователя за определенную дату."""
    connection = None
    try:
        connection = await create_async_connection()
        query = """
        SELECT * FROM reports WHERE user_id = %s AND report_date = %s
        """
        result = await execute_query_with_logging(connection, query, (user_id, report_date))
        if result:
            logger.info(f"Отчеты для пользователя {user_id} за {report_date} найдены.")
        else:
            logger.info(f"Отчеты для пользователя {user_id} за {report_date} не найдены.")
        return result
    except Exception as e:
        logger.error(f"Ошибка при получении отчетов для пользователя {user_id} за {report_date}: {e}")
        return None
    finally:
        if connection:
            await connection.ensure_closed()

# Обновление отчета по его ID
async def update_report(report_id, new_report_text):
    """Обновление текста отчета по его ID."""
    connection = None
    try:
        connection = await create_async_connection()
        query = """
        UPDATE reports SET report_text = %s WHERE report_id = %s
        """
        await execute_query_with_logging(connection, query, (new_report_text, report_id))
        logger.info(f"Отчет с report_id {report_id} успешно обновлен.")
    except Exception as e:
        logger.error(f"Ошибка при обновлении отчета с report_id {report_id}: {e}")
    finally:
        if connection:
            await connection.ensure_closed()

# === Существующие функции === #

# Поиск оператора по имени с частичным совпадением
async def find_operator_by_name(operator_name):
    """Поиск оператора по имени с частичным совпадением."""
    connection = None
    try:
        connection = await create_async_connection()
        query = "SELECT user_id, extension FROM users WHERE name LIKE %s"
        result = await execute_query_with_logging(connection, query, (f"%{operator_name}%",))
        if result:
            return result[0]
        else:
            logger.warning(f"Оператор с именем {operator_name} не найден.")
            return None
    except Exception as e:
        logger.error(f"Ошибка при поиске оператора с именем {operator_name}: {e}")
        return None
    finally:
        if connection:
            await connection.ensure_closed()

# Поиск оператора по user_id
async def find_operator_by_id(user_id):
    """Поиск оператора по его user_id."""
    connection = None
    try:
        connection = await create_async_connection()
        query = "SELECT user_id, extension FROM users WHERE user_id = %s"
        result = await execute_query_with_logging(connection, query, (user_id,))
        if result:
            return result[0]
        else:
            logger.warning(f"Оператор с ID {user_id} не найден.")
            return None
    except Exception as e:
        logger.error(f"Ошибка при поиске оператора с ID {user_id}: {e}")
        return None
    finally:
        if connection:
            await connection.ensure_closed()

# Регистрация пользователя, если его нет в базе данных
async def register_user_if_not_exists(user_id, username, full_name):
    """Регистрация пользователя, если он не существует в базе данных."""
    connection = None
    try:
        connection = await create_async_connection()
        query = "SELECT * FROM UsersTelegaBot WHERE user_id = %s"
        result = await execute_query_with_logging(connection, query, (user_id,))
        if not result:
            query = "INSERT INTO UsersTelegaBot (user_id, username, full_name) VALUES (%s, %s, %s)"
            await execute_query_with_logging(connection, query, (user_id, username, full_name))
            logger.info(f"Пользователь {username} зарегистрирован.")
        else:
            logger.info(f"Пользователь {username} уже существует.")
    except Exception as e:
        logger.error(f"Ошибка при регистрации пользователя {username}: {e}")
    finally:
        if connection:
            await connection.ensure_closed()

# Получение роли пользователя из базы данных
async def get_user_role(user_id):
    """Получение роли пользователя из базы данных."""
    connection = None
    try:
        connection = await create_async_connection()
        query = """
        SELECT R.role_name 
        FROM UsersTelegaBot U
        JOIN RolesTelegaBot R ON U.role_id = R.id
        WHERE U.user_id = %s
        """
        result = await execute_query_with_logging(connection, query, (user_id,))
        if result:
            logger.info(f"Роль пользователя с user_id {user_id} успешно получена.")
            return result[0]['role_name']
        logger.warning(f"Роль пользователя с user_id {user_id} не найдена.")
        return None
    except Exception as e:
        logger.error(f"Ошибка при получении роли пользователя с user_id {user_id}: {e}")
        return None
    finally:
        if connection:
            await connection.ensure_closed()

# Получение пароля пользователя
async def get_user_password(user_id):
    """Получение пароля пользователя."""
    connection = None
    try:
        connection = await create_async_connection()
        query = "SELECT password FROM UsersTelegaBot WHERE user_id = %s"
        result = await execute_query_with_logging(connection, query, (user_id,))
        if result:
            logger.info(f"Пароль для пользователя с user_id {user_id} успешно получен.")
            return result[0]['password']
        logger.warning(f"Пароль для пользователя с user_id {user_id} не найден.")
        return None
    except Exception as e:
        logger.error(f"Ошибка при получении пароля пользователя с user_id {user_id}: {e}")
        return None
    finally:
        if connection:
            await connection.ensure_closed()

# Обновление пароля пользователя
async def update_user_password(user_id, password):
    """Обновление пароля пользователя в базе данных."""
    connection = None
    try:
        connection = await create_async_connection()
        query = "UPDATE UsersTelegaBot SET password = %s WHERE user_id = %s"
        await execute_query_with_logging(connection, query, (password, user_id))
        logger.info(f"Пароль для пользователя с user_id {user_id} успешно обновлен.")
    except Exception as e:
        logger.error(f"Ошибка при обновлении пароля пользователя с user_id {user_id}: {e}")
    finally:
        if connection:
            await connection.ensure_closed()
