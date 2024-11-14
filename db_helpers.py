import asyncio
import time
import aiomysql
import logging
import os
import bcrypt
from dotenv import load_dotenv
from logger_utils import setup_logging

# Загрузка переменных окружения из .env
load_dotenv()

# Инициализация логирования
logger = setup_logging()

# Конфигурация базы данных из переменных окружения с проверкой на наличие всех параметров
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "db": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", 3306)),  # По умолчанию порт MySQL
}

# Проверка на наличие всех обязательных переменных окружения для базы данных
required_db_vars = ["DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME", "DB_PORT"]
missing_vars = [var for var in required_db_vars if not os.getenv(var)]
if missing_vars:
    logger.critical(f"Отсутствуют необходимые переменные окружения для базы данных: {', '.join(missing_vars)}")
    raise EnvironmentError(f"Отсутствуют переменные окружения: {', '.join(missing_vars)}")

# Пул соединений (инициализируется позже)
pool = None

async def create_async_connection():
    """Создание пула соединений с базой данных."""
    global pool
    if not pool:  # Проверка, если пул еще не создан
        try:
            logger.info(f"Попытка подключения к базе данных с параметрами: host={DB_CONFIG['host']}, "
                        f"port={DB_CONFIG['port']}, user={DB_CONFIG['user']}, db={DB_CONFIG['db']}")
            
            pool = await aiomysql.create_pool(
                host=DB_CONFIG["host"],
                port=int(DB_CONFIG["port"]),
                user=DB_CONFIG["user"],
                password=DB_CONFIG["password"],
                db=DB_CONFIG["db"],
                autocommit=True,
                minsize=1,
                maxsize=10,
                cursorclass=aiomysql.DictCursor
            )
            logger.info("[DB] Пул соединений успешно создан.")
        except aiomysql.Error as e:
            logger.error(f"[DB] Ошибка создания пула соединений: {e}")
            raise
        except Exception as e:
            logger.error(f"Общая ошибка при создании пула соединений: {e}")
            raise

async def close_async_connection():
    """Закрытие пула соединений."""
    global pool
    if pool:
        pool.close()
        await pool.wait_closed()
        pool = None  # Обнуляем пул после закрытия
        logger.info("[DB] Пул соединений закрыт.")

async def execute_query(query, params=None, fetchone=False, fetchall=False):
    """
    Универсальная функция для выполнения SQL-запросов.
    :param query: SQL-запрос.
    :param params: Параметры для запроса.
    :param fetchone: Если True, возвращает одну запись.
    :param fetchall: Если True, возвращает все записи.
    :return: Результат запроса.
    """
    global pool
    await create_async_connection()  # Создаем соединение, если еще не создано
    async with pool.acquire() as connection:
        async with connection.cursor() as cursor:
            try:
                start_time = time.time()
                await cursor.execute(query, params)
                elapsed_time = time.time() - start_time
                logger.info(f"[DB] Выполнен запрос: {query} с параметрами {params} за {elapsed_time:.4f} сек.")
                
                if fetchone:
                    result = await cursor.fetchone()
                    if result:
                        logger.debug(f"[DB] Получена одна запись: {result}")
                    else: 
                        logger.warning(f"[DB] Запись не найдена.")                   
                    return result
                if fetchall:
                    result = await cursor.fetchall()
                    if result:
                        logger.debug(f"[DB] Получено {len(result)} записей.")
                    else:
                        logger.warning(f"[DB] Данные не найдены.")
                    return result
            except aiomysql.Error as e:
                logger.error(f"[DB] Ошибка выполнения запроса: {query}, параметры: {params}, ошибка: {e}")
                raise

# === Функция для обновления пароля пользователя === #
async def update_user_password(user_id, new_password):
    """Обновление хешированного пароля пользователя."""
    hashed_password = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
    query = """
        UPDATE UsersTelegaBot 
        SET password = %s 
        WHERE user_id = %s
    """
    try:
        await execute_query(query, (hashed_password, user_id))
        logger.info(f"[DB] Пароль для user_id {user_id} успешно обновлен.")
    except Exception as e:
        logger.error(f"[DB] Ошибка при обновлении пароля для user_id {user_id}: {e}")

# === Функции для работы с таблицей reports === #
async def save_report_to_db(user_id, report_text):
    """Сохранение сгенерированного отчета в таблицу reports."""
    query = """
        INSERT INTO reports (user_id, report_text, report_date)
        VALUES (%s, %s, CURRENT_DATE)
        ON DUPLICATE KEY UPDATE report_text=VALUES(report_text), report_date=VALUES(report_date)
    """
    try:
        await execute_query(query, (user_id, report_text))
        logger.info(f"[DB] Отчет для user_id {user_id} успешно сохранен.")
    except Exception as e:
        logger.error(f"[DB] Ошибка при сохранении отчета для user_id {user_id}: {e}")

async def get_reports_for_today():
    """Получение всех отчетов за текущий день."""
    query = "SELECT * FROM reports WHERE report_date = CURRENT_DATE"
    try:
        reports = await execute_query(query, fetchall=True)
        if reports:
            logger.info(f"[DB] Получено {len(reports)} отчетов за сегодняшний день.")
        else:
            logger.info("[DB] Отчеты за текущий день не найдены.")
        return reports
    except Exception as e:
        logger.error(f"[DB] Ошибка при получении отчетов за текущий день: {e}")
        return None

async def get_reports_by_user_and_date(user_id, report_date):
    """Получение отчетов для пользователя за определенную дату."""
    query = """
        SELECT * FROM reports 
        WHERE user_id = %s AND report_date = %s
    """
    try:
        reports = await execute_query(query, (user_id, report_date), fetchall=True)
        if reports:
            logger.info(f"[DB] Найдены отчеты для user_id {user_id} за {report_date}.")
        else:
            logger.info(f"[DB] Отчеты для user_id {user_id} за {report_date} не найдены.")
        return reports
    except Exception as e:
        logger.error(f"[DB] Ошибка при получении отчетов для user_id {user_id} за {report_date}: {e}")
        return None

# === Функции для работы с операторами и пользователями === #
async def find_operator_by_name_and_extension(operator_name, extension):
    """
    Поиск оператора по имени и extension (частичное совпадение).
    Связь с call_scores происходит через поля caller_info и called_info, которые содержат extension и имя.
    """
    query = """
        SELECT user_id, extension, name
        FROM users 
        WHERE name LIKE %s AND extension = %s
    """
    try:
        operator = await execute_query(query, (f"%{operator_name}%", extension), fetchone=True)
        if operator:
            logger.info(f"[DB] Оператор с именем '{operator_name}' и extension '{extension}' найден: {operator}")
            return operator
        else:
            logger.warning(f"[DB] Оператор с именем '{operator_name}' и extension '{extension}' не найден.")
            return None
    except Exception as e:
        logger.error(f"[DB] Ошибка при поиске оператора по имени '{operator_name}' и extension '{extension}': {e}")
        return None

async def find_operator_by_user_id(user_id):
    """Поиск оператора по Telegram user_id."""
    query = """
        SELECT user_id, extension, name
        FROM users
        WHERE user_id = %s
    """
    try:
        result = await execute_query(query, (user_id,), fetchone=True)
        if result:
            logger.info(f"[DB] Оператор с Telegram user_id {user_id} найден: {result}")
            return result
        else:
            logger.warning(f"[DB] Оператор с Telegram user_id {user_id} не найден.")
            return None
    except Exception as e:
        logger.error(f"[DB] Ошибка при поиске оператора с Telegram user_id {user_id}: {e}")
        return None

# === Функции для поиска звонков и метрик === #
async def get_operator_calls(user_id, start_date=None, end_date=None):
    """Получение звонков оператора за указанный период через user_id."""
    query = """
        SELECT cs.call_date, cs.call_score, cs.result, cs.talk_duration 
        FROM call_scores cs
        JOIN users u ON cs.caller_info LIKE CONCAT('%', u.extension, '%') OR cs.called_info LIKE CONCAT('%', u.extension, '%')
        WHERE u.user_id = %s
    """
    params = [user_id]
    if start_date and end_date:
        query += " AND cs.call_date BETWEEN %s AND %s"
        params.extend([start_date, end_date])
    
    try:
        result = await execute_query(query, params, fetchall=True)
        if result:
            logger.info(f"[DB] Найдены звонки для user_id {user_id} за период с {start_date} по {end_date}.")
        else:
            logger.info(f"[DB] Звонки для user_id {user_id} не найдены.")
        return result
    except Exception as e:
        logger.error(f"[DB] Ошибка при получении звонков для user_id {user_id}: {e}")
        return None

async def get_operator_call_metrics(user_id, start_date=None, end_date=None):
    """Получение метрик звонков оператора за определенный период."""
    query = """
        SELECT COUNT(*) as total_calls, 
               AVG(talk_duration) as avg_talk_time,
               SUM(CASE WHEN result = 'success' THEN 1 ELSE 0 END) as successful_calls
        FROM call_scores cs
        JOIN users u ON cs.caller_info LIKE CONCAT('%', u.extension, '%') OR cs.called_info LIKE CONCAT('%', u.extension, '%')
        WHERE u.user_id = %s
    """
    params = [user_id]
    if start_date:
        query += " AND cs.call_date >= %s"
        params.append(start_date)
    if end_date:
        query += " AND cs.call_date <= %s"
        params.append(end_date)

    try:
        result = await execute_query(query, params, fetchone=True)
        if result:
            logger.info(f"[DB] Метрики звонков для user_id {user_id} найдены.")
        else:
            logger.info(f"[DB] Метрики звонков для user_id {user_id} не найдены.")
        return result
    except Exception as e:
        logger.error(f"[DB] Ошибка при получении метрик для user_id {user_id}: {e}")
        return None

# Закрытие пула
async def close_connection():
    """Закрытие пула соединений."""
    await close_async_connection()
