import asyncio
from datetime import datetime, timedelta
import time
import aiomysql
import logging
from dotenv import load_dotenv
import os
from contextlib import asynccontextmanager

# Загрузка переменных окружения из .env
load_dotenv()

# Настройка логирования
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Конфигурация базы данных
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "db": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", 3306)),  # По умолчанию порт MySQL
}

# Проверка обязательных переменных окружения для базы данных
required_db_vars = ["DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME", "DB_PORT"]
missing_vars = [var for var in required_db_vars if not os.getenv(var)]
if missing_vars:
    logger.critical(f"Отсутствуют необходимые переменные окружения для базы данных: {', '.join(missing_vars)}")
    raise EnvironmentError(f"Отсутствуют переменные окружения: {', '.join(missing_vars)}")

class DatabaseManager:
    def __init__(self):
        self.pool = None
        self._lock = asyncio.Lock()
        
    async def create_pool(self):
        """Создание пула соединений с базой данных."""
        async with self._lock:
            if not self.pool:
                try:
                    logger.info("Попытка подключения к базе данных.")
                    self.pool = await aiomysql.create_pool(
                        host=DB_CONFIG["host"],
                        port=DB_CONFIG["port"],
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
                
    @asynccontextmanager     
    async def acquire(self):
        """Возвращает соединение из пула."""
        await self.create_pool()  # Убедитесь, что пул создан
        conn = await self.pool.acquire()
        try:
            yield conn
        finally:
            self.pool.release(conn)
    
    def parse_period(self, period):
        """Формирование диапазона дат для SQL-запроса"""
        today = datetime.today().date()
        if period == "daily":
            return today, today
        elif period == "weekly":
            start_week = today - timedelta(days=today.weekday())
            return start_week, today
        elif period == "biweekly":
            start_biweek = today - timedelta(days=14)
            return start_biweek, today
        elif period == "monthly":
            start_month = today.replace(day=1)
            return start_month, today
        elif period == "half_year":
            start_half_year = today - timedelta(days=183)
            return start_half_year, today
        elif period == "yearly":
            start_year = today - timedelta(days=365)
            return start_year, today
        else:
            raise ValueError(f"Неизвестный период: {period}")

    async def close_pool(self):
        """Закрытие пула соединений."""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            self.pool = None
            logger.info("[DB] Пул соединений закрыт.")

    async def execute_query(self, query, params=None, fetchone=False, fetchall=False):
        """Универсальная функция для выполнения SQL-запросов с поддержкой повторных попыток."""
        await self.create_pool()
        async with self.pool.acquire() as connection:
            async with connection.cursor() as cursor:
                try:
                    # Логируем запрос и параметры перед выполнением
                    logger.debug(f"[DB] Выполнение запроса: {query}, параметры: {params}")
                    start_time = time.time()
                    await cursor.execute(query, params)
                    elapsed_time = time.time() - start_time
                    logger.info(f"[DB] Запрос выполнен за {elapsed_time:.4f} сек.")
                
                    if fetchone:
                        result = await cursor.fetchone()
                        # Проверяем, что результат - словарь или возвращаем пустой словарь
                        logger.debug(f"[DB] Получена одна запись: {result}")
                        return result if isinstance(result, dict) else {}
                
                    if fetchall:
                        result = await cursor.fetchall()
                        # Проверяем, что результат - список словарей или возвращаем пустой список
                        logger.debug(f"[DB] Получено {len(result)} записей.")
                        return result if isinstance(result, list) else []

                    return True  # Если запрос не требует данных
                except aiomysql.Error as e:
                    logger.error(f"[DB] Ошибка выполнения запроса: {query}, параметры: {params}, ошибка: {e}")
                    raise


    # === Управление пользователями === #
    async def register_user_if_not_exists(self, user_id, username, full_name, operator_id=None, password=None, role_id=None):
        """Регистрация пользователя, если он не существует в базе данных."""
        if not await self.user_exists(user_id):
            if password is None:
                raise ValueError("Пароль не может быть пустым при регистрации нового пользователя.")
            query_insert = """
                INSERT INTO UsersTelegaBot (user_id, username, full_name, operator_id, password, role_id)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            await self.execute_query(query_insert, (user_id, username, full_name, operator_id, password, role_id))
            logger.info(f"[DB] Пользователь '{full_name}' зарегистрирован.")
        else:
            logger.info(f"[DB] Пользователь '{full_name}' уже существует.")

    async def get_user_by_id(self, user_id):
        """Получение пользователя по user_id."""
        query = "SELECT * FROM UsersTelegaBot WHERE user_id = %s"
        result = await self.execute_query(query, (user_id,), fetchone=True)
        logger.debug(f"[DB] Результат запроса на extension для user_id {user_id}: {result}")
        if not result or not isinstance(result, dict):
            logger.warning(f"[DB] Пользователь с ID {user_id} не найден.")
            return None
        return result

    async def get_user_role(self, user_id):
        """Получение роли пользователя по user_id."""
        query = "SELECT role_id FROM UsersTelegaBot WHERE user_id = %s"
        user_role = await self.execute_query(query, (user_id,), fetchone=True)
        if not user_role:
            logger.warning(f"[DB] Роль для пользователя с ID {user_id} не найдена.")
        return user_role['role_id'] if user_role else None

    async def update_user_password(self, user_id, hashed_password):
        """Обновление хешированного пароля пользователя."""
        query = "UPDATE UsersTelegaBot SET password = %s WHERE user_id = %s"
        await self.execute_query(query, (hashed_password, user_id))
        logger.info(f"[DB] Пароль для user_id {user_id} успешно обновлен.")

    async def get_user_password(self, user_id):
        """Получение хешированного пароля пользователя по его user_id."""
        query = "SELECT password FROM UsersTelegaBot WHERE user_id = %s"
        result = await self.execute_query(query, (user_id,), fetchone=True)
        if not result or not isinstance(result, dict):
            logger.warning(f"[DB] Пользователь с ID {user_id} не найден.")
            return None
        return result
    
    async def get_role_password_by_id(self, role_id):
        """Получает пароль роли по role_id из таблицы RolesTelegaBot."""
        query = "SELECT role_password FROM RolesTelegaBot WHERE id = %s"
        async with self.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(query, (role_id,))
                result = await cursor.fetchone()
                if result:
                    return result.get('role_password')
                return None

    # === Управление операторами === #
    async def find_operator_by_id(self, user_id):
        """
        Поиск оператора по его ID.
        :param user_id: ID оператора.
        :return: Информация об операторе или None, если оператор не найден.
        """
        query = "SELECT * FROM users WHERE id = %s"
        result = await self.execute_query(query, (user_id,), fetchone=True)
        if not result:
            logger.warning(f"[DB] Оператор с ID {user_id} не найден.")
        return result
    async def find_operator_by_extension(self, extension):
        """Поиск оператора по его extension в таблице users."""
        query = "SELECT * FROM users WHERE extension = %s"
        result = await self.execute_query(query, (extension,), fetchone=True)
        if not result:
            logger.warning(f"[DB] Оператор с extension {extension} не найден.")
        return result

    async def find_operator_by_name(self, operator_name):
        """Поиск оператора по имени в таблице users."""
        query = "SELECT * FROM users WHERE name = %s"
        result = await self.execute_query(query, (operator_name,), fetchone=True)
        if not result:
            logger.warning(f"[DB] Оператор с именем {operator_name} не найден.")
        return result

    async def get_role_id_by_name(self, role_name):
        """Получение role_id по названию роли."""
        query = "SELECT id FROM RolesTelegaBot WHERE role_name = %s"
        result = await self.execute_query(query, (role_name,), fetchone=True)
        if not result or not isinstance(result, dict):
            logger.warning(f"[DB] Пользователь с ID {role_name} не найден.")
            return None
        return result

    async def get_role_name_by_id(self, role_id):
        """Получение названия роли по role_id."""
        query = "SELECT role_name FROM RolesTelegaBot WHERE id = %s"
        result = await self.execute_query(query, (role_id,), fetchone=True)
        if not result or not isinstance(result, dict):
            logger.warning(f"[DB] Пользователь с ID {role_id} не найден.")
            return None
        return result
    
    async def get_operator_extension(self, user_id):
        """
        Получение extension по user_id из таблицы users.
        :param user_id: ID пользователя.
        :return: extension или None, если не найден.
        """
        query = "SELECT extension FROM users WHERE user_id = %s"
        result = await self.execute_query(query, (user_id,), fetchone=True)
        if result and 'extension' in result:
            extension = result['extension']
            logger.info(f"[DB] Найден extension {extension} для user_id {user_id}")
            return extension
        else:
            logger.warning(f"[DB] Extension не найден для user_id {user_id}")
            return None



    # === Работа с отчётами === #
    async def save_report_to_db(self, user_id, total_calls, accepted_calls, booked_services, conversion_rate,
                                avg_call_rating, total_cancellations, cancellation_rate, total_conversation_time,
                                avg_conversation_time, avg_spam_time, total_spam_time, total_navigation_time,
                                avg_navigation_time, total_talk_time, complaint_calls, complaint_rating, recommendations):
        """Сохранение отчета в базу данных."""
        logger.debug(f"Saving report to DB for user_id: {user_id}, data: {locals()}")

        query = """
        INSERT INTO reports (user_id, report_date, total_calls, accepted_calls, booked_services, conversion_rate,
                             avg_call_rating, total_cancellations, cancellation_rate, total_conversation_time,
                             avg_conversation_time, avg_spam_time, total_spam_time, total_navigation_time,
                             avg_navigation_time, total_talk_time, complaint_calls, complaint_rating, recommendations)
        VALUES (%s, CURRENT_DATE, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            total_calls=VALUES(total_calls),
            accepted_calls=VALUES(accepted_calls),
            booked_services=VALUES(booked_services),
            conversion_rate=VALUES(conversion_rate),
            avg_call_rating=VALUES(avg_call_rating),
            total_cancellations=VALUES(total_cancellations),
            cancellation_rate=VALUES(cancellation_rate),
            total_conversation_time=VALUES(total_conversation_time),
            avg_conversation_time=VALUES(avg_conversation_time),
            avg_spam_time=VALUES(avg_spam_time),
            total_spam_time=VALUES(total_spam_time),
            total_navigation_time=VALUES(total_navigation_time),
            avg_navigation_time=VALUES(avg_navigation_time),
            total_talk_time=VALUES(total_talk_time),
            complaint_calls=VALUES(complaint_calls),
            complaint_rating=VALUES(complaint_rating),
            recommendations=VALUES(recommendations)
        """
        params = (
            user_id, total_calls, accepted_calls, booked_services, conversion_rate, avg_call_rating,
            total_cancellations, cancellation_rate, total_conversation_time, avg_conversation_time,
            avg_spam_time, total_spam_time, total_navigation_time, avg_navigation_time, total_talk_time,
            complaint_calls, complaint_rating, recommendations
        )
        await self.execute_query(query, params)
        logger.info(f"[DB] Отчет для user_id {user_id} сохранен.")

    async def get_reports_for_today(self):
        """Получение всех отчетов за текущий день."""
        query = """
        SELECT user_id, report_date, total_calls, accepted_calls, booked_services, conversion_rate, avg_call_rating,
               total_cancellations, cancellation_rate, total_conversation_time, avg_conversation_time, avg_spam_time,
               total_spam_time, total_navigation_time, avg_navigation_time, total_talk_time, complaint_calls,
               complaint_rating, recommendations
        FROM reports
        WHERE report_date = CURRENT_DATE
        """
        
        result = await self.execute_query(query, fetchall=True)
        if not result:
            logger.warning("[DB] Отчеты за текущий день не найдены.")
            return []
        return result

    # === Работа с таблицей call_scores === #
    async def get_operator_calls(self, extension, start_date=None, end_date=None):
        """Получение звонков оператора за указанный период."""
        if not await self.operator_exists(extension):
            logger.warning(f"Оператор с extension {extension} не найден.")
            return []

        query = """
        SELECT u.*, cs.call_date, cs.call_score, cs.result, cs.talk_duration
        FROM UsersTelegaBot u
        JOIN call_scores cs 
        ON (SUBSTRING_INDEX(cs.caller_info, ' ', 1) = u.extension
        OR SUBSTRING_INDEX(cs.called_info, ' ', 1) = u.extension)
        WHERE u.extension = %s
        """
        params = [extension]
        if start_date and end_date:
            query += " AND cs.call_date BETWEEN %s AND %s"
            params.extend( [start_date, end_date])
        result = await self.execute_query(query, params, fetchall=True)
        if not result or not isinstance(result, list):
            logger.warning(f"[DB] Звонки оператора с extension {extension} за период не найдены.")
            return []
        
        return result

    async def get_operator_call_metrics(self, extension, start_date=None, end_date=None):
        """Получение метрик звонков оператора за определенный период."""
        query = """
        SELECT COUNT(*) as total_calls, 
               AVG(talk_duration) as avg_talk_time,
               SUM(CASE WHEN result = 'success' THEN 1 ELSE 0 END) as successful_calls
        FROM call_scores
        WHERE (caller_info LIKE %s OR called_info LIKE %s)
        """
        params = [f"%{extension}%", f"%{extension}%"]

        if start_date:
            query += " AND call_date >= %s"
            params.append(start_date)
        if end_date:
            query += " AND call_date <= %s"
            params.append(end_date)

        result = await self.execute_query(query, params, fetchone=True)
        if not result:
            logger.warning(f"[DB] Метрики звонков для оператора с extension {extension} за период не найдены.")
        return result

    # === Создание таблиц === #
    async def create_tables(self):
        """Создание необходимых таблиц, если их не существует."""
        try:
            await self.create_pool()
            logger.info("Проверка и создание таблиц...")

            await self.execute_query("""
            CREATE TABLE IF NOT EXISTS UsersTelegaBot (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id BIGINT UNIQUE,
                username VARCHAR(255),
                full_name VARCHAR(255),
                operator_id BIGINT,
                password VARBINARY(255),
                role_id INT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )""")

            await self.execute_query("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255),
                role VARCHAR(50),
                extension VARCHAR(50)
            )""")

            await self.execute_query("""
            CREATE TABLE IF NOT EXISTS reports (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id BIGINT,
                report_date DATE,
                total_calls INT,
                accepted_calls INT,
                booked_services INT,
                conversion_rate FLOAT,
                avg_call_rating FLOAT,
                total_cancellations INT,
                cancellation_rate FLOAT,
                total_conversation_time INT,
                avg_conversation_time INT,
                avg_spam_time INT,
                total_spam_time INT,
                total_navigation_time INT,
                avg_navigation_time INT,
                total_talk_time INT,
                complaint_calls INT,
                complaint_rating FLOAT,
                recommendations TEXT,
                generated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY (user_id, report_date),
                FOREIGN KEY (user_id) REFERENCES UsersTelegaBot(user_id)
            )""")

            await self.execute_query("""
            CREATE TABLE IF NOT EXISTS call_scores (
                id INT AUTO_INCREMENT PRIMARY KEY,
                history_id INT,
                call_score TEXT,
                score_date DATE,
                called_info VARCHAR(255),
                call_date DATETIME,
                call_type VARCHAR(50),
                talk_duration VARCHAR(50),
                call_success VARCHAR(50),
                transcript TEXT,
                result TEXT,
                caller_info VARCHAR(255),
                call_category TEXT,
                number_category INT,
                number_checklist INT,
                category_checklist TEXT
            )""")

            logger.info("Все таблицы успешно проверены и созданы.")
        except Exception as e:
            logger.error(f"Ошибка при создании таблиц: {e}")

    # === Основная точка входа для инициализации БД === #
    async def initialize_db(self):
        """Инициализация базы данных: создание таблиц и пула соединений."""
        await self.create_pool()
        await self.create_tables()

    # Закрытие пула соединений
    async def close_connection(self):
        """Закрытие пула соединений."""
        await self.close_pool()

    # Использование контекстного менеджера для работы с базой данных
    async def __aenter__(self):
        await self.create_pool()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close_pool()

    # Добавление метода проверки существования пользователя/оператора
    async def user_exists(self, user_id):
        """Проверка существования пользователя по user_id."""
        query = "SELECT 1 FROM UsersTelegaBot WHERE user_id = %s"
        result = await self.execute_query(query, (user_id,), fetchone=True)
        return bool(result)

    async def operator_exists(self, extension):
        """Проверка существования оператора по extension."""
        query = "SELECT 1 FROM users WHERE extension = %s"
        result = await self.execute_query(query, (extension,), fetchone=True)
        return bool(result)
