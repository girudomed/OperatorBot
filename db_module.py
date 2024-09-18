import asyncio
import time
import aiomysql
import logging
import os
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler

# Загрузка переменных окружения из .env
load_dotenv()

# Настройка логирования
log_handler = RotatingFileHandler('logs.log', maxBytes=10**6, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log_handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)

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

    async def close_pool(self):
        """Закрытие пула соединений."""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            self.pool = None
            logger.info("[DB] Пул соединений закрыт.")

    async def execute_query(self, query, params=None, fetchone=False, fetchall=False, retries=3):
        """Универсальная функция для выполнения SQL-запросов с поддержкой повторных попыток."""
        await self.create_pool()
        for attempt in range(retries):
            try:
                async with self.pool.acquire() as connection:
                    async with connection.cursor() as cursor:
                        start_time = time.time()
                        await cursor.execute(query, params)
                        elapsed_time = time.time() - start_time
                        logger.info(f"[DB] Запрос выполнен за {elapsed_time:.4f} сек.")
                        if fetchone:
                            result = await cursor.fetchone()
                            logger.debug(f"[DB] Получена одна запись: {result}")
                            return result
                        if fetchall:
                            result = await cursor.fetchall()
                            logger.debug(f"[DB] Получено {len(result)} записей.")
                            return result
                        await connection.commit()
                        return True
            except aiomysql.Error as e:
                logger.error(f"[DB] Ошибка выполнения запроса: {e}")
                if attempt < retries - 1:
                    logger.warning(f"[DB] Повторная попытка запроса ({attempt + 1}/{retries})...")
                    await asyncio.sleep(1)
                else:
                    raise
            except Exception as e:
                logger.error(f"Общая ошибка при выполнении запроса: {e}")
                raise

    # === Управление пользователями === #
    async def register_user_if_not_exists(self, user_id, username, full_name, operator_id=None, password=None, role_id=None):
        """Регистрация пользователя, если он не существует в базе данных."""
        query_check = "SELECT * FROM UsersTelegaBot WHERE user_id = %s"
        user = await self.execute_query(query_check, (user_id,), fetchone=True)
        if not user:
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
        return result

    async def get_user_role(self, user_id):
        """Получение роли пользователя по user_id."""
        query = "SELECT role_id FROM UsersTelegaBot WHERE user_id = %s"
        user_role = await self.execute_query(query, (user_id,), fetchone=True)
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
        return result['password'] if result else None

    async def find_operator_by_id(self, operator_id):
        """Поиск оператора по operator_id в таблице users."""
        query = "SELECT * FROM users WHERE extension = %s"
        result = await self.execute_query(query, (operator_id,), fetchone=True)
        return result

    async def find_operator_by_name(self, operator_name):
        """Поиск оператора по имени в таблице users."""
        query = "SELECT * FROM users WHERE name = %s"
        result = await self.execute_query(query, (operator_name,), fetchone=True)
        return result

    async def get_role_id_by_name(self, role_name):
        """Получение role_id по названию роли."""
        query = "SELECT id FROM RolesTelegaBot WHERE role_name = %s"
        result = await self.execute_query(query, (role_name,), fetchone=True)
        return result['id'] if result else None

    async def get_role_name_by_id(self, role_id):
        """Получение названия роли по role_id."""
        query = "SELECT role_name FROM RolesTelegaBot WHERE id = %s"
        result = await self.execute_query(query, (role_id,), fetchone=True)
        return result['role_name'] if result else None

    # === Работа с отчётами === #
    async def save_report_to_db(self, user_id, total_calls, accepted_calls, booked_services, conversion_rate, 
                                avg_call_rating, total_cancellations, cancellation_rate, total_conversation_time, 
                                avg_conversation_time, avg_spam_time, total_spam_time, total_navigation_time, 
                                avg_navigation_time, total_talk_time, complaint_calls, complaint_rating, recommendations):
        """Сохранение отчета в базу данных."""
        # Логируем переданные данные
        logger.debug(f"Saving report to DB for user_id: {user_id}, data: {locals()}")

        # Проверка типов данных перед вставкой
        if not isinstance(total_calls, int) or not isinstance(accepted_calls, int) or not isinstance(booked_services, int):
            logger.error("Тип данных для total_calls, accepted_calls или booked_services некорректен.")
            return

        query = """
        INSERT INTO reports (user_id, report_date, total_calls, accepted_calls, booked_services, conversion_rate, 
                             avg_call_rating, total_cancellations, cancellation_rate, total_conversation_time, 
                             avg_conversation_time, avg_spam_time, total_spam_time, total_navigation_time, 
                             avg_navigation_time, total_talk_time, complaint_calls, complaint_rating, recommendations)
        VALUES (%s, CURRENT_DATE, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
        await self.execute_query(query, (
            user_id, total_calls, accepted_calls, booked_services, conversion_rate, avg_call_rating,
            total_cancellations, cancellation_rate, total_conversation_time, avg_conversation_time,
            avg_spam_time, total_spam_time, total_navigation_time, avg_navigation_time, total_talk_time,
            complaint_calls, complaint_rating, recommendations
        ))
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
        return await self.execute_query(query, fetchall=True)

    # === Работа с таблицей call_scores === #
    async def get_operator_calls(self, operator_id, start_date, end_date):
        """Получение звонков оператора за указанный период."""
        query = """
        SELECT * FROM call_scores
        WHERE (caller_info LIKE %s OR called_info LIKE %s)
        AND call_date BETWEEN %s AND %s
        """
        operator_pattern = f"%{operator_id}%"
        return await self.execute_query(query, (operator_pattern, operator_pattern, start_date, end_date), fetchall=True)

    # === Работа с таблицами === #
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
