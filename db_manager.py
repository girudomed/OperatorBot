import aiomysql
import logging
import time  # Для замера времени выполнения
from logger_utils import setup_logging

# Настройка логирования
logger = setup_logging()

class DBManager:
    def __init__(self, db_config):
        """
        Инициализация менеджера базы данных с настройками.
        :param db_config: Конфигурация подключения к базе данных.
        """
        self.db_config = db_config
        self.pool = None
    async def connect(self):
        """
        Подключение к базе данных и создание пула соединений.
        """
        try:
            self.pool = await aiomysql.create_pool(
                host=self.db_config['host'],
                port=self.db_config['port'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                db=self.db_config['db'],
                autocommit=self.db_config['autocommit'],
                minsize=1, maxsize=10
            )
            logger.info("[DBManager]: Пул соединений успешно создан.")
        except aiomysql.Error as e:
            logger.error(f"[DBManager]: Ошибка при подключении к базе данных: {e}")
            raise

    async def disconnect(self):
        """
        Закрытие пула соединений.
        """
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            logger.info("[DBManager]: Пул соединений закрыт.")

    async def __aenter__(self):
        """
        Контекстный менеджер для открытия соединения.
        """
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Контекстный менеджер для закрытия соединения.
        """
        await self.disconnect()

    async def execute_query(self, query, params=None):
        """
        Выполнение SQL-запроса с логированием времени выполнения и обработки ошибок.
        :param query: SQL-запрос.
        :param params: Параметры для выполнения запроса.
        :return: Результат выполнения запроса или None в случае ошибки.
        """
        if not self.pool:
            await self.create_pool()
            
        async with self.pool.acquire() as connection:
            async with connection.cursor() as cursor:
                start_time = time.time()
                try:
                    logger.info(f"[DBManager]: Выполнение запроса: {query} с параметрами: {params}")
                    await cursor.execute(query, params)
                    result = await cursor.fetchall()
                    elapsed_time = time.time() - start_time
                    logger.info(f"[DBManager]: Запрос выполнен за {elapsed_time:.4f} сек.")
                    
                    if result and isinstance(result, list):
                       logger.debug(f"Получены данные: {result}") 
                       return result
                    else:
                        logger.warning(f"Данные не найдены для запроса: {query}")
                        return []   
                
                except aiomysql.Error as e:
                    logger.error(f"[DBManager]: Ошибка при выполнении запроса: {query}, параметры: {params}, ошибка: {e}")
                    return []

    async def execute_query_one(self, query, params=None):
        """
        Выполнение SQL-запроса и возврат одной записи с логированием времени выполнения.
        :param query: SQL-запрос.
        :param params: Параметры для выполнения запроса.
        :return: Одна запись или None в случае ошибки.
        """
        if not self.pool:
            await self.connect()
        async with self.pool.acquire() as connection:
            async with connection.cursor() as cursor:
                start_time = time.time()
                try:
                    logger.info(f"[DBManager]: Выполнение запроса на одну запись: {query} с параметрами: {params}")
                    await cursor.execute(query, params)
                    result = await cursor.fetchone()
                    elapsed_time = time.time() - start_time
                    logger.info(f"[DBManager]: Запрос на одну запись выполнен за {elapsed_time:.4f} сек.")
                    
                    if result:
                        logger.debug(f"Получена запись: {result}")    
                        return result
                    else:
                     logger.warning(f"Запись не найдена для запроса: {query}")
                    return None    
                except aiomysql.Error as e:
                    logger.error(f"[DBManager]: Ошибка при выполнении запроса на одну запись: {query}, параметры: {params}, ошибка: {e}")
                    return None

    async def get_user_role(self, username):
        """
        Получает роль пользователя по его имени пользователя.
        :param username: Имя пользователя.
        :return: Название роли пользователя или None.
        """
        if not username:
            logger.warning("[DBManager]: Получен пустой username для запроса роли пользователя.")
            return None
        
        query = """
        SELECT roles.role_name 
        FROM users 
        JOIN roles ON users.role_id = roles.id 
        WHERE users.username = %s
        """
        return await self.execute_query_one(query, (username,))
    async def check_permission(self, role_name, permission):
        """
        Проверяет наличие определенного разрешения для указанной роли.
        :param role_name: Название роли пользователя.
        :param permission: Название проверяемого разрешения.
        :return: True, если разрешение существует для этой роли, иначе False.
        """
        if not role_name or not permission:
            logger.warning("[DBManager]: Получены пустые значения для проверки прав доступа.")
            return False

        query = """
        SELECT 1 
        FROM permissions 
        JOIN roles ON permissions.role_id = roles.id 
        WHERE roles.role_name = %s AND permissions.permission_name = %s
        """
        result = await self.execute_query_one(query, (role_name, permission))
        return result is not None
    
    async def register_user_if_not_exists(self, user_id, username, full_name, operator_id=None, password=None, role_id=None):
        """
        Регистрация пользователя, если он не существует в базе данных.
        """
        # Проверка, существует ли пользователь
        if not await self.user_exists(user_id):
            if password is None:
                raise ValueError("Пароль не может быть пустым при регистрации нового пользователя.")
        
            query_insert = """
            INSERT INTO UsersTelegaBot (user_id, username, full_name, password, role_id)
            VALUES (%s, %s, %s, %s, %s)
        """
            await self.execute_query(query_insert, (user_id, username, full_name, password, role_id))
            logger.info(f"[DB] Пользователь '{full_name}' зарегистрирован.")
        else:
            logger.info(f"[DB] Пользователь '{full_name}' уже существует.")

async def ensure_closed(self):
    """
    Закрытие соединения, если оно открыто.
    """
    if self.pool is not None:
        await self.disconnect()
