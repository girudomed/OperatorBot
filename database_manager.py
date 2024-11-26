import asyncio
import time
import aiomysql
import logging
import os
from dotenv import load_dotenv

# Загрузка переменных окружения из .env
load_dotenv()

logger = logging.getLogger(__name__)

# Конфигурация базы данных
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "db": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", 3306)),  # По умолчанию порт MySQL
}

class DatabaseManager:
    def __init__(self):
        self.pool = None
        # Инициализация пула соединений при создании экземпляра класса
        asyncio.create_task(self.create_pool())

    async def get_user_id(self, user_id):
        """
        Получение user_id
        """
        query = """
        SELECT user_id FROM users
        WHERE user_id = %s
        """
        try:
            async with self.pool.acquire() as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(query, (user_id,))
                    result = await cursor.fetchone()
                    if result:
                        return result['user_id']
                    else:
                        return None
        except Exception as e:
            logger.error(f"Ошибка при получении user_id для user_id {user_id}: {e}")
            return None

    async def create_pool(self):
        """Создание пула соединений с базой данных, если он еще не создан."""
        if not self.pool:
            try:
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

    async def execute_query(self, query, params=None, fetchone=False, fetchall=False):
        """Универсальная функция для выполнения SQL-запросов."""
        if not self.pool:
            await self.create_pool()  # Убедимся, что пул создан
        async with self.pool.acquire() as connection:
            async with connection.cursor() as cursor:
                try:
                    start_time = time.time()
                    await cursor.execute(query, params)
                    elapsed_time = time.time() - start_time
                    logger.info(f"[DB] Выполнен запрос: {query} с параметрами {params} за {elapsed_time:.4f} сек.")
                    
                    if fetchone:
                        result = await cursor.fetchone()
                        return result
                    if fetchall:
                        result = await cursor.fetchall()
                        return result
                except aiomysql.Error as e:
                    logger.error(f"[DB] Ошибка выполнения запроса: {query}, параметры: {params}, ошибка: {e}")
                    raise

    async def get_user_password(self, user_id):
        """Получение хешированного пароля пользователя по его user_id."""
        query = "SELECT password FROM UsersTelegaBot WHERE user_id = %s"
        result = await self.execute_query(query, (user_id,), fetchone=True)
        return result['password'] if result else None

    async def close_connection(self):
        """Закрытие пула соединений."""
        await self.close_pool()

# Пример использования:
async def main():
    db_manager = DatabaseManager()

    # Пример использования метода get_user_password
    user_id = 12345  # Введите реальный ID пользователя
    user_password = await db_manager.get_user_password(user_id)
    if user_password:
        print(f"Пароль пользователя: {user_password}")
    else:
        print(f"Пользователь с ID {user_id} не найден.")

    # Закрытие соединений
    await db_manager.close_connection()

# Для тестирования можно использовать следующую команду:
# asyncio.run(main())
