import aiomysql
import os
import logging
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

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


class OperatorData:
    def __init__(self):
        """
        Инициализация асинхронного подключения к базе данных MySQL.
        """
        self.connection = None

    async def create_connection(self):
        """
        Создание асинхронного подключения к базе данных.
        """
        try:
            self.connection = await aiomysql.connect(
                host=os.getenv("DB_HOST"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                db=os.getenv("DB_NAME"),
                port=int(os.getenv("DB_PORT")),
                cursorclass=aiomysql.DictCursor,
                autocommit=True
            )
            logger.info("Подключение к базе данных успешно установлено.")
            await self._check_table_exists("operators")
        except Exception as e:
            logger.error(f"Ошибка при подключении к базе данных: {e}")
            if self.connection:
                self.connection.close()

    async def _check_table_exists(self, table_name):
        """
        Проверка существования таблицы в базе данных.
        """
        try:
            query = f"SHOW TABLES LIKE '{table_name}'"
            async with self.connection.cursor() as cursor:
                await cursor.execute(query)
                result = await cursor.fetchone()
                if not result:
                    raise ValueError(f"Таблица '{table_name}' не существует в базе данных.")
                logger.info(f"Таблица '{table_name}' успешно найдена.")
        except Exception as e:
            logger.error(f"Ошибка при проверке таблицы '{table_name}': {e}")
            raise

    async def get_operator_metrics(self, operator_id):
        """
        Асинхронное извлечение данных по конкретному оператору на основе его ID.
        """
        try:
            query = "SELECT * FROM operators WHERE operator_id = %s"
            async with self.connection.cursor() as cursor:
                await cursor.execute(query, (operator_id,))
                result = await cursor.fetchone()
                logger.info(f"Данные оператора с ID {operator_id} успешно извлечены.")
                return result
        except Exception as e:
            logger.error(f"Ошибка при получении данных оператора с ID {operator_id}: {e}")
            return None

    async def get_all_operators_metrics(self):
        """
        Асинхронное извлечение данных по всем операторам.
        """
        try:
            query = "SELECT * FROM operators"
            async with self.connection.cursor() as cursor:
                await cursor.execute(query)
                result = await cursor.fetchall()
                logger.info("Данные по всем операторам успешно извлечены.")
                return result
        except Exception as e:
            logger.error(f"Ошибка при получении данных всех операторов: {e}")
            return []

    async def get_operators_by_performance(self, min_calls=None, max_calls=None):
        """
        Асинхронное извлечение данных операторов, соответствующих определенным критериям (например, количество звонков).
        """
        try:
            query = "SELECT * FROM operators WHERE 1=1"
            params = []

            if min_calls is not None:
                query += " AND calls >= %s"
                params.append(min_calls)
            if max_calls is not None:
                query += " AND calls <= %s"
                params.append(max_calls)

            async with self.connection.cursor() as cursor:
                await cursor.execute(query, params)
                result = await cursor.fetchall()
                logger.info(f"Данные операторов по производительности успешно извлечены (min_calls={min_calls}, max_calls={max_calls}).")
                return result
        except Exception as e:
            logger.error(f"Ошибка при получении данных по производительности операторов: {e}")
            return []

    async def close_connection(self):
        """
        Закрытие подключения к базе данных.
        """
        if self.connection:
            await self.connection.ensure_closed()
            logger.info("Соединение с базой данных закрыто.")

# Пример использования модуля
if __name__ == "__main__":
    import asyncio

    async def main():
        operator_data = OperatorData()
        await operator_data.create_connection()

        # Пример получения данных по конкретному оператору
        operator_id = 1
        operator_metrics = await operator_data.get_operator_metrics(operator_id)
        print(f"Данные по оператору {operator_id}: {operator_metrics}")

        # Пример получения данных по всем операторам
        all_operators_metrics = await operator_data.get_all_operators_metrics()
        print("Данные по всем операторам:")
        for metrics in all_operators_metrics:
            print(metrics)

        await operator_data.close_connection()

    asyncio.run(main())
