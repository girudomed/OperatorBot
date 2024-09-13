import aiomysql
import os
import logging
import time  # Для замера времени
from dotenv import load_dotenv
import asyncio

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
from logger_utils import setup_logging
logger = setup_logging()

class OperatorData:
    def __init__(self):
        """
        Инициализация асинхронного подключения к базе данных MySQL.
        """
        self.connection = None

    async def create_connection(self):
        """
        Создание асинхронного подключения к базе данных с ретраями.
        """
        retries = 3
        delay = 5  # задержка между попытками подключения
        for attempt in range(retries):
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
                logger.info("[КРОТ]: Подключение к базе данных успешно установлено.")
                await self._check_table_exists("call_scores")  # Проверим таблицу call_scores
                return
            except Exception as e:
                logger.error(f"[КРОТ]: Ошибка при подключении к базе данных: {e}")
                if attempt < retries - 1:
                    logger.info(f"[КРОТ]: Повторная попытка подключения через {delay} секунд...")
                    await asyncio.sleep(delay)
                else:
                    logger.error("[КРОТ]: Все попытки подключения к базе данных исчерпаны.")
                    raise

    async def _check_table_exists(self, table_name):
        """
        Проверка существования таблицы в базе данных.
        """
        try:
            query = f"SHOW TABLES LIKE '{table_name}'"
            start_time = time.time()
            async with self.connection.cursor() as cursor:
                await cursor.execute(query)
                result = await cursor.fetchone()
                elapsed_time = time.time() - start_time
                if not result:
                    raise ValueError(f"Таблица '{table_name}' не существует в базе данных.")
                logger.info(f"[КРОТ]: Таблица '{table_name}' успешно найдена (Время выполнения: {elapsed_time:.4f} сек).")
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при проверке таблицы '{table_name}': {e}")
            raise

    async def get_operator_metrics(self, operator_id):
        """
        Асинхронное извлечение данных по конкретному оператору на основе его ID.
        """
        try:
            query = "SELECT * FROM operators WHERE operator_id = %s"
            start_time = time.time()
            async with self.connection.cursor() as cursor:
                await cursor.execute(query, (operator_id,))
                result = await cursor.fetchone()
                elapsed_time = time.time() - start_time
                if result:
                    logger.info(f"[КРОТ]: Данные оператора с ID {operator_id} успешно извлечены (Время выполнения: {elapsed_time:.4f} сек).")
                else:
                    logger.warning(f"[КРОТ]: Оператор с ID {operator_id} не найден (Время выполнения: {elapsed_time:.4f} сек).")
                return result
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при получении данных оператора с ID {operator_id}: {e}")
            return None

    async def get_all_operators_metrics(self):
        """
        Асинхронное извлечение данных по всем операторам.
        """
        try:
            query = "SELECT * FROM operators"
            start_time = time.time()
            async with self.connection.cursor() as cursor:
                await cursor.execute(query)
                result = await cursor.fetchall()
                elapsed_time = time.time() - start_time
                logger.info(f"[КРОТ]: Данные по всем операторам успешно извлечены (Время выполнения: {elapsed_time:.4f} сек).")
                return result
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при получении данных всех операторов: {e}")
            return []

    async def get_operator_call_data(self, operator_id):
        """
        Получение данных о звонках для конкретного оператора из таблицы call_scores.
        """
        try:
            query = """
            SELECT caller_info, called_info, transcript, result, call_date, talk_duration
            FROM call_scores
            WHERE operator_id = %s
            """
            start_time = time.time()
            async with self.connection.cursor() as cursor:
                await cursor.execute(query, (operator_id,))
                result = await cursor.fetchall()
                elapsed_time = time.time() - start_time
                logger.info(f"[КРОТ]: Данные звонков для оператора с ID {operator_id} успешно извлечены (Время выполнения: {elapsed_time:.4f} сек).")
                return result
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при получении данных звонков для оператора с ID {operator_id}: {e}")
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

            start_time = time.time()
            async with self.connection.cursor() as cursor:
                await cursor.execute(query, params)
                result = await cursor.fetchall()
                elapsed_time = time.time() - start_time
                logger.info(f"[КРОТ]: Данные операторов по производительности успешно извлечены "
                            f"(min_calls={min_calls}, max_calls={max_calls}, Время выполнения: {elapsed_time:.4f} сек).")
                return result
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при получении данных по производительности операторов: {e}")
            return []

    async def get_operator_call_metrics(self, operator_id, start_date=None, end_date=None):
        """
        Получение метрик звонков оператора за определенный период (с датой начала и конца).
        """
        try:
            query = """
            SELECT COUNT(*) as total_calls, 
                   AVG(talk_duration) as avg_talk_time,
                   SUM(CASE WHEN result = 'success' THEN 1 ELSE 0 END) as successful_calls
            FROM call_scores
            WHERE operator_id = %s
            """
            params = [operator_id]

            if start_date:
                query += " AND call_date >= %s"
                params.append(start_date)
            if end_date:
                query += " AND call_date <= %s"
                params.append(end_date)

            start_time = time.time()
            async with self.connection.cursor() as cursor:
                await cursor.execute(query, params)
                result = await cursor.fetchone()
                elapsed_time = time.time() - start_time
                logger.info(f"[КРОТ]: Метрики звонков для оператора с ID {operator_id} успешно извлечены (Время выполнения: {elapsed_time:.4f} сек).")
                return result
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при получении метрик звонков для оператора с ID {operator_id}: {e}")
            return None

    async def close_connection(self):
        """
        Закрытие подключения к базе данных.
        """
        if self.connection:
            await self.connection.ensure_closed()
            logger.info("[КРОТ]: Соединение с базой данных закрыто.")

# Пример использования модуля
if __name__ == "__main__":
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

        # Пример получения данных звонков по оператору
        call_data = await operator_data.get_operator_call_data(operator_id)
        print(f"Данные звонков оператора {operator_id}: {call_data}")

        # Пример получения метрик звонков оператора за период
        call_metrics = await operator_data.get_operator_call_metrics(operator_id, "2024-01-01", "2024-09-12")
        print(f"Метрики звонков оператора {operator_id}: {call_metrics}")

        await operator_data.close_connection()

    asyncio.run(main())
