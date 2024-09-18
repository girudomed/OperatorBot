import logging
import time  # Для замера времени
from logger_utils import setup_logging

class OperatorData:
    def __init__(self, db_manager):
        """
        Инициализация с использованием db_manager для взаимодействия с базой данных.
        """
        self.db_manager = db_manager
        self.logger = setup_logging()

    async def get_operator_metrics(self, user_id):
        """
        Асинхронное извлечение данных по конкретному оператору на основе его user_id.
        """
        try:
            query = "SELECT * FROM UsersTelegaBot WHERE user_id = %s"
            start_time = time.time()
            result = await self.db_manager.execute_query(query, (user_id,), fetchone=True)
            elapsed_time = time.time() - start_time
            if result and isinstance(result, dict):  # Проверяем, что результат - это словарь
                self.logger.info(f"[КРОТ]: Данные пользователя с user_id {user_id} успешно извлечены (Время выполнения: {elapsed_time:.4f} сек).")
            else:
                self.logger.warning(f"[КРОТ]: Пользователь с user_id {user_id} не найден или данные некорректны (Время выполнения: {elapsed_time:.4f} сек).")
            return result
        except Exception as e:
            self.logger.error(f"[КРОТ]: Ошибка при получении данных пользователя с user_id {user_id}: {e}")
            return None

    async def get_all_operators_metrics(self):
        """
        Асинхронное извлечение данных по всем операторам.
        """
        try:
            query = "SELECT * FROM UsersTelegaBot"
            start_time = time.time()
            result = await self.db_manager.execute_query(query, fetchall=True)
            elapsed_time = time.time() - start_time
            if result and isinstance(result, list):  # Проверяем, что результат - это список
                self.logger.info(f"[КРОТ]: Данные по всем пользователям успешно извлечены (Время выполнения: {elapsed_time:.4f} сек).")
            else:
                self.logger.warning(f"[КРОТ]: Данные по пользователям некорректны или отсутствуют (Время выполнения: {elapsed_time:.4f} сек).")
            return result
        except Exception as e:
            self.logger.error(f"[КРОТ]: Ошибка при получении данных всех пользователей: {e}")
            return []

    async def get_operator_call_data(self, user_id):
        """
        Получение данных о звонках для конкретного оператора (user_id) из таблицы call_scores.
        """
        try:
            query = """
            SELECT caller_info, called_info, transcript, result, call_date, talk_duration
            FROM call_scores
            WHERE caller_info LIKE %s OR called_info LIKE %s
            """
            operator_pattern = f"%{user_id}%"
            start_time = time.time()
            result = await self.db_manager.execute_query(query, (operator_pattern, operator_pattern), fetchall=True)
            elapsed_time = time.time() - start_time
            if result and isinstance(result, list):  # Проверяем, что результат - это список
                self.logger.info(f"[КРОТ]: Данные звонков для пользователя с user_id {user_id} успешно извлечены (Время выполнения: {elapsed_time:.4f} сек).")
            else:
                self.logger.warning(f"[КРОТ]: Данные звонков для пользователя с user_id {user_id} некорректны или отсутствуют (Время выполнения: {elapsed_time:.4f} сек).")
            return result
        except Exception as e:
            self.logger.error(f"[КРОТ]: Ошибка при получении данных звонков для пользователя с user_id {user_id}: {e}")
            return []

    async def get_operators_by_performance(self, min_calls=None, max_calls=None):
        """
        Асинхронное извлечение данных операторов, соответствующих определенным критериям (например, количество звонков).
        """
        try:
            query = "SELECT * FROM UsersTelegaBot WHERE 1=1"
            params = []

            if min_calls is not None:
                query += " AND calls >= %s"
                params.append(min_calls)
            if max_calls is not None:
                query += " AND calls <= %s"
                params.append(max_calls)

            start_time = time.time()
            result = await self.db_manager.execute_query(query, params, fetchall=True)
            elapsed_time = time.time() - start_time
            if result and isinstance(result, list):  # Проверяем, что результат - это список
                self.logger.info(f"[КРОТ]: Данные пользователей по производительности успешно извлечены "
                                 f"(min_calls={min_calls}, max_calls={max_calls}, Время выполнения: {elapsed_time:.4f} сек).")
            else:
                self.logger.warning(f"[КРОТ]: Данные по производительности пользователей некорректны или отсутствуют (Время выполнения: {elapsed_time:.4f} сек).")
            return result
        except Exception as e:
            self.logger.error(f"[КРОТ]: Ошибка при получении данных по производительности пользователей: {e}")
            return []

    async def get_operator_call_metrics(self, user_id, start_date=None, end_date=None):
        """
        Получение метрик звонков оператора за определенный период (с датой начала и конца).
        """
        try:
            query = """
            SELECT COUNT(*) as total_calls, 
                   AVG(talk_duration) as avg_talk_time,
                   SUM(CASE WHEN result = 'success' THEN 1 ELSE 0 END) as successful_calls
            FROM call_scores
            WHERE (caller_info LIKE %s OR called_info LIKE %s)
            """
            params = [f"%{user_id}%", f"%{user_id}%"]

            if start_date:
                query += " AND call_date >= %s"
                params.append(start_date)
            if end_date:
                query += " AND call_date <= %s"
                params.append(end_date)

            start_time = time.time()
            result = await self.db_manager.execute_query(query, params, fetchone=True)
            elapsed_time = time.time() - start_time
            if result and isinstance(result, dict):  # Проверяем, что результат - это словарь
                self.logger.info(f"[КРОТ]: Метрики звонков для пользователя с user_id {user_id} успешно извлечены (Время выполнения: {elapsed_time:.4f} сек).")
            else:
                self.logger.warning(f"[КРОТ]: Метрики звонков для пользователя с user_id {user_id} некорректны или отсутствуют (Время выполнения: {elapsed_time:.4f} сек).")
            return result
        except Exception as e:
            self.logger.error(f"[КРОТ]: Ошибка при получении метрик звонков для пользователя с user_id {user_id}: {e}")
            return None

# Пример использования модуля
if __name__ == "__main__":
    import asyncio
    from db_module import DatabaseManager

    async def main():
        db_manager = DatabaseManager()
        await db_manager.create_pool()
        operator_data = OperatorData(db_manager)

        # Пример получения данных по конкретному пользователю
        user_id = 1
        operator_metrics = await operator_data.get_operator_metrics(user_id)
        print(f"Данные по пользователю {user_id}: {operator_metrics}")

        # Пример получения данных по всем операторам
        all_operators_metrics = await operator_data.get_all_operators_metrics()
        print("Данные по всем операторам:")
        for metrics in all_operators_metrics:
            print(metrics)

        # Пример получения данных звонков по оператору
        call_data = await operator_data.get_operator_call_data(user_id)
        print(f"Данные звонков пользователя {user_id}: {call_data}")

        # Пример получения метрик звонков пользователя за период
        call_metrics = await operator_data.get_operator_call_metrics(user_id, "2024-01-01", "2024-09-12")
        print(f"Метрики звонков пользователя {user_id}: {call_metrics}")

        await db_manager.close_connection()

    asyncio.run(main())
