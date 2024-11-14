import logging
import time  # Для замера времени
from logger_utils import setup_logging
from datetime import datetime, timedelta
import traceback

class OperatorData:
    def __init__(self, db_manager):
        """
        Инициализация с использованием db_manager для взаимодействия с базой данных.
        """
        self.db_manager = db_manager
        self.logger = setup_logging()

    def parse_period(self, period_str):
        """
        Парсинг периода в диапазон дат в зависимости от типа отчета или кастомного кортежа дат.
        Аргументы:
            - period: строка ('daily', 'weekly', 'biweekly', 'monthly', 'half_year', 'yearly') или кортеж дат (начало и конец).
        Возвращает:
            - Кортеж с начальной и конечной датой (period_start, period_end).
        """
        today = datetime.today().date()

        # Если период передан в виде кортежа (начальная и конечная дата)
        if isinstance(period_str, tuple) and len(period_str) == 2:
            # Возвращаем кортеж как есть
            return period_str

        # Если период равен 'daily' - возвращаем сегодняшнюю дату как начало и конец
        if period_str == 'daily':
            return today, today
        elif period_str == 'weekly':
            start_week = today - timedelta(days=today.weekday())
            return start_week, today
        elif period_str == 'biweekly':
            start_biweek = today - timedelta(days=14)
            return start_biweek, today
        elif period_str == 'monthly':
            start_month = today.replace(day=1)
            return start_month, today
        elif period_str == 'half_year':
            start_half_year = today - timedelta(days=183)
            return start_half_year, today
        elif period_str == 'yearly':
            start_year = today - timedelta(days=365)
            return start_year, today
        # Если период не распознан, вызываем ошибку
        else:
            raise ValueError(f"Неизвестный период: {period_str}. Операторы должны быть 'daily', 'weekly', 'monthly' или кортеж дат.")

    async def get_operator_metrics(self, user_id, period):
        """
        Асинхронное извлечение данных по конкретному оператору на основе его user_id и периода.
        Аргументы:
            - user_id: идентификатор пользователя.
            - period: строка, представляющая период ('daily', 'weekly', 'monthly') или кортеж дат.
        """
        try:
            # Преобразуем период в диапазон дат
            if isinstance(period, str):
                period_start, period_end = self.parse_period(period)
            elif isinstance(period, tuple) and len(period) == 2:
                period_start, period_end = period
            else:
                raise ValueError(f"Неверный формат периода: {period}")

            # Логирование перед SQL-запросом
            self.logger.debug(f"[КРОТ]: Получение данных для пользователя {user_id} за период с {period_start} по {period_end}")

            query = """
            SELECT u.*, cs.call_date, cs.call_score, cs.result, cs.talk_duration
            FROM UsersTelegaBot u
            JOIN call_scores cs ON u.user_id = CAST(cs.caller_info AS SIGNED)
            WHERE u.user_id = %s
            AND cs.call_date BETWEEN %s AND %s
            """
            params = [user_id, period_start, period_end]

            start_time = time.time()
            result = await self.db_manager.execute_query(query, params, fetchone=True)
            # проверяем, что результат это словарь, а не строка
            if result and isinstance(result, dict):
                self.logger.info(f"Данные пользователя с user_id {user_id} за период {period} успешно извлечены.")
            else:
                self.logger.warning(f"Данные для user_id {user_id} не найдены или некорректны.")
            elapsed_time = time.time() - start_time
            if result and isinstance(result, dict):  # Проверяем, что результат - это словарь
                self.logger.info(f"[КРОТ]: Данные пользователя с user_id {user_id} за период {period} успешно извлечены (Время выполнения: {elapsed_time:.4f} сек).")
            else:
                self.logger.warning(f"[КРОТ]: Пользователь с user_id {user_id} не найден или данные некорректны за период {period} (Время выполнения: {elapsed_time:.4f} сек).")
                return {}
            self.logger.debug(f"[КРОТ]: Полученные данные: {result}")
            return result
        except Exception as e:
            self.logger.error(f"[КРОТ]: Ошибка при получении данных пользователя с user_id {user_id} за период {period}: {e}")
            return {}

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
                return []
            return result
        except Exception as e:
            self.logger.error(f"[КРОТ]: Ошибка при получении данных всех пользователей: {e}")
            return []

    async def get_operator_call_data(self, extension):
        """
        Получение данных о звонках для конкретного оператора (extension) из таблицы call_scores.
        """
        try:
            query = """
            SELECT caller_info, called_info, transcript, result, call_date, talk_duration
            FROM call_scores
            WHERE caller_info LIKE %s OR called_info LIKE %s
            """
            operator_pattern = f"%{str(extension)}%"
            start_time = time.time()
            result = await self.db_manager.execute_query(query, (operator_pattern, operator_pattern), fetchall=True)
            elapsed_time = time.time() - start_time
            if result and isinstance(result, list):  # Проверяем, что результат - это список
                self.logger.info(f"[КРОТ]: Данные звонков для пользователя с extension {extension} успешно извлечены (Время выполнения: {elapsed_time:.4f} сек).")
            else:
                self.logger.warning(f"[КРОТ]: Данные звонков для пользователя с extension {extension} некорректны или отсутствуют (Время выполнения: {elapsed_time:.4f} сек).")
                return []
            return result
        except Exception as e:
            self.logger.error(f"[КРОТ]: Ошибка при получении данных звонков для пользователя с extension {extension}: {e}")
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
                return []
            return result
        except Exception as e:
            self.logger.error(f"[КРОТ]: Ошибка при получении данных по производительности пользователей: {e}")
            return []

    async def get_operator_call_metrics(self, extension, start_date=None, end_date=None):
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
            params = [f"%{extension}%", f"%{extension}%"]

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
                self.logger.info(f"[КРОТ]: Метрики звонков для пользователя с extension {extension} успешно извлечены (Время выполнения: {elapsed_time:.4f} сек).")
            if not result:
                self.logger.warning(f"[КРОТ]: Пустой результат запроса для extension {extension}.")
            else:
                self.logger.warning(f"[КРОТ]: Метрики звонков для пользователя с extension {extension} некорректны или отсутствуют (Время выполнения: {elapsed_time:.4f} сек).")
                return {}
            return result
        except Exception as e:
            self.logger.error(f"[КРОТ]: Ошибка при получении метрик звонков для пользователя с extension {extension}: {e}")
            return {}

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
        period = "daily"  # Пример периода
        operator_metrics = await operator_data.get_operator_metrics(user_id, period)
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
