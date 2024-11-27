###operator_data.py

from asyncio.log import logger
import logging
import time
from typing import Any, Dict, List

import aiomysql  # Для замера времени
from logger_utils import setup_logging
import datetime
from datetime import timedelta
import traceback
from typing import Union

logger = logging.getLogger("operator_data")

def validate_and_format_date(
        input_date: Union[str, datetime.date, datetime.datetime]
    ) -> datetime.datetime:
        """
        Преобразует дату в формат datetime.datetime.

        :param input_date: Дата в формате строки, datetime.date или datetime.datetime.
        :return: Объект datetime.datetime.
        """
        if isinstance(input_date, datetime.datetime):
            return input_date
        elif isinstance(input_date, datetime.date):
            return datetime.datetime.combine(input_date, datetime.time.min)
        elif isinstance(input_date, str):
            try:
                return datetime.datetime.strptime(input_date, '%Y-%m-%d')
            except ValueError:
                raise ValueError(f"Неверный формат даты: {input_date}. Используйте YYYY-MM-DD.")
        else:
            raise TypeError("Дата должна быть строкой, datetime.date или datetime.datetime.")
        
def format_date_for_mysql(dt: datetime.datetime, mysql_type: str) -> str:
        logger.debug(f"Inside format_date_for_mysql: dt={dt}, mysql_type={mysql_type}")
        """
        Преобразует дату/время для использования в SQL-запросах MySQL.

        :param dt: Объект datetime.
        :param mysql_type: Тип данных MySQL ('TIMESTAMP' или 'DATETIME').
        :return: Строка в формате для SQL-запроса.
        """
        if not isinstance(dt, datetime.datetime):
            logger.error(f"[КРОТ]: Некорректный тип даты: {type(dt)}. Ожидался datetime.datetime.")
            raise TypeError("dt должен быть объектом datetime.datetime")

        if mysql_type == "TIMESTAMP":
            return dt.strftime('%Y-%m-%d %H:%M:%S')  # TIMESTAMP принимает этот формат
        elif mysql_type == "DATETIME":
            return dt.strftime('%Y-%m-%d %H:%M:%S')  # DATETIME также
        else:
            raise ValueError(f"Unsupported MySQL type: {mysql_type}")
class OperatorData:
    def __init__(self, db_manager):
        """
        Инициализация с использованием db_manager для взаимодействия с базой данных.
        """
        self.db_manager = db_manager
        self.logger = logger or logging.getLogger(__name__)


    def parse_period(self, period_str, custom_dates=None):
        """
        Парсинг периода в диапазон дат в зависимости от типа отчета или кастомного кортежа дат.
        Аргументы:
            - period_str: строка ('daily', 'weekly', 'biweekly', 'monthly', 'half_year', 'yearly') или 'custom'.
            - custom_dates: кортеж строк или объектов datetime (начальная и конечная дата) для кастомного периода.
        Возвращает:
            - Кортеж с начальной и конечной датой (period_start, period_end).
        Исключения:
            - ValueError: Если формат периода некорректен.
        """
        today = datetime.today().date()

        # Обработка кастомного диапазона
        if period_str == 'custom':
            if not custom_dates or len(custom_dates) != 2:
                raise ValueError("Для периода 'custom' требуется кортеж из двух дат (start_date, end_date).")

            try:
                start_date, end_date = custom_dates
                # Конвертация строковых дат в объекты datetime.date
                if isinstance(start_date, str):
                    start_date = datetime.strptime(start_date.strip(), '%Y-%m-%d').date()
                if isinstance(end_date, str):
                    end_date = datetime.strptime(end_date.strip(), '%Y-%m-%d').date()

                # Проверка порядка дат
                if start_date > end_date:
                    raise ValueError("Начальная дата не может быть больше конечной даты.")
                return start_date, end_date
            except ValueError as e:
                raise ValueError(f"Некорректный формат дат для кастомного периода: {e}")

        # Если период передан в виде строки
        try:
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
            elif period_str == 'quarterly':
                # Рассчёт квартала
                month = today.month
                start_quarter_month = (month - 1) // 3 * 3 + 1
                start_quarter = today.replace(month=start_quarter_month, day=1)
                return start_quarter, today
            elif period_str == 'half_year':
                start_half_year = today - timedelta(days=183)
                return start_half_year, today
            elif period_str == 'yearly':
                start_year = today.replace(month=1, day=1)
                return start_year, today
            else:
                raise ValueError(f"Неизвестный период: {period_str}.")
        except Exception as e:
            raise ValueError(f"Ошибка при обработке периода {period_str}: {e}")

    async def get_operator_calls(self, extension, start_date, end_date) -> List[Dict[str, Any]]:
        """
        Получает данные о звонках оператора из call_history и call_scores,
        объединяет их и возвращает полный список звонков.
        """
        try:
            # Преобразование дат
            start_datetime = validate_and_format_date(start_date)
            end_datetime = validate_and_format_date(end_date)
            end_datetime = end_datetime.replace(hour=23, minute=59, second=59, microsecond=999999)

            # Преобразование дат для call_history (в timestamp)
            start_timestamp = int(start_datetime.timestamp())
            end_timestamp = int(end_datetime.timestamp())

            # Преобразование дат для call_scores (в строковый формат DATETIME)
            start_datetime_str = start_datetime.strftime('%Y-%m-%d %H:%M:%S')
            end_datetime_str = end_datetime.strftime('%Y-%m-%d %H:%M:%S')

            # Получение данных из call_history
            call_history_query = """
            SELECT history_id, caller_info, called_info, context_start_time, talk_duration
            FROM call_history
            WHERE 
                (caller_info LIKE %s OR called_info LIKE %s)
                AND context_start_time BETWEEN %s AND %s
            """
            params_call_history = (
                f"%{extension}%",
                f"%{extension}%",
                start_timestamp,
                end_timestamp
            )
            call_history_data = await self.db_manager.execute_query(call_history_query, params_call_history, fetchall=True)

            # Получение данных из call_scores
            call_scores_query = """
            SELECT history_id, call_category, call_score, result
            FROM call_scores
            WHERE 
                (caller_info LIKE %s OR called_info LIKE %s)
                AND call_date BETWEEN %s AND %s
            """
            params_call_scores = (
                f"%{extension}%",
                f"%{extension}%",
                start_datetime_str,
                end_datetime_str
            )
            call_scores_data = await self.db_manager.execute_query(call_scores_query, params_call_scores, fetchall=True)

            self.logger.info(f"[КРОТ]: Получено {len(call_history_data)} записей из call_history и {len(call_scores_data)} записей из call_scores для оператора {extension}")

            # Создание словаря для быстрого доступа к call_scores по history_id
            call_scores_dict = {row['history_id']: row for row in call_scores_data}

            # Объединение данных
            combined_calls = []
            for call in call_history_data:
                history_id = call['history_id']
                if history_id in call_scores_dict:
                    # Объединяем данные
                    combined_call = {**call, **call_scores_dict[history_id]}
                else:
                    # Если нет соответствия в call_scores, добавляем call_category и другие поля как None
                    combined_call = {**call, 'call_category': None, 'call_score': None, 'result': None}
                combined_calls.append(combined_call)

            return combined_calls

        except Exception as e:
            self.logger.error(f"[КРОТ]: Ошибка при получении данных звонков для оператора {extension}: {e}")
            return []

    async def get_average_talk_duration_by_category(
            self,
            connection,
            extension,
            start_date=None,
            end_date=None
        ) -> Dict[str, float]:
        """
        Получение среднего времени разговора по каждой категории.

        Возвращает:
            - Словарь с категориями и средними временами разговора.
        """
        try:
            # Преобразование дат
            operator_filter = [f"%{extension}%", f"%{extension}%"]
            date_filter = []
            
            if start_date:
                start_date = validate_and_format_date(start_date)
                date_filter.append(start_date)
            if end_date:
                end_date = validate_and_format_date(end_date)
                date_filter.append(end_date)

            # Запрос для получения средних времен по категориям
            duration_query = f"""
            SELECT
                call_category,
                AVG(CAST(talk_duration AS DECIMAL(10,2))) AS avg_duration
            FROM call_scores
            WHERE (caller_info LIKE %s OR called_info LIKE %s)
            AND CAST(talk_duration AS DECIMAL(10,2)) > 10
            """
            if start_date and end_date:
                duration_query += " AND call_date BETWEEN %s AND %s"
            elif start_date:
                duration_query += " AND call_date >= %s"
            elif end_date:
                duration_query += " AND call_date <= %s"

            duration_query += " GROUP BY call_category"

            duration_params = operator_filter + date_filter

            # Выполнение запроса
            async with connection.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(duration_query, duration_params)
                durations = await cursor.fetchall()

            # Формирование словаря с результатами
            avg_durations = {}
            for row in durations:
                category = row.get('call_category', 'Неизвестно')
                avg_duration = row.get('avg_duration', 0.0)
                avg_durations[category] = avg_duration

            return avg_durations

        except Exception as e:
            self.logger.error(f"[КРОТ]: Ошибка при получении средних времен разговора по категориям для extension {extension}: {e}")
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
