###operator_data.py

from asyncio.log import logger
import logging
import time
from typing import Any, Dict

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
        self.logger = setup_logging()

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

    async def get_operator_metrics(self, user_id, period, custom_dates=None):
        """
        Асинхронное извлечение данных по конкретному оператору на основе его user_id и периода.
        
        Аргументы:
            - user_id: идентификатор пользователя.
            - period: строка, представляющая период ('daily', 'weekly', 'monthly') или 'custom'.
            - custom_dates: кортеж строк или объектов datetime (начальная и конечная дата) для кастомного периода.

        Возвращает:
            - Список словарей с данными звонков, включая все необходимые поля для расчетов метрик.
        """
        try:
            # Преобразуем период в диапазон дат
            if period == 'custom' and custom_dates:
                # Обработка кастомного диапазона
                if not isinstance(custom_dates, tuple) or len(custom_dates) != 2:
                    raise ValueError("Для периода 'custom' требуется кортеж из двух дат (start_date, end_date).")
                period_start, period_end = self.parse_period('custom', custom_dates)
            elif isinstance(period, str):
                # Обработка стандартных периодов (daily, weekly и т.д.)
                period_start, period_end = self.parse_period(period)
            else:
                raise ValueError(f"Неверный формат периода: {period}")


            # Логирование параметров запроса
            self.logger.debug(f"[КРОТ]: Подготовка к запросу данных для user_id={user_id} "
                            f"с периодом {period_start} - {period_end}.")

            # Подготовка SQL-запроса и параметров
            query = """
            SELECT 
                u.user_id,
                u.extension,
                cs.call_date,
                cs.call_score,
                cs.result,
                cs.talk_duration,
                cs.call_category
            FROM 
                UsersTelegaBot u
            JOIN 
                call_scores cs 
            ON 
                u.user_id = CAST(cs.caller_info AS SIGNED)
            WHERE 
                u.user_id = %s
            AND 
                cs.call_date BETWEEN %s AND %s
            """
            params = [
            user_id,
            period_start,
            period_end
            ]

            # Выполняем запрос
            start_time = time.time()
            result = await self.db_manager.execute_query(query, params, fetchall=True)
            elapsed_time = time.time() - start_time

            # Логируем результат выполнения запроса
            if result:
                self.logger.info(f"[КРОТ]: Успешно извлечены данные для user_id={user_id} "
                                f"за период {period_start} - {period_end} (время: {elapsed_time:.2f} сек).")
                self.logger.debug(f"[КРОТ]: Полученные данные (пример): {result[:5]}")
            else:
                self.logger.warning(f"[КРОТ]: Данные для user_id={user_id} за период {period_start} - {period_end} отсутствуют.")

            return result or []

        except ValueError as ve:
            self.logger.error(f"[КРОТ]: Ошибка преобразования периода {period}: {ve}")
            return []
        except Exception as e:
            self.logger.error(f"[КРОТ]: Ошибка при запросе данных для user_id={user_id}: {e}")
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

    async def get_operator_call_metrics(
        self,
        connection,
        extension,
        start_date=None,
        end_date=None
    ) -> Dict[str, Any]:
        """
        Получение метрик звонков оператора за определенный период (с датой начала и конца).

        Аргументы:
            - connection: соединение с базой данных.
            - extension: extension оператора.
            - start_date: начальная дата периода.
            - end_date: конечная дата периода.

        Возвращает:
            - Словарь с метриками звонков оператора.
        """
        try:
            # Преобразование дат в корректный формат
            date_filter = []
            if start_date:
                start_date = validate_and_format_date(start_date)
                date_filter.append(start_date.strftime('%Y-%m-%d %H:%M:%S'))
            if end_date:
                end_date = validate_and_format_date(end_date)
                date_filter.append(end_date.strftime('%Y-%m-%d %H:%M:%S'))

            operator_filter = [f"%{extension}%", f"%{extension}%"]

            # Запросы с преобразованием типов данных
            booked_services_query = """
            SELECT COUNT(*) AS booked_services
            FROM call_scores
            WHERE (caller_info LIKE %s OR called_info LIKE %s)
            AND call_category = 'Запись на услугу'
            """
            if start_date and end_date:
                booked_services_query += " AND call_date BETWEEN %s AND %s"
            elif start_date:
                booked_services_query += " AND call_date >= %s"
            elif end_date:
                booked_services_query += " AND call_date <= %s"
            booked_services_params = operator_filter + date_filter

            cancellations_query = """
            SELECT
                SUM(CASE WHEN call_category = 'Отмена записи' THEN 1 ELSE 0 END) AS total_cancellations,
                SUM(CASE WHEN call_category = 'Перенос записи' THEN 1 ELSE 0 END) AS total_reschedules,
                AVG(CASE WHEN call_category = 'Отмена записи' THEN CAST(call_score AS DECIMAL(5,2)) ELSE NULL END) AS avg_cancel_score
            FROM call_scores
            WHERE (caller_info LIKE %s OR called_info LIKE %s)
            AND call_category IN ('Отмена записи', 'Перенос записи')
            """
            if start_date and end_date:
                cancellations_query += " AND call_date BETWEEN %s AND %s"
            elif start_date:
                cancellations_query += " AND call_date >= %s"
            elif end_date:
                cancellations_query += " AND call_date <= %s"
            cancellations_params = operator_filter + date_filter

            # Выполнение запросов
            async with connection.cursor(aiomysql.DictCursor) as cursor:
                # Записаны на услугу
                await cursor.execute(booked_services_query, booked_services_params)
                booked_services = (await cursor.fetchone()).get('booked_services', 0)

                # Отмены и переносы
                await cursor.execute(cancellations_query, cancellations_params)
                cancellations = await cursor.fetchone()
                total_cancellations = cancellations.get('total_cancellations', 0)
                total_reschedules = cancellations.get('total_reschedules', 0)  # Добавлено извлечение total_reschedules
                avg_cancel_score = cancellations.get('avg_cancel_score', 0.0)

            # Расчёт дополнительных метрик
            conversion_rate = (booked_services / total_cancellations * 100) if total_cancellations > 0 else 0.0
            cancellation_rate = (total_cancellations / (total_cancellations + total_reschedules) * 100) if (total_cancellations + total_reschedules) > 0 else 0.0

            # Сбор метрик
            metrics = {
                "booked_services": booked_services,
                "total_cancellations": total_cancellations,
                "total_reschedules": total_reschedules,
                "avg_cancel_score": avg_cancel_score,
                "conversion_rate": conversion_rate,
                "cancellation_rate": cancellation_rate,
            }

            self.logger.info(f"[КРОТ]: Метрики звонков для extension {extension} успешно извлечены.")
            self.logger.debug(f"[КРОТ]: Результат: {metrics}")
            return metrics

        except Exception as e:
            self.logger.error(f"[КРОТ]: Ошибка при извлечении метрик звонков для extension {extension}: {e}")
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
