#opeanai_telebot.py
import datetime
import asyncio
from asyncio import Semaphore
import logging
from logging.handlers import RotatingFileHandler

import traceback
import sys
import time  # Для замера времени
import os
from operator_data import OperatorData
from logger_utils import setup_logging
import openai
import httpx
from openai import AsyncOpenAI, OpenAIError #импорт класса
import config
import aiomysql
from dotenv import load_dotenv
from permissions_manager import PermissionsManager
from typing import Any, List, Dict, Optional, Union, TypedDict, Tuple
from textwrap import wrap
import sentry_sdk
from aiohttp import web
import pdb
from metrics_calculator import MetricsCalculator
from db_utils import execute_async_query
from collections import Counter
import re

# Загрузка переменных окружения
load_dotenv()
#Глобальный логер делаем в bot.py, а тут ссылаемся на логирование
# Получаем логгер для текущего модуля
logger = logging.getLogger(__name__)

# Проверяем, что сообщения из этого модуля логируются
logger.info("Логгер в openai_telebot.py настроен и работает.")
# Настройка Sentry
sentry_sdk.init(
    dsn=os.getenv("SENTRY_DSN"),
    traces_sample_rate=1.0,
    _experiments={"continuous_profiling_auto_start": True},
)

# Настройка логирования
#log_file = "logs.log"
###log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# RotatingFileHandler с ограничением на количество строк
# Ориентировочно, одна строка логов занимает около 100 символов
##max_log_size = 70000 * 100  # 70,000 строк по 100 символов каждая
#backup_count = 5  # Сохраняем до 5 резервных копий

#file_handler = RotatingFileHandler(
# log_file, maxBytes=max_log_size, backupCount=backup_count, encoding='utf-8'
#)
#file_handler.setFormatter(log_formatter)
##file_handler.setLevel(logging.INFO)

# Консольный обработчик для дублирования логов
#console_handler = logging.StreamHandler()
#console_handler.setFormatter(log_formatter)
#console_handler.setLevel(logging.INFO)

# Конфигурация основного логгера
#logger = logging.getLogger(__name__)
#logger.addHandler(file_handler)
#logger.addHandler(console_handler)
#logger.setLevel(logging.INFO)

# Глобальный обработчик исключений
def setup_global_error_handler():
    def global_excepthook(exc_type, exc_value, exc_traceback):
        logger.error("Необработанное исключение", exc_info=(exc_type, exc_value, exc_traceback))
        sentry_sdk.capture_exception(exc_value)
    sys.excepthook = global_excepthook

setup_global_error_handler()

# AIOHTTP-приложение
async def hello(request):
    return web.Response(text="Hello, world")

async def trigger_error(request):
    1 / 0  # Искусственная ошибка

app = web.Application()
app.add_routes([web.get("/", hello), web.get("/error", trigger_error)])
if __name__ == "__main__":
    web.run_app(app)

class OpenAIReportGenerator:
    def __init__(self, db_manager, model="gpt-4o-mini"):
        # Настройка OpenAI API ключа из переменных окружения
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            logger.error("OpenAI API ключ не найден. Пожалуйста, установите переменную окружения OPENAI_API_KEY.")
            raise EnvironmentError("OpenAI API ключ не найден.")
        
        self.client = AsyncOpenAI(api_key=api_key)
        self.db_manager = db_manager
        self.operator_data = OperatorData(db_manager)
        self.model = model  # Устанавливаем модель gpt-4o-mini 
        self.permissions_manager = PermissionsManager(db_manager)

        self.metrics_calculator = MetricsCalculator(
            db_manager=self.db_manager,
            execute_query=execute_async_query,
            logger=logger
        )

    def get_date_range(
        self,
        period: str,
        custom_start: Optional[Union[str, datetime.date, datetime.datetime]] = None,
        custom_end: Optional[Union[str, datetime.date, datetime.datetime]] = None
    ) -> Tuple[datetime.datetime, datetime.datetime]:
        """
        Возвращает начальную и конечную дату для указанного периода в формате datetime.datetime.

        :param period: Период (daily, weekly, monthly, yearly или custom).
        :param custom_start: Начальная дата для произвольного периода (только для периода 'custom').
        :param custom_end: Конечная дата для произвольного периода (только для периода 'custom').
        :return: Кортеж из начальной и конечной даты.
        """
        now = datetime.datetime.now()

        if period == 'daily':
            start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = now.replace(hour=23, minute=59, second=59, microsecond=999999)
        elif period == 'weekly':
            start_date = now - datetime.timedelta(days=now.weekday())
            end_date = now
        elif period == 'monthly':
            start_date = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            end_date = now
        elif period == 'yearly':
            start_date = now - datetime.timedelta(days=365)
            end_date = now
        elif period == 'custom':
            if not custom_start or not custom_end:
                raise ValueError("Для периода 'custom' необходимо указать custom_start и custom_end.")
            start_date = self.validate_and_format_date(custom_start)
            end_date = self.validate_and_format_date(custom_end)
        else:
            raise ValueError("Неподдерживаемый период.")
        
        # Убедимся, что конечная дата не меньше начальной
        if start_date > end_date:
            raise ValueError("Начальная дата не может быть позже конечной даты.")
        
        return start_date, end_date
    
    def validate_date_range(
        self, 
        start_date: Union[str, datetime.date, datetime.datetime], 
        end_date: Union[str, datetime.date, datetime.datetime]
    ) -> Tuple[datetime.datetime, datetime.datetime]:
        """
        Проверяет и форматирует диапазон дат.

        :param start_date: Дата начала периода.
        :param end_date: Дата окончания периода.
        :return: Кортеж из двух объектов datetime.datetime (start_date, end_date).
        """
        start_datetime = self.validate_and_format_date(start_date)
        end_datetime = self.validate_and_format_date(end_date)
        logger.debug(f"validate_date_range: start_datetime={start_datetime}, end_datetime={end_datetime}")
        
        if start_datetime > end_datetime:
            raise ValueError("Начальная дата не может быть позже конечной даты.")

        return start_datetime, end_datetime
        
    def validate_and_format_date(
        self,
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
            
    async def get_user_extension(self, connection, user_id):
        """
        Получение extension оператора по его user_id.
        """
        # Проверяем, что user_id имеет корректный тип
        if not isinstance(user_id, int):
            logger.error(f"[КРОТ]: Некорректный формат user_id: {user_id}. Ожидался тип int.")
            return None

        query = "SELECT extension FROM users WHERE user_id = %s"
        try:
            async with connection.cursor() as cursor:
                # Выполняем запрос
                await cursor.execute(query, (user_id,))
                results = await cursor.fetchall()

            # Проверяем количество записей в результате
            if not results:
                logger.warning(f"[КРОТ]: Не найден extension для user_id {user_id}")
                return None

            if len(results) > 1:
                logger.warning(f"[КРОТ]: Найдено несколько записей для user_id {user_id}. Возвращена первая запись.")

            # Возвращаем extension из первой записи
            extension = results[0].get('extension')
            logger.info(f"[КРОТ]: Получен extension {extension} для user_id {user_id}")
            return extension

        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при получении extension для user_id {user_id}: {e}")
            return None

    async def get_operator_name(self, connection: aiomysql.Connection, extension: str) -> str:
        """
        Получение имени оператора по extension из базы данных.
        """
        query = "SELECT name FROM users WHERE extension = %s"
        try:
            async with connection.cursor() as cursor:
                await cursor.execute(query, (extension,))
                result = await cursor.fetchone()
                if result and 'name' in result:
                    return result['name']
                else:
                    logger.warning(f"[КРОТ]: Имя оператора не найдено для extension {extension}")
                    return 'Неизвестно'
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при получении имени оператора: {e}")
            return 'Неизвестно'
    
    async def get_operator_data(
        self,
        connection: aiomysql.Connection,
        extension: str,
        start_date: Union[str, datetime.date, datetime.datetime],
        end_date: Union[str, datetime.date, datetime.datetime]
    ) -> Optional[Dict[str, List[Dict[str, Any]]]]:
        """
        Получение данных о звонках оператора из базы данных за указанный период.

        Параметры:
            connection: Асинхронное соединение с базой данных.
            extension (str): Extension оператора.
            start_date: Дата начала периода (строка или объект datetime).
            end_date: Дата окончания периода (строка или объект datetime).

        Возвращает:
            Словарь с данными из таблиц call_history и call_scores или None в случае ошибки.
        """
        try:
            # Валидация и преобразование дат
            start_datetime, end_datetime = self.validate_date_range(start_date, end_date)
            end_datetime = end_datetime.replace(hour=23, minute=59, second=59, microsecond=999999)

            # Преобразование дат для call_history (в timestamp)
            start_timestamp = int(start_datetime.timestamp())
            end_timestamp = int(end_datetime.timestamp())

            # Преобразование дат для call_scores (в строковый формат DATETIME)
            start_datetime_str = start_datetime.strftime('%Y-%m-%d %H:%M:%S')
            end_datetime_str = end_datetime.strftime('%Y-%m-%d %H:%M:%S')

            logger.debug(f"start_datetime_str: {start_datetime_str}, type: {type(start_datetime_str)}")
            logger.debug(f"end_datetime_str: {end_datetime_str}, type: {type(end_datetime_str)}")
            logger.debug(f"start_timestamp: {start_timestamp}, type: {type(start_timestamp)}")
            logger.debug(f"end_timestamp: {end_timestamp}, type: {type(end_timestamp)}")

        except (ValueError, TypeError) as e:
            logger.error(f"[КРОТ]: Ошибка валидации дат: {e}")
            return None

        # Формирование SQL-запросов
        call_history_query = """
        SELECT history_id, called_info, context_start_time, talk_duration
        FROM call_history
        WHERE 
            called_info LIKE CONCAT(%s, '%%')
            AND context_start_time BETWEEN %s AND %s
        """

        call_scores_query = """
        SELECT history_id, called_info, call_date, talk_duration, call_category, call_score, result
        FROM call_scores
        WHERE 
            called_info LIKE CONCAT(%s, '%%')
            AND call_date BETWEEN %s AND %s
        """

        try:
            # Параметры для запросов
            params_call_history = (
                extension,
                start_timestamp,  # Используем timestamp для call_history
                end_timestamp     # Используем timestamp для call_history
            )

            params_call_scores = (
                extension,
                start_datetime_str,  # Используем строковый формат для call_scores
                end_datetime_str     # Используем строковый формат для call_scores
            )

            logger.debug(f"Параметры call_history_query: {params_call_history}")
            logger.debug(f"Параметры call_scores_query: {params_call_scores}")

            # Выполнение запросов
            call_history_data = await execute_async_query(connection, call_history_query, params_call_history)
            call_scores_data = await execute_async_query(connection, call_scores_query, params_call_scores)

            # Преобразование результатов в списки
            call_history_data = list(call_history_data or [])
            call_scores_data = list(call_scores_data or [])

            # Логируем данные
            logger.info(f"[КРОТ]: Найдено {len(call_history_data)} записей в call_history")
            logger.info(f"[КРОТ]: Найдено {len(call_scores_data)} записей в call_scores")

            # Логирование history_id
            if call_history_data:
                history_ids = [row['history_id'] for row in call_history_data if 'history_id' in row]
                logger.info(f"[КРОТ]: Найдены history_id из call_history: {history_ids}")
            if call_scores_data:
                scores_ids = [row['history_id'] for row in call_scores_data if 'history_id' in row]
                logger.info(f"[КРОТ]: Найдены history_id из call_scores: {scores_ids}")

        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при выполнении запросов в get_operator_data: {e}")
            return None

        if not call_history_data and not call_scores_data:
            logger.warning(f"[КРОТ]: Данные о звонках не найдены для оператора {extension} в период {start_datetime_str} - {end_datetime_str}")
            return None

        logger.info(f"[КРОТ]: Получено {len(call_history_data)} записей из call_history и {len(call_scores_data)} записей из call_scores для оператора {extension}")
        
        # Сортируем по history_id и статусу
        history_ids_from_scores = {row['history_id'] for row in call_scores_data}
        accepted_calls = [row for row in call_history_data if row['history_id'] in history_ids_from_scores]
        missed_calls = [row for row in call_history_data if row['history_id'] not in history_ids_from_scores]

        logger.info(f"[КРОТ]: Принятых звонков: {len(accepted_calls)}. Пропущенных звонков: {len(missed_calls)}.")

        # Возвращаем данные в виде словаря
        return {
            'call_history': call_history_data,
            'call_scores': call_scores_data,
            'accepted_calls': accepted_calls,
            'missed_calls': missed_calls
        }
    
    async def generate_report(self, connection, user_id, period='daily', date_range=None):
        """
        Генерация отчета для оператора по его user_id с использованием данных и OpenAI.
        Параметры:
            - user_id: ID пользователя, для которого нужно сгенерировать отчет.
            - period: Период отчета (daily, weekly, monthly, biweekly, half-year, yearly, custom).
            - extension: extension оператора, для которого нужно сгенерировать отчет.
            - date_range: Кастомный диапазон дат в формате "DD/MM/YYYY-DD/MM/YYYY".
        """
        logger.info(f"[КРОТ]: Начата генерация отчета для оператора с extension {user_id} за период {period}.")
        try:
            start_time = time.time()
            # Определение диапазона дат
            if period == 'custom' and date_range:
                # Обработка пользовательского диапазона дат
                try:
                    if isinstance(date_range, tuple):
                        # Если date_range уже кортеж
                        custom_start, custom_end = date_range
                    else:
                        # Если date_range строка
                        custom_start, custom_end = map(
                            lambda x: datetime.datetime.strptime(x.strip(), '%d/%m/%Y'),
                            date_range.split('-')
                        )
                except ValueError:
                    logger.error("[КРОТ]: Некорректный формат дат для кастомного периода.")
                    return "Ошибка: Некорректный формат дат. Ожидается 'DD/MM/YYYY-DD/MM/YYYY'."

                # Валидация и настройка диапазона через get_date_range
                start_date, end_date = self.get_date_range('custom', custom_start, custom_end)
            else:
                # Используем стандартную обработку периода
                start_date, end_date = self.get_date_range(period)

            # Используем стандартную обработку периода
            logger.info(f"[КРОТ]: Определен диапазон дат: {start_date} - {end_date}")

            # Получаем extension пользователя по его user_id
            extension = await self.get_user_extension(connection, user_id)
            if not extension:
                logger.error(f"[КРОТ]: Не удалось получить extension для пользователя с user_id {user_id}.")
                return "Ошибка: Не удалось получить данные пользователя."
            # Получаем имя оператора
            operator_name = await self.get_operator_name(connection, extension)
            logger.info(f"[КРОТ]: Имя оператора: {operator_name}")
            # Получение данных оператора из базы данных за указанный период
            operator_data = await self.get_operator_data(connection, extension, start_date, end_date)
            if operator_data is None:
                return "Ошибка при извлечении данных оператора или данных нет."
            if not operator_data:
                logger.warning(f"[КРОТ]: Нет данных по оператору с extension {extension} за период {start_date} - {end_date}")
                return f"Данные по оператору {operator_name} (extension {extension}) за период {start_date} - {end_date} не найдены."
            logger.info(f"[КРОТ]: Получено {len(operator_data)} записей для оператора с extension {extension}")
            accepted_calls = operator_data.get('accepted_calls', [])
            missed_calls = operator_data.get('missed_calls', [])
            logger.info(f"[КРОТ]: Перед вызовом calculate_operator_metrics: accepted_calls={len(accepted_calls)}, missed_calls={len(missed_calls)}")

            call_history_data = list(operator_data.get('call_history', []))
            call_scores_data = list(operator_data.get('call_scores', []))
            combined_call_data = call_history_data + call_scores_data
            # Расчет метрик оператора
            operator_metrics = await self.metrics_calculator.calculate_operator_metrics(
            call_history_data=call_history_data,
            call_scores_data=call_scores_data,
            extension=extension,
            start_date=start_date,
            end_date=end_date
            )
            # Создаём словарь звонков из call_history_data по history_id
            call_history_dict = {call['history_id']: call for call in call_history_data}

            # Обновляем словарь данными из call_scores_data
            for score in call_scores_data:
                history_id = score['history_id']
                if history_id in call_history_dict:
                    call_history_dict[history_id].update(score)
                else:
                    call_history_dict[history_id] = score  # Если звонка нет в call_history_data, добавляем его

            # Получаем список объединённых данных без дубликатов
            combined_call_data = list(call_history_dict.values())
            
            # Используем метрики для генерации отчёта
            accepted_calls = operator_metrics.get('accepted_calls')
            missed_calls = operator_metrics.get('missed_calls')

            if accepted_calls is None or missed_calls is None:
                self.logger.error("Ошибка: Отсутствуют метрики accepted_calls, missed_calls.")
                return

            ## Обновляем список обязательных метрик
            required_metrics = [
            'total_calls', 'accepted_calls', 'missed_calls', 'booked_services',
            'conversion_rate_leads', 'avg_call_rating', 'avg_lead_call_rating',
            'total_cancellations', 'avg_cancel_score', 'cancellation_rate',
            'complaint_calls', 'complaint_rating', 'avg_conversation_time',
            'avg_navigation_time', 'avg_service_time'
            ]
            missing_metrics = [metric for metric in required_metrics if metric not in operator_metrics]
            if missing_metrics:
                return f"Ошибка: Отсутствуют метрики {', '.join(missing_metrics)}."

            # Генерация рекомендаций
            logger.info(f"[КРОТ]: Начало генерации рекомендаций для оператора {operator_name} (extension {extension}).")
            recommendations_text = await self.generate_combined_recommendations (
                operator_metrics, combined_call_data, user_id, operator_name
            )
            if not recommendations_text or "Ошибка" in recommendations_text:
                logger.error(f"[КРОТ]: Ошибка при генерации рекомендаций: {recommendations_text}")
                return "Ошибка: Не удалось получить рекомендации."

            logger.info(f"[КРОТ]: Рекомендации успешно сгенерированы: {recommendations_text[:100]}...")  # Логируем начало текста рекомендаций

            # Формирование отчета
            report_date = f"{start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}"
            report = self.create_report(operator_metrics, recommendations_text, report_date, operator_name)
            logger.info(f"[КРОТ]: Отчет успешно сформирован для оператора {operator_name} (extension {extension}).")

            # Сохранение отчета в базу данных
            logger.info(f"[КРОТ]: Сохранение отчета в базу данных для оператора {operator_name} (extension {extension}).")
            await self.save_report_to_db(
                connection=connection,
                user_id=user_id,
                name=operator_name,
                report_text=report,
                period=period,
                start_date=start_date,
                end_date=end_date,
                operator_metrics=operator_metrics,
                recommendations=recommendations_text
            )
            logger.info(f"[КРОТ]: Отчет для оператора {operator_name} (extension {extension}) успешно сохранен.")

            elapsed_time = time.time() - start_time
            logger.info(f"[КРОТ]: Отчет успешно сгенерирован для оператора {operator_name} (extension {extension}) за {elapsed_time:.2f} секунд.")
            return report

        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при генерации отчета для оператора {user_id}: {e}")
            return f"Ошибка при генерации отчета: {e}"

    

    def validate_metrics(self, operator_metrics):
        """
        Проверка на наличие обязательных полей в данных оператора.
        """
        required_fields = [
            'extension', 'empathy_score', 'understanding_score', 'response_quality_score',
            'problem_solving_score', 'call_closing_score', 'total_call_score',
            'conversion_rate_leads', 'avg_complaint_time', 'avg_service_time',
            'avg_navigation_time', 'total_calls', 'accepted_calls', 'total_talk_time'
        ]
        missing_fields = [field for field in required_fields if field not in operator_metrics]
        if missing_fields:
            logger.error(f"[КРОТ]: Отсутствуют обязательные поля в метриках оператора: {', '.join(missing_fields)}.")
            return False
        return True
    
    async def generate_combined_recommendations(self, operator_metrics, operator_data, user_id, name, max_length=1500, max_retries=3, batch_size=5000):
        """
        Генерация рекомендаций для оператора на основе его метрик и данных.
        """
        try:
            logger.info("[РЕКОМЕНДАЦИИ]: Начало генерации рекомендаций для оператора.")
            logger.debug(f"[РЕКОМЕНДАЦИИ]: Метрики оператора: {operator_metrics}")
            logger.debug(f"[РЕКОМЕНДАЦИИ]: Данные звонков оператора (количество записей): {len(operator_data)}")

            # Определяем даты из данных оператора
            dates = []
            for call in operator_data:
                call_date_value = call.get('call_date') or call.get('context_start_time')
                if call_date_value:
                    try:
                        if isinstance(call_date_value, datetime.datetime):
                            call_date = call_date_value
                        elif isinstance(call_date_value, datetime.date):
                            call_date = datetime.datetime.combine(call_date_value, datetime.time.min)
                        elif isinstance(call_date_value, str):
                            call_date = datetime.datetime.strptime(call_date_value, '%Y-%m-%d %H:%M:%S')
                        else:
                            continue  # Пропускаем записи с неверным типом даты
                        dates.append(call_date)
                    except ValueError as e:
                        logger.warning(f"[РЕКОМЕНДАЦИИ]: Пропущена запись с некорректной датой: {call_date_value}. Ошибка: {e}")
            start_date, end_date = ("неизвестно", "неизвестно") if not dates else (min(dates).strftime('%Y-%m-%d'), max(dates).strftime('%Y-%m-%d'))

            # Проверка обязательных метрик
            required_metrics = [
                'total_calls', 'accepted_calls', 'missed_calls', 'booked_services',
                'conversion_rate_leads', 'avg_call_rating', 'avg_lead_call_rating',
                'total_cancellations', 'avg_cancel_score', 'cancellation_rate',
                'complaint_calls', 'complaint_rating', 'avg_conversation_time',
                'avg_navigation_time', 'avg_service_time'
            ]
            missing_metrics = [metric for metric in required_metrics if metric not in operator_metrics]
            if missing_metrics:
                logger.error(f"[РЕКОМЕНДАЦИИ]: Отсутствуют обязательные метрики: {', '.join(missing_metrics)}")
                return f"Ошибка: отсутствуют метрики {', '.join(missing_metrics)}"

            # Подготовка данных из 'result'
            results = [call.get('result') for call in operator_data if call.get('result')]
            result_text = '\n'.join(results)[:10000] if results else ""
            if not result_text:
                logger.warning("[РЕКОМЕНДАЦИИ]: Нет данных в поле 'result' для генерации.")
                return "Нет данных для анализа."

            # Разбиение данных на пакеты
            batches = self.split_into_batches(result_text, batch_size)
            logger.info(f"[РЕКОМЕНДАЦИИ]: Данные разбиты на {len(batches)} пакетов для обработки.")

            # Формирование промпта
            coaching_prompt = f"""
            Данные звонков:
            {result_text}
            
            ### Рекомендации:
            На основе приведенных данных предоставь краткие персонализированные рекомендации для оператора {name}, проанализировав все данные звонков, осознавая, что рекомендации это среднее и ты оцениваешь много данных и даешь краткую выжимку. Укажи:

            - Сильные и слабые стороны оператора.
            - Аспекты, которые можно улучшить.
            - Конкретные шаги для повышения эффективности работы.
            """
            logger.debug("[РЕКОМЕНДАЦИИ]: Промпт сформирован.")

            # Разбиение промпта на части
            sub_requests = self.split_into_batches(coaching_prompt, max_length)
            logger.debug(f"[РЕКОМЕНДАЦИИ]: Промпт разбит на {len(sub_requests)} блоков.")

            # Обработка запросов
            partial_recommendations = await self.process_requests(sub_requests, max_retries, max_length)
            if partial_recommendations.startswith("Ошибка"):
                logger.error(f"[РЕКОМЕНДАЦИИ]: Ошибка при обработке рекомендаций: {partial_recommendations}")
                return "Ошибка при обработке рекомендаций."

            # Объединение промежуточных рекомендаций
            combined_recommendations = partial_recommendations  # Уже строка
            logger.info("[РЕКОМЕНДАЦИИ]: Промежуточные рекомендации объединены.")

            # Финальный запрос для обобщения
            final_prompt = f"""
            На основе всех рекомендаций ниже, предоставь краткий и связный итоговый вывод для оператора {name}:
            {combined_recommendations}
            """
            logger.info("[РЕКОМЕНДАЦИИ]: Финальный запрос для обобщения подготовлен.")

            # Отправка финального запроса
            final_recommendation = await self.process_requests([final_prompt], max_retries, max_length)
            if final_recommendation.startswith("Ошибка"):
                logger.error(f"[РЕКОМЕНДАЦИИ]: Ошибка при обработке финальной рекомендации: {final_recommendation}")
                return "Ошибка при обработке финальной рекомендации."

            logger.info("[РЕКОМЕНДАЦИИ]: Генерация финальной рекомендации завершена успешно.")
            return final_recommendation  # Возвращаем финальный объединённый ответ

        except Exception as e:
            logger.error(f"[РЕКОМЕНДАЦИИ]: Ошибка при генерации рекомендаций: {e}", exc_info=True)
            return f"Ошибка при генерации рекомендаций: {e}"
    def split_into_batches(self, text, max_length):
        """
        Разделяет текст на части, не превышающие max_length символов.
        """
        return [text[i:i+max_length] for i in range(0, len(text), max_length)]

    async def send_request(self, sub_request, semaphore, batch_index, max_retries, max_length):
        """
        Отправка запроса к OpenAI с поддержкой повторных попыток.
        """
        async with semaphore:
            logger.info(f"[РЕКОМЕНДАЦИИ]: Отправка пакета {batch_index + 1}: {sub_request[:500]}...")
            for attempt in range(max_retries):
                try:
                    response = await self.client.chat.completions.create(
                        model=self.model,
                        messages=[{"role": "user", "content": sub_request}],
                        max_tokens=max_length,
                    )
                    if not response.choices or not response.choices[0].message or not response.choices[0].message.content:
                        raise ValueError(f"Пустой ответ от OpenAI для пакета {batch_index + 1}")
                    result = response.choices[0].message.content.strip()
                    logger.debug(f"[РЕКОМЕНДАЦИИ]: Ответ OpenAI для пакета {batch_index + 1}: {result[:500]}")
                    return result  # Успех: выходим из цикла
                except Exception as e:
                    logger.error(
                        f"[РЕКОМЕНДАЦИИ]: Ошибка при обработке пакета {batch_index + 1}: {e}. "
                        f"Попытка {attempt + 1}/{max_retries}"
                    )
                    await asyncio.sleep(2 ** attempt)  # Экспоненциальная задержка
            # Если все попытки неудачны, возвращаем сообщение об ошибке
            logger.error(f"[РЕКОМЕНДАЦИИ]: Не удалось обработать пакет {batch_index + 1} после {max_retries} попыток.")
            return f"Ошибка: Не удалось получить рекомендации для пакета {batch_index + 1}."
    async def process_requests(self, sub_requests, max_retries, max_length):
        """
        Обработка запросов к OpenAI API с использованием семафора.
        """
        if not sub_requests:
            logger.error("[РЕКОМЕНДАЦИИ]: Нет подзапросов для обработки.")
            return "Ошибка: Нет данных для обработки."

        semaphore = Semaphore(5)  # Ограничение параллелизма
        tasks = [
            self.send_request(req, semaphore, idx, max_retries, max_length)
            for idx, req in enumerate(sub_requests)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Фильтруем успешные результаты
        successful_results = [result for result in results if not isinstance(result, Exception)]
        if not successful_results:
            logger.error("[РЕКОМЕНДАЦИИ]: Все запросы завершились с ошибками.")
            return ["Ошибка: Все запросы завершились с ошибками."]
        
        final_results = "\n".join(successful_results)
        logger.debug(f"[РЕКОМЕНДАЦИИ]: Итоговые объединённые рекомендации: {final_results[:500]}")
        return final_results
    
    async def request_with_retries(self, text_packet, max_retries=3, max_tokens=2500):
        """
        Запрос к ChatGPT с поддержкой динамической разбивки `text_packet` на подзапросы,
        поддержкой повторных попыток и лимитом по токенам.
        """
        logger.info("[РЕКОМЕНДАЦИИ]: Инициализация процесса отправки запросов с динамической подстройкой и обработкой ошибок.")
        
        # Разбиваем text_packet на подзапросы, чтобы каждый не превышал max_tokens
        sub_requests = self.split_text_into_chunks(text_packet, max_length=max_tokens)
        logger.debug(f"[РЕКОМЕНДАЦИИ]: Текст разбит на {len(sub_requests)} блока(ов) для отправки. Размер блоков: max_tokens={max_tokens}")
        
        full_recommendations = []  # Список для всех рекомендаций

        for i, sub_request in enumerate(sub_requests):
            prompt = f"На основе данных звонков и метрик: {sub_request}\nПредоставьте рекомендации по улучшению работы оператора."
            logger.info(f"[РЕКОМЕНДАЦИИ]: Подготовка отправки для блока {i + 1}/{len(sub_requests)}. Длина блока: {len(sub_request)} символов.")
            
            for attempt in range(max_retries):
                try:
                    logger.debug(f"[РЕКОМЕНДАЦИИ]: Попытка {attempt + 1} для блока {i + 1}. Запрос отправляется к API ChatGPT.")
                    
                    # Отправка запроса к API с учетом лимита max_tokens
                    response = await self.client.chat.completions.create(
                        model=self.model,
                        messages=[{"role": "user", "content": prompt}],
                        max_tokens=max_tokens,
                        temperature=0.7,
                    )
                    recommendation = response.choices[0].message.content.strip()
                    full_recommendations.append(recommendation)
                    
                    logger.info(f"[РЕКОМЕНДАЦИИ]: Рекомендация успешно получена с попытки {attempt + 1} для блока {i + 1}.")
                    logger.debug(f"[РЕКОМЕНДАЦИИ]: Ответ от ChatGPT для блока {i + 1} (первые 500 символов): {recommendation[:100]}...")
                    break  # Успешно обработанный блок, выходим из цикла повторов
                    
                except OpenAIError as e:
                    logger.warning(f"[РЕКОМЕНДАЦИИ]: [Попытка {attempt + 1} для блока {i + 1}] OpenAIError: {e}. Задержка перед повтором.")
                    await asyncio.sleep(2 ** attempt)  # Экспоненциальная задержка
                    
                except Exception as e:
                    logger.error(f"[РЕКОМЕНДАЦИИ]: [Попытка {attempt + 1} для блока {i + 1}] Непредвиденная ошибка: {e}. Повтор через задержку.")
                    await asyncio.sleep(2 ** attempt)

            else:
                # Если все попытки завершились неудачей, фиксируем отсутствие результата для блока
                logger.error(f"[РЕКОМЕНДАЦИИ]: Не удалось получить рекомендацию для блока {i + 1} после всех {max_retries} попыток.")
                full_recommendations.append(f"Не удалось сгенерировать рекомендации для блока {i + 1}.")

        # Объединяем все рекомендации в один итоговый текст
        combined_recommendations = "\n".join(full_recommendations)
        logger.info("[РЕКОМЕНДАЦИИ]: Генерация рекомендаций завершена. Все блоки обработаны.")
        logger.debug(f"[РЕКОМЕНДАЦИИ]: Итоговые рекомендации (первые 1000 символов): {combined_recommendations[:100]}...")
        
        return combined_recommendations

    def split_text_into_chunks(self, text, max_length=300):
        """
        Разделяет текст на блоки по длине с учетом лимита max_length (в символах).
        """
        logger.debug(f"[РЕКОМЕНДАЦИИ]: Начало разделения текста на блоки с лимитом {max_length} символов.")
        sentences = text.split('. ')
        chunks = []
        current_chunk = ""

        for sentence in sentences:
            if len(current_chunk) + len(sentence) + 1 <= max_length:
                current_chunk += sentence + ". "
            else:
                chunks.append(current_chunk.strip())
                logger.debug(f"[РЕКОМЕНДАЦИИ]: Добавлен блок размером {len(current_chunk.strip())} символов.")
                current_chunk = sentence + ". "

        if current_chunk:
            chunks.append(current_chunk.strip())
            logger.debug(f"[РЕКОМЕНДАЦИИ]: Добавлен последний блок размером {len(current_chunk.strip())} символов.")

        logger.info(f"[РЕКОМЕНДАЦИИ]: Разделение завершено. Получено {len(chunks)} блок(ов).")
        return chunks


        
    def create_report(
        self,
        operator_metrics: Dict[str, Any],
        recommendations: str,
        report_date: str,
        name: str
    ) -> str:
        """
        Форматирование отчета на основе метрик оператора и рекомендаций.
        Параметры:
            operator_metrics (Dict[str, Any]): Метрики оператора.
            recommendations (str): Рекомендации для оператора.
            report_date (str): Дата отчета.
            name (str): Имя оператора.

        Возвращает:
            str: Сформированный отчет.
        """
        # Проверка обязательных метрик
        ## Обновляем список обязательных метрик
        required_metrics = [
        'total_calls', 'accepted_calls', 'missed_calls', 'booked_services',
        'conversion_rate_leads', 'avg_call_rating', 'avg_lead_call_rating',
        'total_cancellations', 'avg_cancel_score', 'cancellation_rate',
        'complaint_calls', 'complaint_rating', 'avg_conversation_time',
        'avg_navigation_time', 'avg_service_time', 'extension'
        ]
        
        missing_metrics = [metric for metric in required_metrics if metric not in operator_metrics]
        if missing_metrics:
            raise ValueError(f"Метрики отсутствуют: {', '.join(missing_metrics)}")

        # Получаем значения метрик с проверкой на отсутствие
        def get_metric(metric_name, default=0):
            return operator_metrics.get(metric_name, default)

        def format_metric(metric_name, format_spec=".2f", default="Нет данных"):
            value = operator_metrics.get(metric_name)
            if isinstance(value, (int, float)):
                return f"{value:{format_spec}}"
            return default

        # Формируем основную часть отчета
        report = f"""
    📊 Отчет за период: {report_date}
    Оператор {name} с extension {operator_metrics['extension']} выполнил следующие действия во время звонков:
    1. Общая статистика по звонкам:
        - Принято звонков: {get_metric('accepted_calls', 'Нет данных')}
        - Всего звонков: {get_metric('total_calls', 'Нет данных')}
        - Пропущено звонков: {get_metric('missed_calls', 'Нет данных')}
        - Записаны на услугу: {get_metric('booked_services', 'Нет данных')}
        - Конверсия в запись от желающих записаться: {format_metric('conversion_rate_leads')}%
    """

        # Качество обработки звонков
        report += f"""
    2. Качество обработки звонков:
        - Средняя оценка всех разговоров: {format_metric('avg_call_rating')}
        - Средняя оценка разговоров для желающих записаться: {format_metric('avg_lead_call_rating')}
    """

        # Анализ отмен
        report += f"""
    3. Анализ отмен:
        - Всего отмен: {get_metric('total_cancellations', 'Нет данных')}
        - Оценка качества обработки заявок на отмену: {format_metric('avg_cancel_score')}
        - Доля отмен от числа позвонивших отменить запись: {format_metric('cancellation_rate')}%
    """

        # Время обработки звонков
        report += f"""
    4. Время обработки звонков:
        - Среднее время разговора по всем принятым звонкам: {format_metric('avg_conversation_time')} секунд
        - Среднее время разговора по Навигации: {format_metric('avg_navigation_time')} секунд
        - Среднее время по Запись на услугу: {format_metric('avg_service_time')} секунд
    """

        # Динамическое добавление метрик по категориям
        category_keys = {
            'avg_time_spam': 'Среднее время разговора со спамом',
            'avg_time_reminder': 'Среднее время по напоминаниям о приемах',
            'avg_time_cancellation': 'Среднее время по отменам записей',
            'avg_time_complaints': 'Среднее время по звонкам с жалобами',
            'avg_time_reservations': 'Среднее время по резерву',
            'avg_time_reschedule': 'Среднее время по переносу записей'
        }

        for key, description in category_keys.items():
            if key in operator_metrics:
                report += f"    - {description}: {format_metric(key)} секунд\n"

        # Работа с жалобами
        report += f"""
    5. Работа с жалобами:
        - Звонки с жалобами: {get_metric('complaint_calls', 'Нет данных')}
        - Оценка жалоб: {format_metric('complaint_rating')}
    """

        # Добавляем рекомендации
        if recommendations :
            report += f"\nРекомендации:\n{recommendations[:3000]}..."  # Ограничение длины
        # Логируем успешное создание отчета
        logger.info(f"[КРОТ]: Отчет успешно отформатирован для оператора {name} с extension {get_metric('extension')}.")
        return report
    
    def aggregate_metrics(self, all_metrics):
        """
        Агрегирует метрики всех операторов.
        :param all_metrics: список метрик каждого оператора.
        :return: словарь с суммарными метриками.
        """
        # Инициализируем словарь для суммарных метрик
        summary = {
        'total_calls': 0,
        'accepted_calls': 0,
        'missed_calls': 0,
        'booked_services': 0,
        'total_cancellations': 0,
        'complaint_calls': 0,
        'total_conversation_time': 0.0,
        'avg_call_rating_list': [],
        'avg_lead_call_rating_list': [],
        'avg_cancel_score_list': [],
        'cancellation_rate_list': [],
        'avg_conversation_time_list': [],
        'avg_navigation_time_list': [],
        'avg_service_time_list': [],
        'conversion_rate_leads_list': [],
        'complaint_rating_list': []
        }

        for metrics in all_metrics:
            summary['total_calls'] += metrics.get('total_calls', 0)
            summary['accepted_calls'] += metrics.get('accepted_calls', 0)
            summary['missed_calls'] += metrics.get('missed_calls', 0)
            summary['booked_services'] += metrics.get('booked_services', 0)
            summary['total_cancellations'] += metrics.get('total_cancellations', 0)
            summary['complaint_calls'] += metrics.get('complaint_calls', 0)
            summary['total_conversation_time'] += metrics.get('total_conversation_time', 0.0)
            
            # Сбор оценок для вычисления среднего
            avg_call_rating = metrics.get('avg_call_rating')
            if avg_call_rating is not None:
                summary['avg_call_rating_list'].append(avg_call_rating)
            
            avg_lead_call_rating = metrics.get('avg_lead_call_rating')
            if avg_lead_call_rating is not None:
                summary['avg_lead_call_rating_list'].append(avg_lead_call_rating)
            
            avg_cancel_score = metrics.get('avg_cancel_score')
            if avg_cancel_score is not None:
                summary['avg_cancel_score_list'].append(avg_cancel_score)
            
            cancellation_rate = metrics.get('cancellation_rate')
            if cancellation_rate is not None:
                summary['cancellation_rate_list'].append(cancellation_rate)
            
            avg_conversation_time = metrics.get('avg_conversation_time')
            if avg_conversation_time is not None:
                summary['avg_conversation_time_list'].append(avg_conversation_time)
            
            avg_navigation_time = metrics.get('avg_navigation_time')
            if avg_navigation_time is not None:
                summary['avg_navigation_time_list'].append(avg_navigation_time)
            
            avg_service_time = metrics.get('avg_service_time')
            if avg_service_time is not None:
                summary['avg_service_time_list'].append(avg_service_time)
            
            conversion_rate_leads = metrics.get('conversion_rate_leads')
            if conversion_rate_leads is not None:
                summary['conversion_rate_leads_list'].append(conversion_rate_leads)
            
            complaint_rating = metrics.get('complaint_rating')
            if complaint_rating is not None:
                summary['complaint_rating_list'].append(complaint_rating)
        
        # Рассчитываем средние значения
        def calculate_average(value_list):
            return sum(value_list) / len(value_list) if value_list else 0.0

        summary['avg_call_rating'] = calculate_average(summary['avg_call_rating_list'])
        summary['avg_lead_call_rating'] = calculate_average(summary['avg_lead_call_rating_list'])
        summary['avg_cancel_score'] = calculate_average(summary['avg_cancel_score_list'])
        summary['cancellation_rate'] = calculate_average(summary['cancellation_rate_list'])
        summary['avg_conversation_time'] = calculate_average(summary['avg_conversation_time_list'])
        summary['avg_navigation_time'] = calculate_average(summary['avg_navigation_time_list'])
        summary['avg_service_time'] = calculate_average(summary['avg_service_time_list'])
        summary['conversion_rate_leads'] = calculate_average(summary['conversion_rate_leads_list'])
        summary['complaint_rating'] = calculate_average(summary['complaint_rating_list'])
        
        # Удаляем временные списки
        del summary['avg_call_rating_list']
        del summary['avg_lead_call_rating_list']
        del summary['avg_cancel_score_list']
        del summary['cancellation_rate_list']
        del summary['avg_conversation_time_list']
        del summary['avg_navigation_time_list']
        del summary['avg_service_time_list']
        del summary['conversion_rate_leads_list']
        del summary['complaint_rating_list']

        return summary

    async def generate_summary_report(self, connection, start_date, end_date):
        """
        Генерирует сводный отчёт по всем операторам за указанный период.
        """
        logger.info("[КРОТ]: Начата генерация сводного отчёта по всем операторам.")

        try:
            # Получаем список всех операторов
            operators_query = "SELECT user_id, name, extension FROM users WHERE extension IS NOT NULL"
            async with connection.cursor() as cursor:
                await cursor.execute(operators_query)
                operators = await cursor.fetchall()

            if not operators:
                logger.warning("[КРОТ]: Не найдено ни одного оператора с указанным extension.")
                return "Не найдено ни одного оператора для формирования сводного отчёта."

            all_metrics = []

            # Собираем метрики для каждого оператора
            for operator in operators:
                user_id = operator['user_id']
                name = operator['name']
                extension = operator['extension']

                # Получаем данные оператора
                operator_data = await self.get_operator_data(connection, extension, start_date, end_date)
                if not operator_data:
                    logger.warning(f"[КРОТ]: Данные не найдены для оператора {name} (extension {extension}).")
                    continue

                call_history_data = operator_data.get('call_history', [])
                call_scores_data = operator_data.get('call_scores', [])

                # Инициализируем список для объединённых данных звонков
                all_call_scores_data = []
                for operator in operators:

                # Расчитываем метрики оператора
                    operator_metrics = await self.metrics_calculator.calculate_operator_metrics(
                    call_history_data=call_history_data,
                    call_scores_data=call_scores_data,
                    extension=extension,
                    start_date=start_date,
                    end_date=end_date
                )

                if operator_metrics:
                    operator_metrics['name'] = name
                    all_metrics.append(operator_metrics)
                    # Добавляем данные звонков в общий список
                    all_call_scores_data.extend(call_scores_data)
                else:
                    logger.warning(f"[КРОТ]: Метрики не найдены для оператора {name} (extension {extension}).")

            if not all_metrics:
                return "Не удалось собрать метрики ни для одного оператора."

            # Агрегация метрик
            summary_metrics = self.aggregate_metrics(all_metrics)

            # Формирование отчёта
            report = self.create_summary_report(summary_metrics, start_date, end_date)
            logger.info("[КРОТ]: Сводный отчёт успешно сформирован.")

            # Сохранение отчёта в базу данных
            await self.save_report_to_db(
                connection=connection,
                user_id=None,  # Для сводного отчёта используем None или специальный ID
                name='Сводный отчёт',
                report_text=report,
                period='custom',  # Или используйте соответствующий период
                start_date=start_date,
                end_date=end_date,
                operator_metrics=summary_metrics,
                recommendations=''  # Пустая строка, если рекомендации отсутствуют
            )

            return report

        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при генерации сводного отчёта: {e}")
            return f"Ошибка при генерации сводного отчёта: {e}"
        
    def create_summary_report(self, summary_metrics, start_date, end_date):
        """
        Формирует текст сводного отчёта.
        :param summary_metrics: словарь с суммарными метриками.
        :return: текст отчёта.
        """
        report_date = f"{start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}"
        report = f"""
        📊 **Сводный отчёт за период {report_date}**

        1. **Общая статистика по всем операторам:**
        - Всего звонков: {summary_metrics['total_calls']}
        - Принято звонков: {summary_metrics['accepted_calls']}
        - Пропущено звонков: {summary_metrics['missed_calls']}
        - Записаны на услугу: {summary_metrics['booked_services']}
        - Всего отмен: {summary_metrics['total_cancellations']}
        - Жалобы: {summary_metrics['complaint_calls']}

        2. **Качество обслуживания:**
        - Средняя оценка разговоров: {summary_metrics['avg_call_rating']:.2f}
        - Средняя оценка разговоров для желающих записаться: {summary_metrics['avg_lead_call_rating']:.2f}
        - Средняя оценка звонков по отмене: {summary_metrics['avg_cancel_score']:.2f}
        - Доля отмен: {summary_metrics['cancellation_rate']:.2f}%
        - Общее время разговоров: {summary_metrics['total_conversation_time']:.2f} секунд    
        """
        # Удаляем лишние пустые строки
        report = '\n'.join([line.strip() for line in report.strip().split('\n') if line.strip()])
        return report

        
    ##*Тут все запросы в таблицу report. Метод отвечает за сохранение данных в таблицу. Метод сохранения данных в таблицу reports*
    ## *Метод сохранения данных в таблицу reports*
    async def save_report_to_db(
        self,
        connection: Any,  # Используйте aiomysql.Connection, если импорт доступен
        user_id: int,
        name: str,
        report_text: str,
        period: str,
        start_date: Union[str, datetime.datetime],
        end_date: Union[str, datetime.datetime],
        operator_metrics: Dict[str, Any],
        recommendations: str = ''
    ) -> str:
        """
        Сохранение отчета в таблицу reports.
        """
        # Преобразование дат в datetime.datetime
        try:
            start_datetime = self.validate_and_format_date(start_date)
            end_datetime = self.validate_and_format_date(end_date)
            report_date = (
                f"{start_datetime.strftime('%Y-%m-%d')} - {end_datetime.strftime('%Y-%m-%d')}" 
                if period != 'daily' 
                else start_datetime.strftime('%Y-%m-%d')
            )
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при преобразовании дат: {e}")
            return "Ошибка: Некорректные даты."

        logger.info(f"[КРОТ]: Начало сохранения отчета для пользователя {user_id} ({name}). Период: {period}, Дата: {report_date}")

        # Приведение user_id к целому числу
        if user_id is not None:
            try:
                user_id = int(user_id)
            except ValueError as e:
                logger.error(f"[КРОТ]: Ошибка приведения user_id к целому числу: {e}")
                return "Ошибка: user_id должен быть целым числом."
            
        else:
            user_id = -1  # Используем -1 или другое специальное значение для сводных отчётов

        # Проверка обязательных параметров
        if not report_text:
            logger.error(f"[КРОТ]: report_text пуст для user_id {user_id}.")
            return "Ошибка: Отчет отсутствует."

        # Список обязательных метрик
        required_metrics = [
            'total_calls', 'accepted_calls', 'missed_calls', 'booked_services',
            'conversion_rate_leads', 'avg_call_rating', 'avg_lead_call_rating',
            'total_cancellations', 'avg_cancel_score', 'cancellation_rate',
            'complaint_calls', 'complaint_rating', 'avg_conversation_time',
            'avg_navigation_time', 'avg_service_time'
        ]

        # Проверка наличия всех обязательных метрик
        missing_metrics = [metric for metric in required_metrics if metric not in operator_metrics]
        if missing_metrics:
            logger.error(f"[КРОТ]: Отсутствуют обязательные метрики: {', '.join(missing_metrics)}.")
            return f"Ошибка: Отсутствуют метрики: {', '.join(missing_metrics)}."

        # Утилитарные функции для безопасного приведения типов
        def safe_float(value):
            try:
                return float(value)
            except (ValueError, TypeError):
                return 0.0

        def safe_int(value):
            try:
                return int(value)
            except (ValueError, TypeError):
                return 0

        # Приведение метрик к корректным типам
        try:
            metrics_values = {
                'total_calls': safe_int(operator_metrics.get('total_calls', 0)),
                'accepted_calls': safe_int(operator_metrics.get('accepted_calls', 0)),
                'missed_calls': safe_int(operator_metrics.get('missed_calls', 0)),
                'booked_services': safe_int(operator_metrics.get('booked_services', 0)),
                'conversion_rate_leads': safe_float(operator_metrics.get('conversion_rate_leads', 0)),
                'avg_call_rating': safe_float(operator_metrics.get('avg_call_rating', 0)),
                'avg_lead_call_rating': safe_float(operator_metrics.get('avg_lead_call_rating', 0)),
                'total_cancellations': safe_int(operator_metrics.get('total_cancellations', 0)),
                'avg_cancel_score': safe_float(operator_metrics.get('avg_cancel_score', 0)),
                'cancellation_rate': safe_float(operator_metrics.get('cancellation_rate', 0)),
                'complaint_calls': safe_int(operator_metrics.get('complaint_calls', 0)),
                'complaint_rating': safe_float(operator_metrics.get('complaint_rating', 0)),
                'avg_conversation_time': safe_float(operator_metrics.get('avg_conversation_time', 0)),
                'avg_navigation_time': safe_float(operator_metrics.get('avg_navigation_time', 0)),
                'avg_service_time': safe_float(operator_metrics.get('avg_service_time', 0)),
                'total_conversation_time': safe_float(operator_metrics.get('total_conversation_time', 0.0)),
                'missed_rate': safe_float(operator_metrics.get('missed_rate', 0)),
                'cancellation_reschedules': safe_int(operator_metrics.get('cancellation_reschedules', 0)),
                'avg_time_spam': safe_float(operator_metrics.get('avg_time_spam', 0)),
                'avg_time_reminder': safe_float(operator_metrics.get('avg_time_reminder', 0)),
                'avg_time_cancellation': safe_float(operator_metrics.get('avg_time_cancellation', 0)),
                'avg_time_complaints': safe_float(operator_metrics.get('avg_time_complaints', 0)),
                'avg_time_reservations': safe_float(operator_metrics.get('avg_time_reservations', 0)),
                'avg_time_reschedule': safe_float(operator_metrics.get('avg_time_reschedule', 0)),
}
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка приведения метрик к корректным типам: {e}")
            return "Ошибка: Некорректные данные метрик."

        # Добавляем дополнительные параметры
        metrics_values.update({
            'user_id': user_id,
            'name': name,
            'report_text': report_text,
            'period': period,
            'report_date': report_date,
            'recommendations': recommendations
        })

        # Логирование параметров для вставки
        logger.debug(f"[КРОТ]: Параметры для отчета: {metrics_values}")

        # Динамическое построение SQL-запроса
        columns = ", ".join(metrics_values.keys())
        placeholders = ", ".join(["%s"] * len(metrics_values))
        values = tuple(metrics_values.values())

        insert_report_query = f"INSERT INTO reports ({columns}) VALUES ({placeholders})"

        logger.debug(f"[КРОТ]: SQL-запрос: {insert_report_query}")
        logger.debug(f"[КРОТ]: Параметры для SQL-запроса: {values}")

        # Выполнение SQL-запроса
        try:
            async with connection.cursor() as cursor:
                await cursor.execute(insert_report_query, values)
                await connection.commit()
            logger.info(f"[КРОТ]: Отчет для пользователя {user_id} ({name}) успешно сохранен.")
            return "Отчет успешно сохранен."
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при сохранении отчета для пользователя {user_id}: {e}")
            return f"Ошибка при сохранении отчета: {e}"
    
async def create_async_connection():
    """Создание асинхронного подключения к базе данных."""
    logger.info("[КРОТ]: Попытка асинхронного подключения к базе данных MySQL...")
    try:
        connection = await aiomysql.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            db=os.getenv("DB_NAME"),
            port=int(os.getenv("DB_PORT")),
            cursorclass=aiomysql.DictCursor,
            autocommit=True,
            charset='utf8mb4'
        )
        logger.info("[КРОТ]: Подключено к серверу MySQL")
        return connection
    except aiomysql.Error as e:
        logger.error(f"[КРОТ]: Ошибка при подключении к базе данных: {e}")
        return None

async def execute_async_query(
    connection: aiomysql.Connection,
    query: str,
    params: Optional[tuple[Any, ...]] = None,
    retries: int = 3,
) -> Optional[list[dict[str, Any]]]:
    """
    Выполнение SQL-запроса с обработкой ошибок и повторными попытками.

    :param connection: Асинхронное соединение с базой данных MySQL.
    :param query: SQL-запрос для выполнения.
    :param params: Параметры для запроса.
    :param retries: Количество попыток при ошибке.
    :return: Результат выполнения запроса в виде списка словарей или None при ошибке.
    """
    for attempt in range(1, retries + 1):  # Начинаем с 1 для читаемости
        try:
            # Проверка соединения перед выполнением запроса
            if connection is None or connection.closed:  # Проверяем атрибут `closed`
                logger.warning("[КРОТ]: Соединение с базой данных отсутствует или закрыто. Попытка восстановления...")
                connection = await create_async_connection()
                if connection is None:
                    logger.error("[КРОТ]: Не удалось восстановить соединение с базой данных.")
                    return None

            # Выполнение SQL-запроса
            start_time = time.time()
            async with connection.cursor() as cursor:
                logger.debug(f"[КРОТ]: Выполнение запроса: {query}")
                logger.debug(f"[КРОТ]: С параметрами: {params}")
                await cursor.execute(query, params)
                result = await cursor.fetchall()
                elapsed_time = time.time() - start_time
                logger.info(f"[КРОТ]: Запрос выполнен успешно. Записей получено: {len(result)} (Время выполнения: {elapsed_time:.4f} сек)")
                return result

        except aiomysql.Error as e:
            logger.error(f"[КРОТ]: Ошибка при выполнении запроса '{query}': {e}")
            if e.args[0] in (2013, 2006, 1047):  # Обработка ошибок соединения
                logger.info(f"[КРОТ]: Попытка восстановления соединения. Попытка {attempt} из {retries}...")
                await connection.ensure_closed()
                connection = await create_async_connection()
                if connection is None:
                    logger.error("[КРОТ]: Не удалось восстановить соединение с базой данных.")
                    return None
            else:
                logger.error(f"[КРОТ]: Критическая ошибка при выполнении запроса: {e}")
                return None

        except Exception as e:
            logger.error(f"[КРОТ]: Непредвиденная ошибка при выполнении запроса: {e}")
            return None

        if attempt == retries:
            logger.error("[КРОТ]: Запрос не выполнен после всех попыток.")
            return None