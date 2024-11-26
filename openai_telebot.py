#opeanai_telebot.py
import datetime
import asyncio
import logging
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

# Загрузка переменных окружения
load_dotenv()

# Настройка Sentry
sentry_sdk.init(
    dsn=os.getenv("SENTRY_DSN"),
    traces_sample_rate=1.0,
    _experiments={"continuous_profiling_auto_start": True},
)

# Настройка логирования
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

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
            connection=db_manager,
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
        SELECT history_id, caller_info, called_info, context_start_time, talk_duration
        FROM call_history
        WHERE 
            (caller_info LIKE CONCAT(%s, '%%') OR called_info LIKE CONCAT(%s, '%%'))
            AND context_start_time BETWEEN %s AND %s
        """

        call_scores_query = """
        SELECT history_id, caller_info, called_info, call_date, talk_duration, call_category, call_score, result
        FROM call_scores
        WHERE 
            (caller_info LIKE CONCAT(%s, '%%') OR called_info LIKE CONCAT(%s, '%%'))
            AND call_date BETWEEN %s AND %s
        """

        try:
            # Параметры для запросов
            params_call_history = (
                extension,
                extension,
                start_timestamp,  # Используем timestamp для call_history
                end_timestamp     # Используем timestamp для call_history
            )

            params_call_scores = (
                extension,
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
            'call_scores': call_scores_data
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
            call_history_data = list(operator_data.get('call_history', []))
            call_scores_data = list(operator_data.get('call_scores', []))
            combined_call_data = call_history_data + call_scores_data
            # Расчет метрик оператора
            operator_metrics = await self.metrics_calculator.calculate_operator_metrics(
                connection, extension, start_date, end_date
            )            
            if not operator_metrics:
                return "Ошибка: Метрики оператора не рассчитаны."

            ## Обновляем список обязательных метрик
            required_metrics = [
            'total_calls', 'accepted_calls', 'missed_calls', 'booked_calls',
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
    
    async def generate_combined_recommendations(self, operator_metrics, operator_data, user_id, name, max_length=700, max_retries=3):
        """
        Генерация рекомендаций для оператора на основе его метрик и данных.
        """
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
            'total_calls', 'accepted_calls', 'missed_calls', 'booked_calls',
            'conversion_rate_leads', 'avg_call_rating', 'avg_lead_call_rating',
            'total_cancellations', 'avg_cancel_score', 'cancellation_rate',
            'complaint_calls', 'complaint_rating', 'avg_conversation_time',
            'avg_navigation_time', 'avg_service_time'
        ]
        missing_metrics = [metric for metric in required_metrics if metric not in operator_metrics]
        if missing_metrics:
            logger.error(f"[РЕКОМЕНДАЦИИ]: Отсутствуют обязательные метрики: {', '.join(missing_metrics)}")
            return f"Ошибка: отсутствуют метрики {', '.join(missing_metrics)}"

        # Формируем запрос для генерации рекомендаций
        try:
            coaching_prompt = f"""
            📊 Отчет за период: {start_date} — {end_date}

            1. Общая статистика по звонкам:
                - Всего звонков: {operator_metrics.get('total_calls', 0)}
                - Принято звонков: {operator_metrics.get('accepted_calls', 0)}
                - Пропущенные звонки: {operator_metrics.get('missed_calls', 0)}
                - Записаны на услугу: {operator_metrics.get('booked_calls', 0)}
                - Конверсия в запись: {operator_metrics.get('conversion_rate_leads', 0):.2f}%

            2. Качество обработки звонков:
                - Средняя оценка всех разговоров: {operator_metrics.get('avg_call_rating', 0):.2f}
                - Средняя оценка разговоров для желающих записаться: {operator_metrics.get('avg_lead_call_rating', 0):.2f}

            3. Анализ отмен:
                - Всего отмен: {operator_metrics.get('total_cancellations', 0)}
                - Средняя оценка звонков по отмене: {operator_metrics.get('avg_cancel_score', 0):.2f}
                - Доля отмен: {operator_metrics.get('cancellation_rate', 0):.2f}%

            4. Время обработки звонков:
                - Среднее время разговора по Навигации: {operator_metrics.get('avg_navigation_time', 0):.2f} секунд
                - Среднее время по Запись на услугу: {operator_metrics.get('avg_service_time', 0):.2f} секунд
            """
        except Exception as e:
            logger.error(f"[РЕКОМЕНДАЦИИ]: Ошибка при формировании coaching_prompt: {e}")
            return "Ошибка при подготовке рекомендаций."

        # Добавляем данные по времени обработки по категориям
        for key, value in operator_metrics.items():
            if key.startswith('avg_time_') and isinstance(value, (int, float)):
                category = key.replace('avg_time_', '').replace('_', ' ').capitalize()
                coaching_prompt += f"        - Среднее время по категории '{category}': {value:.2f} секунд\n"

        coaching_prompt += f"""
            5. Работа с жалобами:
                - Количество звонков с жалобами: {operator_metrics.get('complaint_calls', 0)}
                - Средняя оценка звонков с жалобами: {operator_metrics.get('complaint_rating', 0):.2f}

        ### Рекомендации:
            На основе сведений о звонках и данных в поле `result` предоставь персонализированные рекомендации оператору {name}.
                Укажи:
                - Что оператор делает хорошо, основываясь на положительных фактах.
                - Какие аспекты можно улучшить с учетом текущих результатов.
                - Как улучшить конверсию в запись или снизить количество пропущенных звонков, если это необходимо.
        """

        # Разбиение текста на части
        try:
            sub_requests = wrap(coaching_prompt, width=max_length, break_long_words=False, break_on_hyphens=False)
        except Exception as e:
            logger.error(f"[РЕКОМЕНДАЦИИ]: Ошибка при разбиении текста: {e}")
            return "Ошибка при подготовке рекомендаций."

        full_recommendations = []
        for sub_request in sub_requests:
            for attempt in range(max_retries):
                try:
                    response = await self.client.chat.completions.create(
                        model=self.model,
                        messages=[{"role": "user", "content": sub_request}],
                        max_tokens=max_length
                    )
                    full_recommendations.append(response.choices[0].message.content.strip())
                    break
                except OpenAIError as e:
                    logger.warning(f"[РЕКОМЕНДАЦИИ]: Ошибка: {e}")
                except Exception as e:
                    logger.error(f"[РЕКОМЕНДАЦИИ]: Непредвиденная ошибка: {e}")
            else:
                full_recommendations.append("Не удалось получить рекомендации для части запроса.")

        # Проверяем итоговые рекомендации
        if not full_recommendations:
            logger.error("[РЕКОМЕНДАЦИИ]: Рекомендации не удалось сгенерировать.")
            return "Ошибка при генерации рекомендаций."

        recommendations = "\n".join(full_recommendations)
        logger.info("[РЕКОМЕНДАЦИИ]: Генерация рекомендаций завершена успешно.")
        return recommendations
    
    async def request_with_retries(self, text_packet, max_retries=3, max_tokens=1000):
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
                        temperature=0.5,
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
        'total_calls', 'accepted_calls', 'missed_calls', 'booked_calls',
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
        - Записаны на услугу: {get_metric('booked_calls', 'Нет данных')}
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
        report += """
    6. Рекомендации:
    """
        report += recommendations if recommendations else "Рекомендации не были сгенерированы."

        # Логируем успешное создание отчета
        logger.info(f"[КРОТ]: Отчет успешно отформатирован для оператора {name} с extension {get_metric('extension')}.")

        return report
        
    ##Тут все запросы в таблицу report. Метод отвечает за сохранение данных в таблицу. Метод сохранения данных в таблицу reports
    ## Метод сохранения данных в таблицу reports
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
        recommendations: str
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
        try:
            user_id = int(user_id)
        except ValueError as e:
            logger.error(f"[КРОТ]: Ошибка приведения user_id к целому числу: {e}")
            return "Ошибка: user_id должен быть целым числом."

        # Проверка обязательных параметров
        if not report_text or not recommendations:
            logger.error(f"[КРОТ]: report_text или recommendations пусты для user_id {user_id}.")
            return "Ошибка: Отчет или рекомендации отсутствуют."

        # Список обязательных метрик
        required_metrics = [
            'total_calls', 'accepted_calls', 'missed_calls', 'booked_calls',
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
                'booked_services': safe_int(operator_metrics.get('booked_services', 0)),
                'conversion_rate': safe_float(operator_metrics.get('conversion_rate', 0)),
                'avg_call_rating': safe_float(operator_metrics.get('avg_call_rating', 0)),
                'total_cancellations': safe_int(operator_metrics.get('total_cancellations', 0)),
                'cancellation_rate': safe_float(operator_metrics.get('cancellation_rate', 0)),
                'total_conversation_time': safe_float(operator_metrics.get('total_conversation_time', 0)),
                'avg_conversation_time': safe_float(operator_metrics.get('avg_conversation_time', 0)),
                'avg_spam_time': safe_float(operator_metrics.get('avg_spam_time', 0)),
                'total_spam_time': safe_float(operator_metrics.get('total_spam_time', 0)),
                'total_navigation_time': safe_float(operator_metrics.get('total_navigation_time', 0)),
                'avg_navigation_time': safe_float(operator_metrics.get('avg_navigation_time', 0)),
                'total_talk_time': safe_float(operator_metrics.get('total_talk_time', 0)),
                'complaint_calls': safe_int(operator_metrics.get('complaint_calls', 0)),
                'complaint_rating': safe_float(operator_metrics.get('complaint_rating', 0))
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