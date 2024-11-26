# metrics_calculator.py
from asyncio.log import logger
import logging
import datetime
from typing import List, Dict, Any, Optional, Union, Tuple
import aiomysql
from db_utils import execute_async_query


class MetricsCalculator:
    def __init__(self, connection, execute_query, logger=None):
        self.connection = connection
        self.execute_query = execute_query
        self.logger = logger or logging.getLogger(__name__)
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.DEBUG)  # Отображение всех уровней
        stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        logger.addHandler(stream_handler)
        
    def validate_date_range(
        self, start_date: Union[str, datetime.date], end_date: Union[str, datetime.date]
    ) -> Tuple[datetime.datetime, datetime.datetime]:
        if isinstance(start_date, str):
            start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        if isinstance(end_date, str):
            end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        if start_date > end_date:
            raise ValueError("Начальная дата не может быть позже конечной.")
        return start_date, end_date

    def calculate_avg_score(self, call_scores_data: List[Dict[str, Any]]) -> float:
        """
        Расчет средней оценки звонков.
        """
        scores = []
        for call in call_scores_data:
            call_score = call.get('call_score')
            if call_score and call_score.replace('.', '', 1).isdigit():
                scores.append(float(call_score))
            else:
                self.logger.warning(f"Некорректное значение call_score: {call_score}")
        avg_score = sum(scores) / len(scores) if scores else 0.0
        return avg_score
    
    
    def calculate_avg_duration(self, operator_data, category=None):
        durations = [
            float(call['talk_duration']) for call in operator_data
            if call.get('call_category') == category and call.get('talk_duration') and float(call['talk_duration']) > 10
        ]
        return sum(durations) / len(durations) if durations else 0.0
    
    async def calculate_operator_metrics(
        self,
        connection: aiomysql.Connection,
        extension: str,
        start_date: Union[str, datetime.date, datetime.datetime],
        end_date: Union[str, datetime.date, datetime.datetime]
    ) -> Optional[Dict[str, Union[str, int, float]]]:
        """
        Расчет всех метрик оператора на основе данных звонков и дополнительной информации.
        """
        self.logger.info(f"[КРОТ]: Начало расчета метрик для оператора с extension {extension}")

        try:
            # Валидация и преобразование дат
            start_datetime, end_datetime = self.validate_date_range(start_date, end_date)
            start_timestamp = int(start_datetime.timestamp())  # Unix timestamp для call_history
            end_timestamp = int(end_datetime.timestamp())     # Unix timestamp для call_history
        except (ValueError, TypeError) as e:
            self.logger.error(f"[КРОТ]: Ошибка валидации дат: {e}")
            return None

        # SQL-запросы
        call_history_query = """
        SELECT caller_info, called_info, context_start_time, talk_duration
        FROM call_history
        WHERE 
            (caller_info LIKE %s OR called_info LIKE %s)
            AND context_start_time BETWEEN %s AND %s
        """
        call_scores_query = """
        SELECT caller_info, called_info, call_date, talk_duration, call_category, call_score, result
        FROM call_scores
        WHERE 
            (caller_info LIKE %s OR called_info LIKE %s)
            AND call_date BETWEEN %s AND %s
        """

        try:
            # Параметры запросов
            params_call_history = (
                f"%{extension}%", 
                f"%{extension}%", 
                start_timestamp, 
                end_timestamp
            )
            params_call_scores = (
                f"%{extension}%", 
                f"%{extension}%", 
                start_datetime.strftime('%Y-%m-%d %H:%M:%S'), 
                end_datetime.strftime('%Y-%m-%d %H:%M:%S')
            )
            # Выполнение запросов
            call_history_data = await self.execute_query(connection, call_history_query, params_call_history)
            call_scores_data = await self.execute_query(connection, call_scores_query, params_call_scores)

        except Exception as e:
            self.logger.error(f"[КРОТ]: Ошибка при выполнении SQL-запроса: {e}")
            return None

        # Проверка данных
        if not call_history_data and not call_scores_data:
            self.logger.warning(f"[КРОТ]: Данные о звонках не найдены для периода {start_datetime} - {end_datetime}")
            return None

        self.logger.info(f"[КРОТ]: Найдено {len(call_history_data)} записей в call_history")
        self.logger.info(f"[КРОТ]: Найдено {len(call_scores_data)} записей в call_scores")

        # Расчет общих метрик звонков
        total_calls = len(call_history_data)
        accepted_calls = sum(
            1 for call in call_history_data 
            if call.get('talk_duration') is not None and float(call.get('talk_duration', 0)) > 0
        )
        missed_calls = total_calls - accepted_calls
        missed_rate = (missed_calls / total_calls) * 100 if total_calls > 0 else 0.0

        # Метрики по записям и конверсии
        booked_calls = sum(
            1 for call in call_scores_data if call.get('call_category') == 'Запись на услугу'
        )
        total_leads = sum(
            1 for call in call_scores_data if call.get('call_category') in ['Лид без записи', 'Запись на услугу']
        )
        conversion_rate_leads = (booked_calls / total_leads) * 100 if total_leads > 0 else 0.0

        # Средние оценки звонков
        avg_call_rating = self.calculate_avg_score(call_scores_data)
        lead_call_scores = [
            float(call['call_score']) for call in call_scores_data
            if call.get('call_category') in ['Лид без записи', 'Запись на услугу'] and call.get('call_score') is not None
        ]
        avg_lead_call_rating = sum(lead_call_scores) / len(lead_call_scores) if lead_call_scores else 0.0

        # Метрики по отменам и переносу записей
        total_cancellations = sum(
            1 for call in call_scores_data if call.get('call_category') == 'Отмена записи'
        )
        avg_cancel_score = self.calculate_avg_score(
            [call for call in call_scores_data if call.get('call_category') == 'Отмена записи']
        )
        cancellation_reschedules = sum(
            1 for call in call_scores_data if call.get('call_category') in ['Отмена записи', 'Перенос записи']
        )
        cancellation_rate = (total_cancellations / cancellation_reschedules) * 100 if cancellation_reschedules > 0 else 0.0

        # Общая длительность и среднее время разговора
        total_conversation_time = sum(
            float(call.get('talk_duration', 0)) for call in call_history_data 
            if call.get('talk_duration') is not None and float(call.get('talk_duration', 0)) > 10
        )
        avg_conversation_time = total_conversation_time / accepted_calls if accepted_calls > 0 else 0.0

        # Временные метрики по категориям
        predefined_categories = {
            'Навигация': 'avg_navigation_time',
            'Запись на услугу': 'avg_service_time',
            'Спам': 'avg_time_spam',
            'Напоминание о приеме': 'avg_time_reminder',
            'Отмена записи': 'avg_time_cancellation',
            'Жалоба': 'avg_time_complaints',
            'Резерв': 'avg_time_reservations',
            'Перенос записи': 'avg_time_reschedule',
        }
        avg_times_by_category = {metric_key: 0.0 for metric_key in predefined_categories.values()}        
        for category_name, metric_key in predefined_categories.items():
            calls_in_category = [
                call for call in call_scores_data
                if call.get('call_category') == category_name 
                and call.get('talk_duration') is not None
                and float(call.get('talk_duration', 0)) > 3
            ]
            if calls_in_category:
                total_duration = sum(float(call['talk_duration']) for call in calls_in_category)
                avg_duration = total_duration / len(calls_in_category)
                avg_times_by_category[metric_key] = avg_duration
            else:
                avg_times_by_category[metric_key] = 0.0

        # Жалобы
        complaint_calls = sum(
            1 for call in call_scores_data if call.get('call_category') == 'Жалоба'
        )
        complaint_rating = self.calculate_avg_score(
            [call for call in call_scores_data if call.get('call_category') == 'Жалоба']
        )

        # Агрегация метрик в общий словарь
        operator_metrics = {
            'extension': extension,
            'total_calls': total_calls,
            'accepted_calls': accepted_calls,
            'missed_calls': missed_calls,
            'missed_rate': missed_rate,
            'booked_calls': booked_calls,
            'conversion_rate_leads': conversion_rate_leads,
            'avg_call_rating': avg_call_rating,
            'avg_lead_call_rating': avg_lead_call_rating,
            'total_cancellations': total_cancellations,
            'avg_cancel_score': avg_cancel_score,
            'cancellation_rate': cancellation_rate,
            'total_conversation_time': total_conversation_time,
            'avg_conversation_time': avg_conversation_time,
            'complaint_calls': complaint_calls,
            'complaint_rating': complaint_rating,
        }

        # Добавление метрик по категориям
        operator_metrics.update(avg_times_by_category)

        self.logger.info(f"[КРОТ]: Метрики рассчитаны для оператора с extension {extension}")
        return operator_metrics
    
    def calculate_total_duration(
        self, operator_data: List[Dict[str, Any]], category: Optional[str] = None
    ) -> float:        
        """
        Подсчет общей длительности звонков.
        Если указана категория, учитываются только звонки в данной категории.
        """
        durations = [
            float(call['talk_duration']) for call in operator_data 
            if (category is None or call.get('call_category') == category) 
            and call.get('talk_duration') is not None 
            and isinstance(call['talk_duration'], (int, float, str))
            and (isinstance(call['talk_duration'], str) and call['talk_duration'].replace('.', '', 1).isdigit())
        ]
        if not durations:
            self.logger.warning(f"[КРОТ]: Нет данных о длительности звонков для категории {category or 'всех категорий'}.")
        return sum(durations)
    def calculate_booked_services(self, operator_data):
        """
        Подсчет количества записей на услугу по категории 'Запись на услугу' в call_category.
        """
        booked_services = sum(
            1 for call in operator_data 
            if call.get('call_category') == 'Запись на услугу' and call.get('caller_info') and call.get('called_info')
        )
        self.logger.info(f"[КРОТ]: Подсчитано записей на услугу: {booked_services}")
        return booked_services

    def calculate_missed_calls(self, call_history_data):
        """
        Подсчет пропущенных звонков из таблицы call_history.
        Пропущенный звонок определяется как звонок, у которого отсутствует talk_duration или transcript.
        """
        missed_calls = sum(
            1 for call in call_history_data 
            if not call.get('talk_duration') or not call.get('transcript')
        )
        self.logger.info(f"[КРОТ]: Подсчитано пропущенных звонков: {missed_calls}")
        return missed_calls

    def calculate_conversion_rate(self, operator_data):
        """
        Подсчет конверсии в запись от желающих записаться.
        """
        leads_and_booked = sum(
    1 for call in operator_data 
    if call.get('call_category') in ['Запись на услугу', 'Лид без записи']
        )
        booked_services = sum(
            1 for call in operator_data 
            if call.get('call_category') == 'Запись на услугу'
        )
        if leads_and_booked == 0:
            self.logger.warning(f"[КРОТ]: Нет данных для расчета конверсии.")
            conversion_rate = 0.0
        else:
            conversion_rate = (booked_services / leads_and_booked) * 100
        self.logger.info(f"[КРОТ]: Конверсия в запись: {conversion_rate}%")
        return conversion_rate

    def calculate_cancellation_rate(self, operator_data):
        """
        Подсчет доли отмен от числа позвонивших отменить запись.
        """
        cancellations = sum(1 for call in operator_data if call.get('call_category') == 'Отмена записи')
        reschedules = sum(1 for call in operator_data if call.get('call_category') == 'Перенос записи')
        total = cancellations + reschedules
        if total == 0:
            self.logger.warning("[КРОТ]: Отсутствуют данные для расчета доли отмен.")
            cancellation_rate = 0.0
        else:
            cancellation_rate = (cancellations / total) * 100
        self.logger.info(f"[КРОТ]: Доля отмен: {cancellation_rate}%")
        return cancellation_rate