# metrics_calculator.py
from asyncio.log import logger
import logging
import datetime
from typing import List, Dict, Any, Optional, Union, Tuple
import aiomysql
from db_utils import execute_async_query
from operator_data import OperatorData


class MetricsCalculator:
    def __init__(self, db_manager, execute_query, logger=None):
        self.db_manager = db_manager
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
            if call_score and str(call_score).replace('.', '', 1).isdigit():
                scores.append(float(call_score))
            else:
                self.logger.warning(f"Некорректное значение call_score: {call_score}")
        avg_score = sum(scores) / len(scores) if scores else 0.0
        self.logger.info(f"[КРОТ]: Расчитанная средняя оценка: {avg_score:.2f}")
        return avg_score
    
    
    def calculate_avg_duration(operator_data, category=None) -> float:
        """
        Расчет средней длительности звонков.
        """
        MIN_VALID_DURATION = 10  # минимальная валидная длительность звонка
        
        durations = []
        for call in operator_data:
            if not (category is None or call.get('call_category') == category):
                continue
                
            try:
                duration = float(call.get('talk_duration', 0))
                if duration >= MIN_VALID_DURATION:
                    durations.append(duration)
            except (ValueError, TypeError):
                logging.warning(f"Некорректная длительность звонка: {call.get('talk_duration')}")
        
        return round(sum(durations) / len(durations), 2) if durations else 0.0
    
    async def calculate_operator_metrics(
    self,
    call_history_data: List[Dict[str, Any]],
    call_scores_data: List[Dict[str, Any]],
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
        except (ValueError, TypeError) as e:
            self.logger.error(f"[КРОТ]: Ошибка валидации дат: {e}")
            return None

        # Получение данных о звонках
        operator_data_instance = OperatorData(self.db_manager)
        operator_calls = await operator_data_instance.get_operator_calls(extension, start_date, end_date)
        if not operator_calls:
            self.logger.warning(f"[КРОТ]: Данные о звонках не найдены для оператора {extension}")
            return None

        self.logger.info(f"[КРОТ]: Получено {len(operator_calls)} звонков для оператора {extension}")

        # Принятые звонки: transcript не NULL
        accepted_calls = [
            call for call in operator_calls
            if call.get('transcript') is not None
        ]

        # Пропущенные звонки: transcript NULL
        missed_calls = [
            call for call in operator_calls
            if call.get('transcript') is None
        ]

        accepted_calls_count = len(accepted_calls)
        missed_calls_count = len(missed_calls)
        total_calls = accepted_calls_count + missed_calls_count
        missed_rate = (missed_calls_count / total_calls) * 100 if total_calls > 0 else 0.0

        self.logger.info(f"[КРОТ]: Всего звонков: {total_calls}")
        self.logger.info(f"[КРОТ]: Принятых звонков: {accepted_calls_count}")
        self.logger.info(f"[КРОТ]: Пропущенных звонков: {missed_calls_count}")

        # Продолжайте расчет метрик, используя accepted_calls и missed_calls

        # Метрики по записям и конверсии
        booked_services = sum(
            1 for call in accepted_calls if call.get('call_category') == 'Запись на услугу (успешная)'
        )
        self.logger.info(f"[КРОТ]: Количество записей на услугу (booked_services): {booked_services}")

        total_leads = sum(
            1 for call in accepted_calls if call.get('call_category') in ['Лид (без записи)', 'Запись на услугу (успешная)']
        )
        self.logger.info(f"[КРОТ]: Общее количество лидов (total_leads): {total_leads}")

        if accepted_calls_count == 0:
            conversion_rate_leads = 0.0
        else:
            conversion_rate_leads = (booked_services / accepted_calls_count) * 100
            self.logger.info(f"[КРОТ]: Конверсия в запись от желающих записаться (conversion_rate_leads): {conversion_rate_leads:.2f}%")

        # Средние оценки звонков
        avg_call_rating = self.calculate_avg_score(accepted_calls)
        self.logger.info(f"[КРОТ]: Средняя оценка всех разговоров (avg_call_rating): {avg_call_rating:.2f}")

        lead_call_scores = [
            float(call['call_score']) for call in accepted_calls
            if call.get('call_category') in ['Лид (без записи)', 'Запись на услугу (успешная)'] and call.get('call_score')
        ]
        avg_lead_call_rating = self.calculate_avg_score([
            call for call in accepted_calls if call.get('call_category') in ['Лид (без записи)', 'Запись на услугу (успешная)']
        ])
        self.logger.info(f"[КРОТ]: Средняя оценка разговоров для желающих записаться (avg_lead_call_rating): {avg_lead_call_rating:.2f}")

        # Метрики по отменам и переносу записей
        total_cancellations = sum(
            1 for call in accepted_calls if call.get('call_category') == 'Отмена записи'
        )
        self.logger.info(f"[КРОТ]: Общее количество отмен (total_cancellations): {total_cancellations}")

        avg_cancel_score = self.calculate_avg_score(
            [call for call in accepted_calls if call.get('call_category') == 'Отмена записи']
        )
        self.logger.info(f"[КРОТ]: Средняя оценка звонков по отмене (avg_cancel_score): {avg_cancel_score:.2f}")

        cancellation_rate = self.calculate_cancellation_rate(accepted_calls)
        self.logger.info(f"[КРОТ]: Доля отмен от числа позвонивших отменить или перенести запись (cancellation_rate): {cancellation_rate:.2f}%")

        # Общая длительность и среднее время разговора
        total_conversation_time = sum(
            float(call.get('talk_duration', 0)) for call in accepted_calls
        )
        self.logger.info(f"[КРОТ]: Общая длительность разговоров (total_conversation_time): {total_conversation_time:.2f} секунд")

        avg_conversation_time = total_conversation_time / accepted_calls_count if accepted_calls_count > 0 else 0.0
        self.logger.info(f"[КРОТ]: Среднее время разговора (avg_conversation_time): {avg_conversation_time:.2f} секунд")

        # Временные метрики по категориям
        predefined_categories = {
            'Навигация': 'avg_navigation_time',
            'Запись на услугу (успешная)': 'avg_service_time',
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
                call for call in accepted_calls
                if call.get('call_category') == category_name
                and call.get('talk_duration') and float(call.get('talk_duration', 0)) > 3
            ]
            self.logger.info(f"[КРОТ]: Количество звонков в категории '{category_name}': {len(calls_in_category)}")
            if calls_in_category:
                total_duration = sum(float(call['talk_duration']) for call in calls_in_category)
                avg_duration = total_duration / len(calls_in_category)
                avg_times_by_category[metric_key] = avg_duration
                self.logger.info(f"[КРОТ]: Среднее время в категории '{category_name}' ({metric_key}): {avg_duration:.2f} секунд")
            else:
                self.logger.info(f"[КРОТ]: Нет данных для категории '{category_name}'")

        # Жалобы
        complaint_calls = sum(
            1 for call in accepted_calls if call.get('call_category') == 'Жалоба'
        )
        self.logger.info(f"[КРОТ]: Количество звонков с жалобами (complaint_calls): {complaint_calls}")

        complaint_rating = self.calculate_avg_score(
            [call for call in accepted_calls if call.get('call_category') == 'Жалоба']
        )
        self.logger.info(f"[КРОТ]: Средняя оценка звонков с жалобами (complaint_rating): {complaint_rating:.2f}")

        # Агрегация метрик в общий словарь
        operator_metrics = {
            'extension': extension,
            'total_calls': total_calls,
            'total_leads': total_leads,
            'accepted_calls': accepted_calls_count,
            'missed_calls': missed_calls_count,
            'missed_rate': missed_rate,
            'booked_services': booked_services,
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
        self.logger.debug(f"[КРОТ]: Итоговые метрики: {operator_metrics}")

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
        Подсчет количества записей на услугу по категории 'Запись на услугу (успешная)' в call_category.
        """
        booked_services = sum(
            1 for call in operator_data 
            if call.get('call_category') == 'Запись на услугу (успешная)' and call.get('called_info')
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
            if call.get('call_category') in ['Запись на услугу (успешная)', 'Лид (без записи)']
        )
        booked_services = sum(
            1 for call in operator_data 
            if call.get('call_category') == 'Запись на услугу (успешная)'
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
            cancellation_rate = (cancellations / total) * 100 if total > 0 else 0.0        
            self.logger.info(f"[КРОТ]: Доля отмен: {cancellation_rate}%")
        return cancellation_rate