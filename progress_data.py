# progress_data.py
import datetime
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from typing import List, Dict, Any
logger = logging.getLogger(__name__)

class ProgressData:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    async def get_operator_reports(self, operator_id: int, start_date: datetime.date, end_date: datetime.date) -> List[Dict[str, Any]]:
        """
        Получение отчетов для конкретного оператора за указанный период из таблицы reports.
        Возвращает список словарей с данными по дням.
        """
        query = """
        SELECT 
            operator_id,
            report_date,
            total_calls,
            accepted_calls,
            missed_calls,
            booked_services,
            conversion_rate_leads,
            avg_call_rating,
            avg_lead_call_rating,
            total_cancellations,
            avg_cancel_score,
            cancellation_rate,
            complaint_calls,
            complaint_rating,
            avg_conversation_time,
            avg_navigation_time,
            avg_service_time
        FROM reports
        WHERE operator_id = %s
          AND report_date BETWEEN %s AND %s
        ORDER BY report_date ASC
        """
        try:
            async with self.db_manager.acquire() as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(query, (operator_id, start_date, end_date))
                    rows = await cursor.fetchall()

            if not rows:
                logger.info(f"Не найдены отчеты для оператора {operator_id} за период {start_date} - {end_date}.")
                return []

            # Преобразуем результат в список словарей
            reports = []
            for row in rows:
                # row это dict (если вы используете DictCursor) или tuple (нужно адаптировать)
                # Предполагается DictCursor. Если нет - адаптируйте row['field_name'] по индексу
                reports.append({
                    'operator_id': row['operator_id'],
                    'report_date': row['report_date'],
                    'total_calls': row['total_calls'],
                    'accepted_calls': row['accepted_calls'],
                    'missed_calls': row['missed_calls'],
                    'booked_services': row['booked_services'],
                    'conversion_rate_leads': row['conversion_rate_leads'],
                    'avg_call_rating': row['avg_call_rating'],
                    'avg_lead_call_rating': row['avg_lead_call_rating'],
                    'total_cancellations': row['total_cancellations'],
                    'avg_cancel_score': row['avg_cancel_score'],
                    'cancellation_rate': row['cancellation_rate'],
                    'complaint_calls': row['complaint_calls'],
                    'complaint_rating': row['complaint_rating'],
                    'avg_conversation_time': row['avg_conversation_time'],
                    'avg_navigation_time': row['avg_navigation_time'],
                    'avg_service_time': row['avg_service_time']
                })

            return reports
        except Exception as e:
            logger.error(f"Ошибка при получении отчетов для оператора {operator_id}: {e}", exc_info=True)
            return []

    async def get_all_operators_reports(self, start_date: datetime.date, end_date: datetime.date) -> List[Dict[str, Any]]:
        """
        Получение отчетов для всех операторов за указанный период.
        Возвращает список словарей с данными по дням и операторам.
        """
        query = """
        SELECT
            operator_id,
            report_date,
            total_calls,
            accepted_calls,
            missed_calls,
            booked_services,
            conversion_rate_leads,
            avg_call_rating,
            avg_lead_call_rating,
            total_cancellations,
            avg_cancel_score,
            cancellation_rate,
            complaint_calls,
            complaint_rating,
            avg_conversation_time,
            avg_navigation_time,
            avg_service_time
        FROM reports
        WHERE report_date BETWEEN %s AND %s
        ORDER BY report_date ASC, operator_id ASC
        """
        try:
            async with self.db_manager.acquire() as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(query, (start_date, end_date))
                    rows = await cursor.fetchall()

            if not rows:
                logger.info(f"Не найдены отчеты для всех операторов за период {start_date} - {end_date}.")
                return []

            reports = []
            for row in rows:
                reports.append({
                    'operator_id': row['operator_id'],
                    'report_date': row['report_date'],
                    'total_calls': row['total_calls'],
                    'accepted_calls': row['accepted_calls'],
                    'missed_calls': row['missed_calls'],
                    'booked_services': row['booked_services'],
                    'conversion_rate_leads': row['conversion_rate_leads'],
                    'avg_call_rating': row['avg_call_rating'],
                    'avg_lead_call_rating': row['avg_lead_call_rating'],
                    'total_cancellations': row['total_cancellations'],
                    'avg_cancel_score': row['avg_cancel_score'],
                    'cancellation_rate': row['cancellation_rate'],
                    'complaint_calls': row['complaint_calls'],
                    'complaint_rating': row['complaint_rating'],
                    'avg_conversation_time': row['avg_conversation_time'],
                    'avg_navigation_time': row['avg_navigation_time'],
                    'avg_service_time': row['avg_service_time']
                })

            return reports
        except Exception as e:
            logger.error(f"Ошибка при получении отчетов для всех операторов: {e}", exc_info=True)
            return []

    def calculate_trends(self, reports: List[Dict[str, Any]], metric_name: str) -> Dict[str, Any]:
        """
        Рассчитывает тренды для заданной метрики на основе данных из reports.
        """
        if not reports:
            return {
                "metric": metric_name,
                "start_value": None,
                "end_value": None,
                "diff": None,
                "trend": "no_data"
            }

        # Фильтруем значения метрики, игнорируем None
        values = [r[metric_name] for r in reports if metric_name in r and r[metric_name] is not None]

        if not values:
            return {
                "metric": metric_name,
                "start_value": None,
                "end_value": None,
                "diff": None,
                "trend": "no_data"
            }

        start_value = values[0]
        end_value = values[-1]
        diff = end_value - start_value
        trend = "up" if diff > 0 else "down" if diff < 0 else "flat"

        return {
            "metric": metric_name,
            "start_value": start_value,
            "end_value": end_value,
            "diff": diff,
            "trend": trend
        }

    def calculate_average_metrics(self, reports: List[Dict[str, Any]]) -> Dict[str, float]:
        """
        Рассчитывает средние значения метрик по отчетам.
        Добавьте или удалите метрики, если нужно.
        """
        if not reports:
            return {}

        numeric_metrics = [
            'total_calls', 'accepted_calls', 'missed_calls', 'booked_services',
            'conversion_rate_leads', 'avg_call_rating', 'avg_lead_call_rating',
            'total_cancellations', 'avg_cancel_score', 'cancellation_rate',
            'complaint_calls', 'complaint_rating', 'avg_conversation_time',
            'avg_navigation_time', 'avg_service_time'
        ]

        sums = {m: 0.0 for m in numeric_metrics}
        counts = {m: 0 for m in numeric_metrics}

        for report in reports:
            for m in numeric_metrics:
                val = report.get(m)
                if isinstance(val, (int, float)):
                    sums[m] += val
                    counts[m] += 1

        averages = {m: sums[m] / counts[m] for m in numeric_metrics if counts[m] > 0}
        return averages

    def group_by_operator(self, reports: List[Dict[str, Any]]) -> Dict[int, List[Dict[str, Any]]]:
        """
        Группирует отчеты по operator_id.
        """
        grouped = {}
        for report in reports:
            op_id = report.get('operator_id')
            if op_id not in grouped:
                grouped[op_id] = []
            grouped[op_id].append(report)
        return grouped

    def filter_by_date_range(self, reports: List[Dict[str, Any]], start_date: datetime.date, end_date: datetime.date) -> List[Dict[str, Any]]:
        """
        Фильтрует отчеты по диапазону дат.
        """
        filtered = []
        for report in reports:
            report_date_str = report['report_date']
            if isinstance(report_date_str, str):
                # Проверяем, является ли report_date диапазоном дат
                if " - " in report_date_str:
                    # Если это диапазон, берем первую дату
                    report_date_str = report_date_str.split(" - ")[0].strip()
                try:
                    # Преобразуем строку в дату
                    report_date = datetime.strptime(report_date_str, '%Y-%m-%d').date()
                except ValueError:
                    # Если преобразование не удалось, пропускаем этот отчет
                    continue
            else:
                # Если report_date уже является объектом date, используем его напрямую
                report_date = report_date_str
            
            # Фильтруем по диапазону дат
            if start_date <= report_date <= end_date:
                filtered.append(report)
        return filtered