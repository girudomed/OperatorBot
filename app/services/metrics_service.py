# Файл: app/services/metrics_service.py

"""
Сервис расчета метрик.
"""

from typing import Any, Dict, List, Optional, Union, Tuple
from datetime import datetime, timedelta

from app.db.repositories.operators import OperatorRepository
from app.db.repositories.lm_repository import LMRepository
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)


class MetricsService:
    def __init__(self, repo: OperatorRepository, lm_repo: Optional[LMRepository] = None):
        self.repo = repo
        self.lm_repo = lm_repo

    async def calculate_quality_summary(
        self,
        period: str = "weekly",
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
    ) -> Dict[str, Any]:
        """
        Агрегированные метрики качества.
        """
        if start_date and end_date:
            start_dt, end_dt = self._validate_date_range(start_date, end_date)
        else:
            now = datetime.now()
            if period == 'weekly':
                start_dt = now - timedelta(days=now.weekday())
                end_dt = now
            else:
                start_dt = now.replace(hour=0, minute=0, second=0)
                end_dt = now

        start_dt = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
        end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)

        stats = await self.repo.get_quality_summary(start_dt, end_dt)
        
        total_calls = stats.get("total_calls", 0)
        missed_calls = stats.get("missed_calls", 0)
        total_leads = stats.get("total_leads", 0)
        booked_leads = stats.get("booked_leads", 0)
        
        missed_rate = (missed_calls / total_calls * 100) if total_calls else 0.0
        lead_pool = total_leads + booked_leads
        conversion_rate = (booked_leads / lead_pool * 100) if lead_pool else 0.0

        return {
            "period": period,
            "start_date": start_dt.date().isoformat(),
            "end_date": end_dt.date().isoformat(),
            "total_calls": int(total_calls),
            "missed_calls": int(missed_calls),
            "missed_rate": round(missed_rate, 2),
            "avg_score": round(float(stats.get("avg_score", 0.0)), 2),
            "total_leads": int(total_leads),
            "booked_leads": int(booked_leads),
            "lead_conversion": round(conversion_rate, 2),
            "cancellations": int(stats.get("cancellations", 0)),
        }

    async def calculate_operator_metrics(
        self,
        call_history_data: List[Dict[str, Any]],
        call_scores_data: List[Dict[str, Any]],
        extension: str,
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, Any]:
        """
        Расчет метрик оператора на основе переданных данных.
        """
        # Merge data to get full context (scores + history)
        operator_calls = self._merge_call_data(call_history_data, call_scores_data)
        
        # ИСПРАВЛЕНИЕ: Пропущенный = входящий звонок с нулевой длительностью разговора
        # (не используем transcript, так как это не надёжный признак)
        missed_calls = [
            c for c in operator_calls 
            if c.get('call_type') == 'входящий' and float(c.get('talk_duration', 0)) == 0
        ]
        accepted_calls = [c for c in operator_calls if c not in missed_calls]
        
        total_calls = len(operator_calls)
        accepted_count = len(accepted_calls)
        missed_count = len(missed_calls)
        missed_rate = (missed_count / total_calls * 100) if total_calls > 0 else 0.0

        # ИСПРАВЛЕНИЕ: Записавшийся лид = outcome='record' (а не категория)
        def _is_successful_booking(call: Dict[str, Any]) -> bool:
            category = (call.get('call_category') or '').lower()
            if call.get('outcome') == 'record':
                return True
            return (
                category.startswith('запись на услугу')
                and 'успеш' in category
            )
        
        booked_services = sum(
            1 for c in accepted_calls if _is_successful_booking(c)
        )
        
        # ИСПРАВЛЕНИЕ: Лид = только outcome='lead_no_record' ИЛИ категория содержит 'Лид'
        # НО исключаем записи (это уже конверсия!)
        total_leads = sum(
            1 for c in accepted_calls 
            if (
                c.get('outcome') == 'lead_no_record' or 
                (c.get('call_category') and 'Лид' in str(c.get('call_category')))
            )
            and not _is_successful_booking(c)  # исключаем записи
        )
        
        conversion_rate = (booked_services / accepted_count * 100) if accepted_count > 0 else 0.0

        # Ratings
        avg_call_rating = self._calculate_avg_score(accepted_calls)
        
        lead_calls = [
            c for c in accepted_calls 
            if (
                c.get('outcome') == 'lead_no_record' or 
                ('Лид' in str(c.get('call_category')))
            )
            and not _is_successful_booking(c)
        ]
        avg_lead_call_rating = self._calculate_avg_score(lead_calls)

        # ИСПРАВЛЕНИЕ: Отмена = outcome='cancel' ИЛИ есть refusal_reason
        total_cancellations = sum(
            1 for c in accepted_calls 
            if c.get('outcome') == 'cancel' or c.get('refusal_reason') is not None
        )
        
        cancel_calls = [
            c for c in accepted_calls 
            if c.get('outcome') == 'cancel' or c.get('refusal_reason') is not None
        ]
        avg_cancel_score = self._calculate_avg_score(cancel_calls)
        
        cancel_reschedule_count = sum(
            1 for c in accepted_calls 
            if c.get('call_category') in ['Отмена записи', 'Перенос записи']
        )
        cancellation_rate = (total_cancellations / cancel_reschedule_count * 100) if cancel_reschedule_count > 0 else 0.0

        # Durations
        total_duration = sum(float(c.get('talk_duration', 0)) for c in accepted_calls)
        avg_duration = total_duration / accepted_count if accepted_count > 0 else 0.0

        # Complaints
        complaint_calls = [c for c in accepted_calls if c.get('call_category') == 'Жалоба']
        complaint_count = len(complaint_calls)
        complaint_rating = self._calculate_avg_score(complaint_calls)

        metrics = {
            'extension': extension,
            'total_calls': total_calls,
            'total_leads': total_leads,
            'accepted_calls': accepted_count,
            'missed_calls': missed_count,
            'missed_rate': missed_rate,
            'booked_services': booked_services,
            'conversion_rate_leads': conversion_rate,
            'avg_call_rating': avg_call_rating,
            'avg_lead_call_rating': avg_lead_call_rating,
            'total_cancellations': total_cancellations,
            'avg_cancel_score': avg_cancel_score,
            'cancellation_rate': cancellation_rate,
            'total_conversation_time': total_duration,
            'avg_conversation_time': avg_duration,
            'complaint_calls': complaint_count,
            'complaint_rating': complaint_rating,
        }

        # Category Durations
        categories = {
            'Навигация': 'avg_navigation_time',
            'Запись на услугу (успешная)': 'avg_service_time',
            'Спам': 'avg_time_spam',
            'Напоминание о приеме': 'avg_time_reminder',
            'Отмена записи': 'avg_time_cancellation',
            'Жалоба': 'avg_time_complaints',
            'Резерв': 'avg_time_reservations',
            'Перенос записи': 'avg_time_reschedule',
        }
        for cat_name, metric_key in categories.items():
            cat_calls = [
                c for c in accepted_calls 
                if c.get('call_category') == cat_name and float(c.get('talk_duration', 0)) > 3
            ]
            if cat_calls:
                avg = sum(float(c['talk_duration']) for c in cat_calls) / len(cat_calls)
                metrics[metric_key] = avg
            else:
                metrics[metric_key] = 0.0

        return metrics

    def _merge_call_data(self, history: List[Dict], scores: List[Dict]) -> List[Dict]:
        scores_map = {row['history_id']: row for row in scores}
        merged = []
        for h in history:
            h_id = h['history_id']
            if h_id in scores_map:
                merged.append({**h, **scores_map[h_id]})
            else:
                merged.append({**h, 'call_category': None, 'call_score': None, 'result': None})
        return merged

    def _calculate_avg_score(self, calls: List[Dict]) -> float:
        scores = []
        for c in calls:
            s = c.get('call_score')
            if s and str(s).replace('.', '', 1).isdigit():
                scores.append(float(s))
        return sum(scores) / len(scores) if scores else 0.0

    def _validate_date_range(self, start: Union[str, datetime], end: Union[str, datetime]) -> Tuple[datetime, datetime]:
        if isinstance(start, str):
            start = datetime.strptime(start, '%Y-%m-%d')
        if isinstance(end, str):
            end = datetime.strptime(end, '%Y-%m-%d')
        return start, end

    # ============================================================================
    # LM INTEGRATION METHODS
    # ============================================================================

    async def get_lm_enhanced_metrics(
        self,
        period: str = "weekly",
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
    ) -> Dict[str, Any]:
        """
        Получает метрики, обогащенные данными LM.
        Комбинирует традиционные метрики с метриками LM.
        
        Returns:
            Словарь с традиционными и LM метриками
        """
        # Get traditional metrics
        traditional_metrics = await self.calculate_quality_summary(
            period=period,
            start_date=start_date,
            end_date=end_date
        )
        
        # If LM repository is not configured, return traditional only
        if not self.lm_repo:
            return {
                **traditional_metrics,
                'lm_enabled': False
            }
        
        # Get LM metrics
        if start_date and end_date:
            start_dt, end_dt = self._validate_date_range(start_date, end_date)
        else:
            now = datetime.now()
            if period == 'weekly':
                start_dt = now - timedelta(days=now.weekday())
                end_dt = now
            else:
                start_dt = now.replace(hour=0, minute=0, second=0)
                end_dt = now
        
        start_dt = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
        end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        # Get aggregated LM metrics
        lm_metrics = await self._get_lm_aggregates(start_dt, end_dt)
        
        return {
            **traditional_metrics,
            'lm_enabled': True,
            'lm_metrics': lm_metrics
        }

    async def _get_lm_aggregates(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """
        Получает агрегированные метрики LM за период.
        """
        if not self.lm_repo:
            return {}
        
        # Define key metrics to aggregate
        key_metrics = [
            'conversion_score',
            'normalized_call_score',
            'churn_risk_level',
            'conversion_prob_forecast'
        ]
        
        try:
            # Get aggregated stats
            aggregated = await self.lm_repo.get_aggregated_metrics(
                metric_codes=key_metrics,
                start_date=start_date,
                end_date=end_date,
                group_by='metric_code'
            )
            
            # Format results
            lm_data = {}
            for row in aggregated:
                metric_code = row.get('metric_code')
                lm_data[metric_code] = {
                    'avg': round(float(row.get('avg_value', 0)), 2) if row.get('avg_value') else None,
                    'min': round(float(row.get('min_value', 0)), 2) if row.get('min_value') else None,
                    'max': round(float(row.get('max_value', 0)), 2) if row.get('max_value') else None,
                    'count': int(row.get('count', 0))
                }
            
            # Get risk distribution
            churn_stats = await self._get_risk_distribution(start_date, end_date)
            
            return {
                'aggregates': lm_data,
                'risk_distribution': churn_stats
            }
        except Exception as e:
            logger.error(f"Failed to get LM aggregates: {e}", exc_info=True)
            return {}

    async def _get_risk_distribution(self, start_date: datetime, end_date: datetime) -> Dict[str, int]:
        """
        Получает распределение уровней риска оттока.
        """
        if not self.lm_repo:
            return {}
        
        try:
            # Get churn risk values
            churn_values = await self.lm_repo.get_lm_values_by_metric(
                metric_code='churn_risk_level',
                start_date=start_date,
                end_date=end_date
            )
            
            # Count by level
            distribution = {'low': 0, 'medium': 0, 'high': 0}
            for value in churn_values:
                level = value.get('value_label', '').lower()
                if level in distribution:
                    distribution[level] += 1
            
            return distribution
        except Exception as e:
            logger.error(f"Failed to get risk distribution: {e}", exc_info=True)
            return {}

    async def get_operator_uplift(
        self,
        operator_name: str,
        days: int = 7
    ) -> Dict[str, Any]:
        """
        Расчёт Uplift оператора: факт записей vs прогноз модели.
        
        Returns:
            {
                'expected_records': float,  # прогноз модели
                'actual_records': int,      # факт
                'uplift': float,            # разница
                'difficulty_index': float   # сложность потока
            }
        """
        if not self.lm_repo:
            return {'expected_records': 0, 'actual_records': 0, 'uplift': 0, 'difficulty_index': 0}
        
        from datetime import datetime, timedelta
        start_date = datetime.now() - timedelta(days=days)
        
        try:
            # Получаем прогнозы конверсии для оператора
            query = """
                SELECT 
                    SUM(lv.value_numeric) as expected_records,
                    COUNT(CASE WHEN cs.outcome = 'record' THEN 1 END) as actual_records,
                    AVG(1 - lv.value_numeric) as difficulty_index
                FROM lm_value lv
                JOIN call_scores cs ON lv.history_id = cs.history_id
                WHERE lv.metric_code = 'conversion_prob_forecast'
                AND lv.created_at >= %s
                AND (cs.called_info LIKE %s OR cs.caller_info LIKE %s)
                AND cs.is_target = 1
            """
            
            operator_pattern = f"%{operator_name}%"
            row = await self.repo.db_manager.execute_with_retry(
                query,
                (start_date, operator_pattern, operator_pattern),
                fetchone=True
            )
            
            expected = float(row.get('expected_records') or 0) if row else 0
            actual = int(row.get('actual_records') or 0) if row else 0
            difficulty = float(row.get('difficulty_index') or 0) if row else 0
            
            return {
                'expected_records': round(expected, 1),
                'actual_records': actual,
                'uplift': round(actual - expected, 1),
                'difficulty_index': round(difficulty * 100, 1)
            }
        except Exception as e:
            logger.error(f"Failed to calculate operator uplift: {e}", exc_info=True)
            return {'expected_records': 0, 'actual_records': 0, 'uplift': 0, 'difficulty_index': 0}

    async def get_hot_missed_leads(
        self,
        operator_name: Optional[str] = None,
        threshold: float = 0.7,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Получает «горячие» упущенные лиды — звонки с высокой вероятностью записи,
        которые не привели к записи.
        
        Args:
            operator_name: Фильтр по оператору
            threshold: Порог вероятности (>=0.7 по умолчанию)
            limit: Максимальное количество результатов
            
        Returns:
            Список звонков с полями: history_id, caller_number, p_record, refusal_reason
        """
        if not self.lm_repo:
            return []
        
        from datetime import datetime, timedelta
        start_date = datetime.now() - timedelta(days=7)
        
        try:
            base_query = """
                SELECT 
                    lv.history_id,
                    cs.caller_number,
                    lv.value_numeric as p_record,
                    cs.refusal_reason,
                    cs.call_category
                FROM lm_value lv
                JOIN call_scores cs ON lv.history_id = cs.history_id
                WHERE lv.metric_code = 'conversion_prob_forecast'
                AND lv.value_numeric >= %s
                AND cs.outcome = 'lead_no_record'
                AND cs.is_target = 1
                AND lv.created_at >= %s
            """
            
            params = [threshold, start_date]
            
            if operator_name:
                base_query += " AND (cs.called_info LIKE %s OR cs.caller_info LIKE %s)"
                params.extend([f"%{operator_name}%", f"%{operator_name}%"])
            
            base_query += " ORDER BY lv.value_numeric DESC LIMIT %s"
            params.append(limit)
            
            rows = await self.repo.db_manager.execute_with_retry(
                base_query,
                tuple(params),
                fetchall=True
            ) or []
            
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Failed to get hot missed leads: {e}", exc_info=True)
            return []
