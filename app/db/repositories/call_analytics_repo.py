# Файл: app/db/repositories/call_analytics_repo.py

"""
Repository для работы с аналитической таблицей call_analytics.

Используется для тяжелых агрегаций вместо call_scores.
Содержит денормализованные данные для быстрого доступа.
"""

from __future__ import annotations

import traceback
from typing import List, Dict, Any, Optional
from datetime import date, datetime, time

from app.db.manager import DatabaseManager
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)


class CallAnalyticsRepository:
    """
    Repository для работы с call_analytics (денормализованная таблица).
    
    Использовать вместо call_scores для:
    - Агрегаций по операторам
    - Тяжелых выборок для дашбордов
    - Статистики за периоды
    """
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
    
    @staticmethod
    def _normalize_period(
        date_from: date | datetime,
        date_to: date | datetime,
    ) -> tuple[datetime, datetime]:
        start_dt = date_from if isinstance(date_from, datetime) else datetime.combine(date_from, time.min)
        end_dt = date_to if isinstance(date_to, datetime) else datetime.combine(date_to, time.max)
        return start_dt, end_dt

    async def get_by_operator_period(
        self,
        operator_name: str,
        date_from: date,
        date_to: date
    ) -> List[Dict[str, Any]]:
        """
        Получить звонки оператора за период из call_analytics.
        
        Args:
            operator_name: Имя оператора
            date_from: Начало периода
            date_to: Конец периода
        
        Returns:
            Список звонков с денормализованными полями
        """
        logger.info(
            f"[CALL_ANALYTICS] Getting calls: operator={operator_name}, "
            f"period={date_from} to {date_to}"
        )
        
        try:
            period_start, period_end = self._normalize_period(date_from, date_to)
            query = """
                SELECT 
                    call_scores_id,
                    history_id,
                    call_date,
                    operator_name,
                    operator_extension,
                    is_target,
                    outcome,
                    call_category,
                    call_score,
                    talk_duration,
                    ml_p_record,
                    ml_score_pred,
                    ml_p_complaint,
                    created_at
                FROM call_analytics
                WHERE operator_name = %s
                  AND call_date BETWEEN %s AND %s
                ORDER BY call_date DESC, created_at DESC
            """
            
            results = await self.db.execute_with_retry(
                query,
                params=(operator_name, period_start, period_end),
                fetchall=True
            ) or []
            
            logger.info(f"[CALL_ANALYTICS] Found {len(results)} calls")
            
            return [dict(row) for row in results]
            
        except Exception as e:
            logger.error(
                f"[CALL_ANALYTICS] Error getting calls: {e}\n{traceback.format_exc()}"
            )
            return []
    
    async def get_all_operators_period(
        self,
        date_from: date,
        date_to: date
    ) -> List[Dict[str, Any]]:
        """
        Получить звонки всех операторов за период.
        
        Args:
            date_from: Начало периода
            date_to: Конец периода
        
        Returns:
            Список звонков всех операторов
        """
        logger.info(f"[CALL_ANALYTICS] Getting all operators calls: {date_from} to {date_to}")
        
        try:
            period_start, period_end = self._normalize_period(date_from, date_to)
            query = """
                SELECT 
                    operator_name,
                    operator_extension,
                    call_date,
                    is_target,
                    outcome,
                    call_category,
                    call_score,
                    talk_duration
                FROM call_analytics
                WHERE call_date BETWEEN %s AND %s
                ORDER BY operator_name, call_date DESC
            """
            
            results = await self.db.execute_with_retry(
                query,
                params=(period_start, period_end),
                fetchall=True
            ) or []
            
            logger.info(f"[CALL_ANALYTICS] Found {len(results)} total calls")
            
            return [dict(row) for row in results]
            
        except Exception as e:
            logger.error(
                f"[CALL_ANALYTICS] Error getting all calls: {e}\n{traceback.format_exc()}"
            )
            return []
    
    async def get_by_call_id(
        self,
        call_scores_id: Optional[int] = None,
        history_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Получить конкретный звонок по ID.
        
        Args:
            call_scores_id: ID из call_scores
            history_id: Mango history ID
        
        Returns:
            Данные звонка или None
        """
        if not call_scores_id and not history_id:
            logger.warning("[CALL_ANALYTICS] No ID provided")
            return None
        
        logger.debug(
            f"[CALL_ANALYTICS] Getting call: "
            f"call_scores_id={call_scores_id}, history_id={history_id}"
        )
        
        try:
            if call_scores_id:
                query = """
                    SELECT *
                    FROM call_analytics
                    WHERE call_scores_id = %s
                    LIMIT 1
                """
                params = (call_scores_id,)
            else:
                query = """
                    SELECT *
                    FROM call_analytics
                    WHERE history_id = %s
                    LIMIT 1
                """
                params = (history_id,)
            
            result = await self.db.execute_with_retry(
                query, params=params, fetchone=True
            )
            
            if result:
                logger.debug(f"[CALL_ANALYTICS] Found call")
                return dict(result)
            else:
                logger.debug(f"[CALL_ANALYTICS] Call not found")
                return None
                
        except Exception as e:
            logger.error(
                f"[CALL_ANALYTICS] Error getting call: {e}\n{traceback.format_exc()}"
            )
            return None
    
    async def get_aggregated_metrics(
        self,
        operator_name: str,
        date_from: date,
        date_to: date
    ) -> Dict[str, Any]:
        """
        Получить агрегированные метрики оператора за период.
        
        Использует денормализованные поля для быстрых GROUP BY.
        
        Returns:
            Dict с метриками (calls, records, avg_score, etc)
        """
        logger.info(
            f"[CALL_ANALYTICS] Aggregating metrics: operator={operator_name}, "
            f"period={date_from} to {date_to}"
        )
        
        try:
            period_start, period_end = self._normalize_period(date_from, date_to)
            query = """
                SELECT 
                    COUNT(*) as accepted_calls,
                    SUM(CASE WHEN is_target = 1 AND outcome = 'record' THEN 1 ELSE 0 END) as records,
                    SUM(CASE WHEN is_target = 1 AND outcome = 'lead_no_record' THEN 1 ELSE 0 END) as leads_no_record,
                    SUM(CASE WHEN is_target = 1 AND outcome IN ('record', 'lead_no_record') THEN 1 ELSE 0 END) as wish_to_record,
                    AVG(call_score) as avg_score_all,
                    AVG(CASE WHEN is_target = 1 AND outcome IN ('record', 'lead_no_record') THEN call_score END) as avg_score_leads,
                    AVG(CASE WHEN call_category = 'Отмена' THEN call_score END) as avg_score_cancel,
                    SUM(CASE WHEN call_category = 'Отмена' THEN 1 ELSE 0 END) as cancel_calls,
                    SUM(CASE WHEN call_category = 'Перенос' THEN 1 ELSE 0 END) as reschedule_calls,
                    AVG(CASE WHEN talk_duration > 10 THEN talk_duration END) as avg_talk_all,
                    SUM(talk_duration) as total_talk_time,
                    AVG(CASE WHEN outcome = 'record' AND talk_duration > 10 THEN talk_duration END) as avg_talk_record,
                    AVG(CASE WHEN call_category = 'Навигация' AND talk_duration > 10 THEN talk_duration END) as avg_talk_navigation,
                    AVG(CASE WHEN call_category = 'Спам' AND talk_duration > 10 THEN talk_duration END) as avg_talk_spam,
                    SUM(CASE WHEN call_category = 'Жалоба' THEN 1 ELSE 0 END) as complaint_calls,
                    AVG(CASE WHEN call_category = 'Жалоба' THEN call_score END) as avg_score_complaint
                FROM call_analytics
                WHERE operator_name = %s
                  AND call_date BETWEEN %s AND %s
            """
            
            result = await self.db.execute_with_retry(
                query,
                params=(operator_name, period_start, period_end),
                fetchone=True
            )
            
            if not result:
                logger.warning(f"[CALL_ANALYTICS] No data for {operator_name}")
                return {}
            
            metrics = dict(result)
            
            # Вычислить производные метрики
            wish_to_record = metrics.get('wish_to_record', 0) or 0
            records = metrics.get('records', 0) or 0

            if wish_to_record < records:
                logger.error(
                    "[CALL_ANALYTICS] Data inconsistency: wish_to_record < records "
                    "(wish_to_record=%s, records=%s)",
                    wish_to_record,
                    records,
                )
                wish_to_record = records

            if wish_to_record < records:
                logger.error(
                    "[CALL_ANALYTICS] Data inconsistency: wish_to_record < records",
                    extra={
                        "operator": operator_name,
                        "wish_to_record": wish_to_record,
                        "records": records,
                    },
                )
                wish_to_record = records
            
            metrics['conversion_rate'] = (
                (records / wish_to_record * 100) if wish_to_record > 0 else 0.0
            )
            
            cancel_total = (
                (metrics.get('cancel_calls', 0) or 0) + 
                (metrics.get('reschedule_calls', 0) or 0)
            )
            metrics['cancel_share'] = (
                (metrics.get('cancel_calls', 0) or 0) / cancel_total * 100 
                if cancel_total > 0 else 0.0
            )
            
            logger.info(
                f"[CALL_ANALYTICS] Metrics calculated: "
                f"calls={metrics.get('accepted_calls')}, "
                f"conversion={metrics.get('conversion_rate'):.2f}%"
            )
            
            return metrics
            
        except Exception as e:
            logger.error(
                f"[CALL_ANALYTICS] Error aggregating metrics: {e}\n{traceback.format_exc()}"
            )
            return {}
    
    async def get_operators_list(
        self,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None
    ) -> List[str]:
        """
        Получить список уникальных операторов.
        
        Args:
            date_from: Фильтр по дате (optional)
            date_to: Фильтр по дате (optional)
        
        Returns:
            Список имен операторов
        """
        logger.info("[CALL_ANALYTICS] Getting operators list")
        
        try:
            if date_from and date_to:
                period_start, period_end = self._normalize_period(date_from, date_to)
                query = """
                    SELECT DISTINCT operator_name
                    FROM call_analytics
                    WHERE call_date BETWEEN %s AND %s
                      AND operator_name IS NOT NULL
                    ORDER BY operator_name
                """
                params = (period_start, period_end)
            else:
                query = """
                    SELECT DISTINCT operator_name
                    FROM call_analytics
                    WHERE operator_name IS NOT NULL
                    ORDER BY operator_name
                """
                params = None
            
            results = await self.db.execute_with_retry(
                query, params=params, fetchall=True
            ) or []
            
            operators = [row['operator_name'] for row in results if row.get('operator_name')]
            
            logger.info(f"[CALL_ANALYTICS] Found {len(operators)} operators")
            
            return operators
            
        except Exception as e:
            logger.error(
                f"[CALL_ANALYTICS] Error getting operators: {e}\n{traceback.format_exc()}"
            )
            return []
    
    async def get_call_count(
        self,
        operator_name: Optional[str] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None
    ) -> int:
        """
        Получить количество звонков (для проверки синхронизации).
        
        Returns:
            Количество записей в call_analytics
        """
        try:
            conditions = []
            params = []
            
            if operator_name:
                conditions.append("operator_name = %s")
                params.append(operator_name)
            
            if date_from and date_to:
                period_start, period_end = self._normalize_period(date_from, date_to)
                conditions.append("call_date BETWEEN %s AND %s")
                params.extend([period_start, period_end])
            
            where_clause = ""
            if conditions:
                where_clause = "WHERE " + " AND ".join(conditions)
            
            query = f"""
                SELECT COUNT(*) as count
                FROM call_analytics
                {where_clause}
            """
            
            result = await self.db.execute_with_retry(
                query, params=tuple(params) if params else None, fetchone=True
            )
            
            count = result.get('count', 0) if result else 0
            
            logger.debug(f"[CALL_ANALYTICS] Count: {count}")
            
            return count
            
        except Exception as e:
            logger.error(
                f"[CALL_ANALYTICS] Error counting: {e}\n{traceback.format_exc()}"
            )
            return 0
