"""
Сервис кеширования для дашборда операторов.
Оптимизирует производительность за счет хранения рассчитанных метрик.
"""

from typing import Optional
from datetime import datetime, timedelta, date

from app.db.manager import DatabaseManager
from app.db.models import DashboardMetrics
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)


class DashboardCacheService:
    """Сервис для кеширования метрик дашборда."""
    
    # TTL кеша в минутах
    CACHE_TTL_MINUTES = 5
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    async def get_cached_dashboard(
        self,
        operator_name: str,
        period_type: str,
        period_start: date,
        period_end: date
    ) -> Optional[DashboardMetrics]:
        """
        Получить закешированный дашборд если он еще актуален.
        
        Returns:
            DashboardMetrics если кеш актуален, иначе None
        """
        query = """
        SELECT *
        FROM operator_dashboards
        WHERE operator_name = %s
          AND period_type = %s
          AND period_start = %s
          AND period_end = %s
          AND cached_at >= DATE_SUB(NOW(), INTERVAL %s MINUTE)
        LIMIT 1
        """
        
        result = await self.db_manager.execute_query(
            query,
            (operator_name, period_type, period_start, period_end, self.CACHE_TTL_MINUTES),
            fetchone=True
        )
        
        if not result:
            return None
        
        logger.debug(f"Cache HIT for {operator_name} {period_type}")
        
        # Конвертируем результат в DashboardMetrics
        return self._row_to_metrics(result)
    
    async def save_dashboard_cache(
        self,
        dashboard: DashboardMetrics
    ) -> None:
        """
        Сохранить дашборд в кеш.
        """
        query = """
        INSERT INTO operator_dashboards (
            operator_name, period_type, period_start, period_end,
            total_calls, accepted_calls, missed_calls,
            records_count, leads_no_record, wish_to_record, conversion_rate,
            avg_score_all, avg_score_leads, avg_score_cancel,
            cancel_calls, reschedule_calls, cancel_share,
            avg_talk_all, total_talk_time, avg_talk_record, 
            avg_talk_navigation, avg_talk_spam,
            complaint_calls, avg_score_complaint,
            expected_records, record_uplift, hot_missed_leads, difficulty_index,
            cached_at
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, %s, %s, %s,
            NOW()
        )
        ON DUPLICATE KEY UPDATE
            total_calls = VALUES(total_calls),
            accepted_calls = VALUES(accepted_calls),
            missed_calls = VALUES(missed_calls),
            records_count = VALUES(records_count),
            leads_no_record = VALUES(leads_no_record),
            wish_to_record = VALUES(wish_to_record),
            conversion_rate = VALUES(conversion_rate),
            avg_score_all = VALUES(avg_score_all),
            avg_score_leads = VALUES(avg_score_leads),
            avg_score_cancel = VALUES(avg_score_cancel),
            cancel_calls = VALUES(cancel_calls),
            reschedule_calls = VALUES(reschedule_calls),
            cancel_share = VALUES(cancel_share),
            avg_talk_all = VALUES(avg_talk_all),
            total_talk_time = VALUES(total_talk_time),
            avg_talk_record = VALUES(avg_talk_record),
            avg_talk_navigation = VALUES(avg_talk_navigation),
            avg_talk_spam = VALUES(avg_talk_spam),
            complaint_calls = VALUES(complaint_calls),
            avg_score_complaint = VALUES(avg_score_complaint),
            expected_records = VALUES(expected_records),
            record_uplift = VALUES(record_uplift),
            hot_missed_leads = VALUES(hot_missed_leads),
            difficulty_index = VALUES(difficulty_index),
            cached_at = NOW()
        """
        
        params = (
            dashboard.get('operator_name'),
            dashboard.get('period_type'),
            dashboard.get('period_start'),
            dashboard.get('period_end'),
            dashboard.get('total_calls', 0),
            dashboard.get('accepted_calls', 0),
            dashboard.get('missed_calls', 0),
            dashboard.get('records_count', 0),
            dashboard.get('leads_no_record', 0),
            dashboard.get('wish_to_record', 0),
            dashboard.get('conversion_rate', 0),
            dashboard.get('avg_score_all', 0),
            dashboard.get('avg_score_leads', 0),
            dashboard.get('avg_score_cancel', 0),
            dashboard.get('cancel_calls', 0),
            dashboard.get('reschedule_calls', 0),
            dashboard.get('cancel_share', 0),
            dashboard.get('avg_talk_all', 0),
            dashboard.get('total_talk_time', 0),
            dashboard.get('avg_talk_record', 0),
            dashboard.get('avg_talk_navigation', 0),
            dashboard.get('avg_talk_spam', 0),
            dashboard.get('complaint_calls', 0),
            dashboard.get('avg_score_complaint', 0),
            dashboard.get('expected_records'),
            dashboard.get('record_uplift'),
            dashboard.get('hot_missed_leads'),
            dashboard.get('difficulty_index')
        )
        
        await self.db_manager.execute_query(query, params)
        logger.debug(f"Saved cache for {dashboard.get('operator_name')} {dashboard.get('period_type')}")
    
    async def invalidate_cache(
        self,
        operator_name: Optional[str] = None,
        period_type: Optional[str] = None
    ) -> None:
        """
        Инвалидировать кеш для оператора или всего периода.
        
        Args:
            operator_name: имя оператора (если None - весь кеш)
            period_type: тип периода (если None - все периоды)
        """
        if operator_name and period_type:
            query = """
            DELETE FROM operator_dashboards
            WHERE operator_name = %s AND period_type = %s
            """
            params = (operator_name, period_type)
        elif operator_name:
            query = """
            DELETE FROM operator_dashboards
            WHERE operator_name = %s
            """
            params = (operator_name,)
        elif period_type:
            query = """
            DELETE FROM operator_dashboards
            WHERE period_type = %s
            """
            params = (period_type,)
        else:
            query = "DELETE FROM operator_dashboards"
            params = ()
        
        await self.db_manager.execute_query(query, params)
        logger.info(f"Invalidated cache for operator={operator_name}, period={period_type}")
    
    async def cleanup_old_cache(self, days: int = 7) -> None:
        """
        Удалить устаревший кеш старше заданного количества дней.
        
        Args:
            days: количество дней для хранения кеша
        """
        query = """
        DELETE FROM operator_dashboards
        WHERE cached_at < DATE_SUB(NOW(), INTERVAL %s DAY)
        """
        
        await self.db_manager.execute_query(query, (days,))
        logger.info(f"Cleaned up cache older than {days} days")
    
    def _row_to_metrics(self, row: dict) -> DashboardMetrics:
        """Конвертировать строку БД в DashboardMetrics."""
        return DashboardMetrics(
            operator_name=row.get('operator_name'),
            period_type=row.get('period_type'),
            period_start=row.get('period_start').isoformat() if row.get('period_start') else '',
            period_end=row.get('period_end').isoformat() if row.get('period_end') else '',
            total_calls=row.get('total_calls', 0),
            accepted_calls=row.get('accepted_calls', 0),
            missed_calls=row.get('missed_calls', 0),
            records_count=row.get('records_count', 0),
            leads_no_record=row.get('leads_no_record', 0),
            wish_to_record=row.get('wish_to_record', 0),
            conversion_rate=float(row.get('conversion_rate', 0)),
            avg_score_all=float(row.get('avg_score_all', 0)),
            avg_score_leads=float(row.get('avg_score_leads', 0)),
            avg_score_cancel=float(row.get('avg_score_cancel', 0)),
            cancel_calls=row.get('cancel_calls', 0),
            reschedule_calls=row.get('reschedule_calls', 0),
            cancel_share=float(row.get('cancel_share', 0)),
            avg_talk_all=row.get('avg_talk_all', 0),
            total_talk_time=row.get('total_talk_time', 0),
            avg_talk_record=row.get('avg_talk_record', 0),
            avg_talk_navigation=row.get('avg_talk_navigation', 0),
            avg_talk_spam=row.get('avg_talk_spam', 0),
            complaint_calls=row.get('complaint_calls', 0),
            avg_score_complaint=float(row.get('avg_score_complaint', 0)),
            expected_records=float(row.get('expected_records')) if row.get('expected_records') else None,
            record_uplift=float(row.get('record_uplift')) if row.get('record_uplift') else None,
            hot_missed_leads=row.get('hot_missed_leads'),
            difficulty_index=float(row.get('difficulty_index')) if row.get('difficulty_index') else None
        )
