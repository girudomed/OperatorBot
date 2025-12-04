"""
Dashboard Cache Service для работы с operator_dashboards.

Кеширует метрики дашборда с TTL для ускорения загрузки.
БД уже содержит таблицу operator_dashboards.
"""

import traceback
from typing import Optional, Dict, Any
from datetime import datetime, date

from app.db.manager import DatabaseManager
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)


class DashboardCacheService:
    """Сервис кеширования дашбордов операторов."""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.cache_ttl_minutes = 5  # TTL кеша
    
    async def get_cached_dashboard(
        self,
        operator_name: str,
        period_type: str,  # 'day', 'week', 'month'
        period_start: date
    ) -> Optional[Dict[str, Any]]:
        """
        Получить закешированный дашборд из operator_dashboards.
        
        Returns:
            Dict с метриками или None если кеш устарел/отсутствует
        """
        logger.info(
            f"[CACHE] Getting cached dashboard: operator={operator_name}, "
            f"period={period_type}, start={period_start}"
        )
        
        try:
            query = """
                SELECT 
                    operator_name,
                    period_type,
                    period_start,
                    period_end,
                    total_calls,
                    accepted_calls,
                    missed_calls,
                    records_count,
                    leads_no_record,
                    wish_to_record,
                    conversion_rate,
                    avg_score_all,
                    avg_score_leads,
                    avg_score_cancel,
                    cancel_calls,
                    reschedule_calls,
                    cancel_share,
                    avg_talk_all,
                    total_talk_time,
                    avg_talk_record,
                    avg_talk_navigation,
                    avg_talk_spam,
                    complaint_calls,
                    avg_score_complaint,
                    cached_at
                FROM operator_dashboards
                WHERE operator_name = %s
                  AND period_type = %s
                  AND period_start = %s
                  AND cached_at >= DATE_SUB(NOW(), INTERVAL %s MINUTE)
                ORDER BY cached_at DESC
                LIMIT 1
            """
            
            result = await self.db.execute_with_retry(
                query,
                params=(operator_name, period_type, period_start, self.cache_ttl_minutes),
                fetchone=True
            )
            
            if result:
                logger.info(f"[CACHE] HIT: Found cached dashboard from {result.get('cached_at')}")
                return dict(result)
            else:
                logger.info(f"[CACHE] MISS: No valid cache found")
                return None
                
        except Exception as e:
            logger.error(
                f"[CACHE] Error getting cached dashboard: {e}\n{traceback.format_exc()}"
            )
            return None
    
    async def save_dashboard_cache(
        self,
        operator_name: str,
        period_type: str,
        period_start: date,
        period_end: date,
        metrics: Dict[str, Any]
    ) -> bool:
        """
        Сохранить дашборд в кеш (UPSERT).
        
        Args:
            operator_name: Имя оператора
            period_type: 'day', 'week', 'month'
            period_start: Начало периода
            period_end: Конец периода
            metrics: Словарь со всеми метриками
        """
        logger.info(
            f"[CACHE] Saving dashboard: operator={operator_name}, "
            f"period={period_type}, start={period_start}"
        )
        
        try:
            query = """
                INSERT INTO operator_dashboards (
                    operator_name,
                    period_type,
                    period_start,
                    period_end,
                    total_calls,
                    accepted_calls,
                    missed_calls,
                    records_count,
                    leads_no_record,
                    wish_to_record,
                    conversion_rate,
                    avg_score_all,
                    avg_score_leads,
                    avg_score_cancel,
                    cancel_calls,
                    reschedule_calls,
                    cancel_share,
                    avg_talk_all,
                    total_talk_time,
                    avg_talk_record,
                    avg_talk_navigation,
                    avg_talk_spam,
                    complaint_calls,
                    avg_score_complaint,
                    cached_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, NOW()
                )
                ON DUPLICATE KEY UPDATE
                    period_end = VALUES(period_end),
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
                    cached_at = NOW()
            """
            
            params = (
                operator_name,
                period_type,
                period_start,
                period_end,
                metrics.get('total_calls', 0),
                metrics.get('accepted_calls', 0),
                metrics.get('missed_calls', 0),
                metrics.get('records', 0),
                metrics.get('leads_no_record', 0),
                metrics.get('wish_to_record', 0),
                metrics.get('conversion_rate', 0.0),
                metrics.get('avg_score_all', 0.0),
                metrics.get('avg_score_leads', 0.0),
                metrics.get('avg_score_cancel', 0.0),
                metrics.get('cancel_calls', 0),
                metrics.get('reschedule_calls', 0),
                metrics.get('cancel_share', 0.0),
                metrics.get('avg_talk_all', 0),
                metrics.get('total_talk_time', 0),
                metrics.get('avg_talk_record', 0),
                metrics.get('avg_talk_navigation', 0),
                metrics.get('avg_talk_spam', 0),
                metrics.get('complaint_calls', 0),
                metrics.get('avg_score_complaint', 0.0)
            )
            
            await self.db.execute_with_retry(query, params=params, commit=True)
            
            logger.info(f"[CACHE] Dashboard saved successfully")
            return True
            
        except Exception as e:
            logger.error(
                f"[CACHE] Error saving dashboard: {e}\n{traceback.format_exc()}"
            )
            return False
    
    async def invalidate_cache(
        self,
        operator_name: str,
        period_type: Optional[str] = None
    ) -> bool:
        """
        Инвалидировать кеш для оператора.
        
        Args:
            operator_name: Имя оператора
            period_type: Если указан - только этот тип периода, иначе все
        """
        logger.info(f"[CACHE] Invalidating cache for {operator_name}, period={period_type}")
        
        try:
            if period_type:
                query = """
                    DELETE FROM operator_dashboards
                    WHERE operator_name = %s AND period_type = %s
                """
                params = (operator_name, period_type)
            else:
                query = """
                    DELETE FROM operator_dashboards
                    WHERE operator_name = %s
                """
                params = (operator_name,)
            
            await self.db.execute_with_retry(query, params=params, commit=True)
            
            logger.info(f"[CACHE] Cache invalidated successfully")
            return True
            
        except Exception as e:
            logger.error(
                f"[CACHE] Error invalidating cache: {e}\n{traceback.format_exc()}"
            )
            return False
    
    async def cleanup_old_cache(self, days: int = 7) -> bool:
        """
        Очистить устаревший кеш старше N дней.
        
        Args:
            days: Количество дней для хранения
        """
        logger.info(f"[CACHE] Cleaning up cache older than {days} days")
        
        try:
            query = """
                DELETE FROM operator_dashboards
                WHERE cached_at < DATE_SUB(NOW(), INTERVAL %s DAY)
            """
            
            await self.db.execute_with_retry(query, params=(days,), commit=True)
            
            logger.info(f"[CACHE] Old cache cleaned up successfully")
            return True
            
        except Exception as e:
            logger.error(
                f"[CACHE] Error cleaning up cache: {e}\n{traceback.format_exc()}"
            )
            return False
