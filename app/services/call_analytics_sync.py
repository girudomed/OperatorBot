"""
ETL Service для синхронизации call_scores → call_analytics.

Переносит данные из call_scores в денормализованную таблицу call_analytics
для ускорения аналитических запросов.
"""


from typing import Optional
from datetime import date, datetime, timedelta

from app.db.manager import DatabaseManager
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)


class CallAnalyticsSyncService:
    """
    Сервис синхронизации call_scores → call_analytics.
    
    Режимы:
    - Полное заполнение (первый запуск)
    - Инкрементальное обновление (cron)
    """
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
    
    async def sync_all(self, batch_size: int = 1000) -> dict:
        """
        Полная синхронизация всех звонков из call_scores в call_analytics.
        
        Используется при первом запуске или полном пересчете.
        
        Args:
            batch_size: Размер batch для вставки
        
        Returns:
            Dict со статистикой (inserted, skipped, errors)
        """
        logger.info("[ETL] Starting FULL sync call_scores → call_analytics")
        
        stats = {
            'inserted': 0,
            'skipped': 0,
            'errors': 0,
            'start_time': datetime.now()
        }
        
        try:
            # Получить количество записей для синхронизации
            count_query = """
                SELECT COUNT(*) as count
                FROM call_scores cs
                WHERE NOT EXISTS (
                    SELECT 1 FROM call_analytics ca 
                    WHERE ca.call_scores_id = cs.id
                )
            """
            
            count_result = await self.db.execute_with_retry(count_query, fetchone=True)
            total_count = count_result.get('count', 0) if count_result else 0
            
            logger.info(f"[ETL] Found {total_count} calls to sync")
            
            if total_count == 0:
                logger.info("[ETL] Nothing to sync, call_analytics is up to date")
                return stats
            
            # Синхронизация батчами
            offset = 0
            
            while True:
                batch_stats = await self._sync_batch(offset, batch_size)
                
                stats['inserted'] += batch_stats['inserted']
                stats['skipped'] += batch_stats['skipped']
                stats['errors'] += batch_stats['errors']
                
                if batch_stats['inserted'] == 0:
                    break
                
                offset += batch_size
                
                logger.info(
                    f"[ETL] Progress: {stats['inserted']}/{total_count} "
                    f"({stats['inserted']/total_count*100:.1f}%)"
                )
            
            stats['end_time'] = datetime.now()
            stats['duration'] = (stats['end_time'] - stats['start_time']).total_seconds()
            
            logger.info(
                f"[ETL] Full sync completed: inserted={stats['inserted']}, "
                f"skipped={stats['skipped']}, errors={stats['errors']}, "
                f"duration={stats['duration']:.2f}s"
            )
            
            return stats
            
        except Exception as e:
            logger.error(f"[ETL] Error in full sync: {e}", exc_info=True)
            stats['errors'] += 1
            return stats
    
    async def sync_new(
        self,
        since_date: Optional[date] = None,
        batch_size: int = 500
    ) -> dict:
        """
        Инкрементальная синхронизация новых/измененных звонков.
        
        Используется в cron для регулярного обновления.
        
        Args:
            since_date: Синхронизировать с этой даты (default: вчера)
            batch_size: Размер batch
        
        Returns:
            Dict со статистикой
        """
        if not since_date:
            since_date = date.today() - timedelta(days=1)

        if isinstance(since_date, datetime):
            since_point = since_date
        else:
            since_point = datetime.combine(since_date, datetime.min.time())
        
        logger.info(f"[ETL] Starting INCREMENTAL sync since {since_date}")
        
        stats = {
            'inserted': 0,
            'updated': 0,
            'skipped': 0,
            'errors': 0,
            'start_time': datetime.now()
        }
        
        try:
            # Синхронизировать только новые или обновленные
            query = """
                INSERT INTO call_analytics (
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
                )
                SELECT 
                    cs.id,
                    cs.history_id,
                    cs.call_date,
                    COALESCE(
                        CASE 
                            WHEN cs.context_type = 'входящий' THEN cs.called_info
                            WHEN cs.context_type = 'исходящий' THEN cs.caller_info
                        END,
                        'Unknown'
                    ) as operator_name,
                    NULL as operator_extension,
                    cs.is_target,
                    cs.outcome,
                    cs.call_category,
                    cs.call_score,
                    cs.talk_duration,
                    cs.ml_p_record,
                    cs.ml_score_pred,
                    cs.ml_p_complaint,
                    NOW()
                FROM call_scores cs
                INNER JOIN call_history ch ON ch.history_id = cs.history_id
                WHERE ch.created_at >= %s
                  AND NOT EXISTS (
                      SELECT 1 FROM call_analytics ca 
                      WHERE ca.call_scores_id = cs.id
                  )
                LIMIT %s
            """
            
            result = await self.db.execute_with_retry(
                query,
                params=(since_point, batch_size),
                commit=True
            )
            
            # MySQL cursor.rowcount для INSERT
            inserted = result if isinstance(result, int) else 0
            stats['inserted'] = inserted
            
            stats['end_time'] = datetime.now()
            stats['duration'] = (stats['end_time'] - stats['start_time']).total_seconds()
            
            logger.info(
                f"[ETL] Incremental sync completed: inserted={stats['inserted']}, "
                f"duration={stats['duration']:.2f}s"
            )
            
            return stats
            
        except Exception as e:
            logger.error(f"[ETL] Error in incremental sync: {e}", exc_info=True)
            stats['errors'] += 1
            return stats
    
    async def _sync_batch(self, offset: int, limit: int) -> dict:
        """
        Синхронизировать один batch звонков.
        
        Returns:
            Dict со статистикой batch
        """
        batch_stats = {'inserted': 0, 'skipped': 0, 'errors': 0}
        
        try:
            query = """
                INSERT INTO call_analytics (
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
                )
                SELECT 
                    cs.id,
                    cs.history_id,
                    cs.call_date,
                    COALESCE(
                        CASE 
                            WHEN cs.context_type = 'входящий' THEN cs.called_info
                            WHEN cs.context_type = 'исходящий' THEN cs.caller_info
                        END,
                        'Unknown'
                    ) as operator_name,
                    NULL as operator_extension,
                    cs.is_target,
                    cs.outcome,
                    cs.call_category,
                    cs.call_score,
                    cs.talk_duration,
                    cs.ml_p_record,
                    cs.ml_score_pred,
                    cs.ml_p_complaint,
                    NOW()
                FROM call_scores cs
                WHERE NOT EXISTS (
                    SELECT 1 FROM call_analytics ca 
                    WHERE ca.call_scores_id = cs.id
                )
                ORDER BY cs.id
                LIMIT %s OFFSET %s
            """
            
            result = await self.db.execute_with_retry(
                query,
                params=(limit, offset),
                commit=True
            )
            
            inserted = result if isinstance(result, int) else 0
            batch_stats['inserted'] = inserted
            
            logger.debug(
                f"[ETL] Batch sync: offset={offset}, limit={limit}, inserted={inserted}"
            )
            
            return batch_stats
            
        except Exception as e:
            logger.error(f"[ETL] Error in batch sync: {e}", exc_info=True)
            batch_stats['errors'] += 1
            return batch_stats
    
    async def get_sync_status(self) -> dict:
        """
        Получить статус синхронизации.
        
        Returns:
            Dict со статусом (call_scores_count, call_analytics_count, missing, etc)
        """
        logger.info("[ETL] Getting sync status")
        
        try:
            # Количество в call_scores
            cs_query = "SELECT COUNT(*) as count FROM call_scores"
            cs_result = await self.db.execute_with_retry(cs_query, fetchone=True)
            cs_count = cs_result.get('count', 0) if cs_result else 0
            
            # Количество в call_analytics
            ca_query = "SELECT COUNT(*) as count FROM call_analytics"
            ca_result = await self.db.execute_with_retry(ca_query, fetchone=True)
            ca_count = ca_result.get('count', 0) if ca_result else 0
            
            # Количество несинхронизированных
            missing_query = """
                SELECT COUNT(*) as count
                FROM call_scores cs
                WHERE NOT EXISTS (
                    SELECT 1 FROM call_analytics ca 
                    WHERE ca.call_scores_id = cs.id
                )
            """
            missing_result = await self.db.execute_with_retry(missing_query, fetchone=True)
            missing_count = missing_result.get('count', 0) if missing_result else 0
            
            # Последняя синхронизированная запись
            last_query = """
                SELECT MAX(created_at) as last_sync
                FROM call_analytics
            """
            last_result = await self.db.execute_with_retry(last_query, fetchone=True)
            last_sync = last_result.get('last_sync') if last_result else None
            
            status = {
                'call_scores_count': cs_count,
                'call_analytics_count': ca_count,
                'missing_count': missing_count,
                'sync_percentage': (ca_count / cs_count * 100) if cs_count > 0 else 0,
                'last_sync': last_sync,
                'is_synced': missing_count == 0
            }
            
            logger.info(
                f"[ETL] Sync status: {ca_count}/{cs_count} "
                f"({status['sync_percentage']:.1f}%), missing={missing_count}"
            )
            
            return status
            
        except Exception as e:
            logger.error(f"[ETL] Error getting sync status: {e}", exc_info=True)
            return {}
