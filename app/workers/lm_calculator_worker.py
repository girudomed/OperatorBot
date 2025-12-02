"""
Worker для фоновогорасчета LM метрик.

Этот worker регулярно запускается для расчета LM метрик для недавних звонков.
Может быть запущен вручную или через cron/планировщик задач.
"""

from typing import List, Optional
from datetime import datetime, timedelta
import asyncio

from app.db.manager import DatabaseManager
from app.db.repositories.lm_repository import LMRepository
from app.services.lm_service import LMService
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)


class LMCalculatorWorker:
    """Worker для расчета LM метрик в фоновом режиме."""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.lm_repo = LMRepository(db_manager)
        self.lm_service = LMService(self.lm_repo)
    
    async def process_recent_calls(
        self,
        hours_back: int = 24,
        batch_size: int = 100,
        skip_existing: bool = True
    ) -> int:
        """
        Обрабатывает недавние звонки и рассчитывает для них LM метрики.
        
        Args:
            hours_back: Сколько часов назад искать звонки
            batch_size: Размер батча для обработки
            skip_existing: Пропускать звонки, для которых уже есть LM метрики
            
        Returns:
            Количество обработанных звонков
        """
        start_time = datetime.now()
        logger.info(f"Starting LM calculation for calls from last {hours_back} hours")
        
        # Get recent calls from call_history
        cutoff_time = datetime.now() - timedelta(hours=hours_back)
        cutoff_timestamp = int(cutoff_time.timestamp())
        
        query = """
        SELECT 
            ch.history_id,
            ch.id,
            ch.call_date,
            ch.talk_duration,
            ch.call_type,
            ch.called_info,
            ch.caller_info,
            ch.caller_number,
            ch.called_number
        FROM call_history ch
        WHERE ch.context_start_time >= %s
        ORDER BY ch.context_start_time DESC
        LIMIT %s
        """
        
        history_rows = await self.db_manager.execute_with_retry(
            query,
            (cutoff_timestamp, batch_size),
            fetchall=True
        ) or []
        
        if not history_rows:
            logger.info("No recent calls found")
            return 0
        
        logger.info(f"Found {len(history_rows)} recent calls")
        
        # Get corresponding call_scores
        history_ids = [row['history_id'] for row in history_rows]
        history_ids_str = ','.join(map(str, history_ids))
        
        scores_query = f"""
        SELECT * FROM call_scores
        WHERE history_id IN ({history_ids_str})
        """
        
        scores_rows = await self.db_manager.execute_with_retry(
            scores_query,
            fetchall=True
        ) or []
        
        # Map scores by history_id
        scores_map = {row['history_id']: row for row in scores_rows}
        
        # Process each call
        processed_count = 0
        skipped_count = 0
        error_count = 0
        
        for history_row in history_rows:
            history_id = history_row['history_id']
            
            # Skip if already has LM metrics (if requested)
            if skip_existing:
                existing = await self.lm_repo.get_lm_values_by_call(history_id)
                if existing:
                    skipped_count += 1
                    continue
            
            # Get call_score if available
            call_score = scores_map.get(history_id)
            
            # Calculate all metrics
            try:
                await self.lm_service.calculate_all_metrics(
                    history_id=history_id,
                    call_history=history_row,
                    call_score=call_score,
                    calc_source="worker_batch"
                )
                processed_count += 1
            except Exception as e:
                logger.error(f"Failed to calculate LM metrics for history_id={history_id}: {e}", exc_info=True)
                error_count += 1
        
        duration = (datetime.now() - start_time).total_seconds()
        
        logger.info(
            f"LM calculation completed in {duration:.2f}s: "
            f"processed={processed_count}, skipped={skipped_count}, errors={error_count}"
        )
        
        return processed_count
    
    async def process_specific_calls(
        self,
        history_ids: List[int],
        recalculate: bool = True
    ) -> int:
        """
        Обрабатывает конкретные звонки по их history_id.
        
        Args:
            history_ids: Список history_id для обработки
            recalculate: Пересчитать, даже если метрики уже есть
            
        Returns:
            Количество обработанных звонков
        """
        if not history_ids:
            return 0
        
        logger.info(f"Processing {len(history_ids)} specific calls")
        
        # Get call_history data
        history_ids_str = ','.join(map(str, history_ids))
        
        history_query = f"""
        SELECT 
            ch.history_id,
            ch.id,
            ch.call_date,
            ch.talk_duration,
            ch.call_type,
            ch.called_info,
            ch.caller_info,
            ch.caller_number,
            ch.called_number
        FROM call_history ch
        WHERE ch.history_id IN ({history_ids_str})
        """
        
        history_rows = await self.db_manager.execute_with_retry(
            history_query,
            fetchall=True
        ) or []
        
        # Get call_scores
        scores_query = f"""
        SELECT * FROM call_scores
        WHERE history_id IN ({history_ids_str})
        """
        
        scores_rows = await self.db_manager.execute_with_retry(
            scores_query,
            fetchall=True
        ) or []
        
        scores_map = {row['history_id']: row for row in scores_rows}
        
        # Process each call
        processed_count = 0
        error_count = 0
        
        for history_row in history_rows:
            history_id = history_row['history_id']
            
            # Delete existing metrics if recalculating
            if recalculate:
                await self.lm_repo.delete_lm_values_by_call(history_id)
            
            # Get call_score if available
            call_score = scores_map.get(history_id)
            
            # Calculate all metrics
            try:
                await self.lm_service.calculate_all_metrics(
                    history_id=history_id,
                    call_history=history_row,
                    call_score=call_score,
                    calc_source="worker_specific"
                )
                processed_count += 1
            except Exception as e:
                logger.error(f"Failed to calculate LM metrics for history_id={history_id}: {e}", exc_info=True)
                error_count += 1
        
        logger.info(f"Processed {processed_count} specific calls, errors={error_count}")
        return processed_count
    
    async def backfill_all_calls(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        batch_size: int = 500
    ) -> int:
        """
        Заполняет LM метрики для всех звонков за период (backfill).
        
        Args:
            start_date: Начало периода (опционально)
            end_date: Конец периода (опционально)
            batch_size: Размер батча
            
        Returns:
            Общее количество обработанных звонков
        """
        logger.info("Starting LM backfill process")
        
        # Build query with date filters
        query = """
        SELECT 
            ch.history_id,
            ch.id,
            ch.call_date,
            ch.talk_duration,
            ch.call_type,
            ch.called_info,
            ch.caller_info,
            ch.caller_number,
            ch.called_number
        FROM call_history ch
        WHERE 1=1
        """
        params = []
        
        if start_date:
            start_ts = int(start_date.timestamp())
            query += " AND ch.context_start_time >= %s"
            params.append(start_ts)
        
        if end_date:
            end_ts = int(end_date.timestamp())
            query += " AND ch.context_start_time <= %s"
            params.append(end_ts)
        
        query += " ORDER BY ch.context_start_time ASC"
        
        # Process in batches
        total_processed = 0
        offset = 0
        
        while True:
            batch_query = query + f" LIMIT {batch_size} OFFSET {offset}"
            
            history_rows = await self.db_manager.execute_with_retry(
                batch_query,
                tuple(params),
                fetchall=True
            ) or []
            
            if not history_rows:
                break
            
            batch_history_ids = [row['history_id'] for row in history_rows]
            processed = await self.process_specific_calls(
                history_ids=batch_history_ids,
                recalculate=False  # Don't recalculate existing
            )
            
            total_processed += processed
            offset += batch_size
            
            logger.info(f"Backfill progress: {total_processed} calls processed")
            
            # Sleep to avoid overwhelming the database
            await asyncio.sleep(0.5)
        
        logger.info(f"Backfill completed: {total_processed} total calls processed")
        return total_processed


# Entry point for running worker manually or via cron
async def main():
    """Точка входа для запуска worker."""
    from app.config import DB_CONFIG
    
    db_manager = DatabaseManager(DB_CONFIG)
    await db_manager.initialize()
    
    worker = LMCalculatorWorker(db_manager)
    
    # Process recent calls (last 24 hours)
    await worker.process_recent_calls(hours_back=24)
    
    await db_manager.close()


if __name__ == "__main__":
    asyncio.run(main())
