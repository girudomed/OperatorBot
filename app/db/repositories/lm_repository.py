"""
Репозиторий для работы с LM метриками.
"""

from typing import Any, Dict, List, Optional
from datetime import datetime
import json

from app.db.manager import DatabaseManager
from app.db.models import LMValueRecord
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)


class LMRepository:
    """Репозиторий для работы с таблицей lm_value."""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    async def save_lm_value(
        self,
        history_id: int,
        metric_code: str,
        metric_group: str,
        lm_version: str,
        calc_method: str,
        value_numeric: Optional[float] = None,
        value_label: Optional[str] = None,
        value_json: Optional[Dict[str, Any]] = None,
        call_score_id: Optional[int] = None,
        calc_source: Optional[str] = None,
    ) -> int:
        """
        Сохраняет одно значение метрики LM.
        
        Args:
            history_id: ID звонка из call_history
            metric_code: Код метрики (например, 'conversion_score')
            metric_group: Группа метрики ('operational', 'conversion', 'quality', 'risk', 'forecast', 'aux')
            lm_version: Версия LM (например, 'lm_v1')
            calc_method: Метод расчета ('rule', 'tree', 'gbm', etc.)
            value_numeric: Числовое значение метрики
            value_label: Текстовое значение метрики
            value_json: JSON значение метрики
            call_score_id: ID из call_scores (опционально)
            calc_source: Источник расчета (опционально)
            
        Returns:
            ID созданной или обновленной записи
        """
        # Validate: at least one value must be provided
        if value_numeric is None and value_label is None and value_json is None:
            raise ValueError(f"At least one value must be provided for metric {metric_code}")
        
        # Convert value_json to JSON string if provided
        value_json_str = json.dumps(value_json) if value_json else None
        
        query = """
        INSERT INTO lm_value (
            history_id, call_score_id, metric_code, metric_group,
            value_numeric, value_label, value_json,
            lm_version, calc_method, calc_source
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            call_score_id = VALUES(call_score_id),
            value_numeric = VALUES(value_numeric),
            value_label = VALUES(value_label),
            value_json = VALUES(value_json),
            lm_version = VALUES(lm_version),
            calc_method = VALUES(calc_method),
            calc_source = VALUES(calc_source),
            updated_at = CURRENT_TIMESTAMP
        """
        
        params = (
            history_id, call_score_id, metric_code, metric_group,
            value_numeric, value_label, value_json_str,
            lm_version, calc_method, calc_source
        )
        
        result = await self.db_manager.execute_with_retry(query, params)
        
        # Get the inserted/updated ID
        if result:
            # Fetch the record to get ID
            fetch_query = """
            SELECT id FROM lm_value 
            WHERE history_id = %s AND metric_code = %s
            """
            row = await self.db_manager.execute_with_retry(
                fetch_query, 
                (history_id, metric_code), 
                fetchone=True
            )
            if row:
                logger.debug(f"Saved LM value: {metric_code} for history_id={history_id}")
                return row['id']
        
        raise RuntimeError(f"Failed to save LM value for {metric_code}")

    async def save_lm_values_batch(
        self,
        values: List[Dict[str, Any]]
    ) -> int:
        """
        Пакетное сохранение метрик LM.
        
        Args:
            values: Список словарей с полями для save_lm_value
            
        Returns:
            Количество сохраненных записей
        """
        if not values:
            return 0
        
        saved_count = 0
        for value_data in values:
            try:
                await self.save_lm_value(**value_data)
                saved_count += 1
            except Exception as e:
                logger.error(f"Failed to save LM value: {e}", exc_info=True)
        
        logger.info(f"Saved {saved_count}/{len(values)} LM values")
        return saved_count
    
    async def get_aggregated_metrics(
        self,
        metric_group: str,
        days: int = 7
    ) -> Dict[str, Dict[str, float]]:
        """
        Получает агрегированные метрики по группе за последние N дней.
        
        Returns:
            {metric_code: {avg: float, min: float, max: float, count: int}}
        """
        from datetime import datetime, timedelta
        
        start_date = datetime.now() - timedelta(days=days)
        
        query = """
            SELECT 
                metric_code,
                AVG(value_numeric) as avg_value,
                MIN(value_numeric) as min_value,
                MAX(value_numeric) as max_value,
                COUNT(*) as count_value
            FROM lm_value
            WHERE metric_group = %s
            AND created_at >= %s
            AND value_numeric IS NOT NULL
            GROUP BY metric_code
        """
        
        rows = await self.db_manager.execute_with_retry(
            query,
            (metric_group, start_date),
            fetchall=True
        ) or []
        
        result = {}
        for row in rows:
            result[row['metric_code']] = {
                'avg': float(row['avg_value'] or 0),
                'min': float(row['min_value'] or 0),
                'max': float(row['max_value'] or 0),
                'count': int(row['count_value'] or 0)
            }
        
        return result
    
    async def get_risk_summary(self, days: int = 7) -> Dict[str, int]:
        """
        Получает сводку по метрикам рисков.
        
        Returns:
            {
                'churn_risk_high': int,
                'churn_risk_medium': int,
                'complaint_risk_count': int,
                'followup_needed_count': int
            }
        """
        from datetime import datetime, timedelta
        
        start_date = datetime.now() - timedelta(days=days)
        
        # Churn risk levels
        churn_query = """
            SELECT value_label, COUNT(*) as count
            FROM lm_value
            WHERE metric_code = 'churn_risk_level'
            AND created_at >= %s
            GROUP BY value_label
        """
        
        churn_rows = await self.db_manager.execute_with_retry(
            churn_query,
            (start_date,),
            fetchall=True
        ) or []
        
        churn_counts = {row['value_label']: int(row['count']) for row in churn_rows}
        
        # Complaint risk (flag true)
        complaint_query = """
            SELECT COUNT(*) as count
            FROM lm_value
            WHERE metric_code = 'complaint_risk_flag'
            AND value_label = 'true'
            AND created_at >= %s
        """
        
        complaint_row = await self.db_manager.execute_with_retry(
            complaint_query,
            (start_date,),
            fetchone=True
        )
        
        # Followup needed
        followup_query = """
            SELECT COUNT(*) as count
            FROM lm_value
            WHERE metric_code = 'followup_needed_flag'
            AND value_label = 'true'
            AND created_at >= %s
        """
        
        followup_row = await self.db_manager.execute_with_retry(
            followup_query,
            (start_date,),
            fetchone=True
        )
        
        return {
            'churn_risk_high': churn_counts.get('high', 0),
            'churn_risk_medium': churn_counts.get('medium', 0),
            'churn_risk_low': churn_counts.get('low', 0),
            'complaint_risk_count': int(complaint_row['count'] if complaint_row else 0),
            'followup_needed_count': int(followup_row['count'] if followup_row else 0)
        }

    async def get_lm_values_by_call(
        self,
        history_id: int,
        metric_group: Optional[str] = None
    ) -> List[LMValueRecord]:
        """
        Получает все метрики LM для конкретного звонка.
        
        Args:
            history_id: ID звонка из call_history
            metric_group: Фильтр по группе метрик (опционально)
            
        Returns:
            Список записей LMValueRecord
        """
        query = "SELECT * FROM lm_value WHERE history_id = %s"
        params = [history_id]
        
        if metric_group:
            query += " AND metric_group = %s"
            params.append(metric_group)
        
        query += " ORDER BY metric_group, metric_code"
        
        rows = await self.db_manager.execute_with_retry(
            query, 
            tuple(params), 
            fetchall=True
        )
        
        # Convert JSON strings back to dicts
        result = []
        for row in (rows or []):
            if row.get('value_json'):
                try:
                    row['value_json'] = json.loads(row['value_json'])
                except (json.JSONDecodeError, TypeError):
                    logger.warning(f"Failed to parse value_json for lm_value.id={row.get('id')}")
                    row['value_json'] = None
            result.append(row)
        
        return result

    async def get_lm_values_by_metric(
        self,
        metric_code: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 1000
    ) -> List[LMValueRecord]:
        """
        Получает все значения конкретной метрики за период.
        
        Args:
            metric_code: Код метрики
            start_date: Начало периода (опционально)
            end_date: Конец периода (опционально)
            limit: Максимальное количество записей
            
        Returns:
            Список записей LMValueRecord
        """
        query = "SELECT * FROM lm_value WHERE metric_code = %s"
        params = [metric_code]
        
        if start_date:
            query += " AND created_at >= %s"
            params.append(start_date)
        
        if end_date:
            query += " AND created_at <= %s"
            params.append(end_date)
        
        query += " ORDER BY created_at DESC LIMIT %s"
        params.append(limit)
        
        rows = await self.db_manager.execute_with_retry(
            query, 
            tuple(params), 
            fetchall=True
        )
        
        # Convert JSON strings back to dicts
        result = []
        for row in (rows or []):
            if row.get('value_json'):
                try:
                    row['value_json'] = json.loads(row['value_json'])
                except (json.JSONDecodeError, TypeError):
                    logger.warning(f"Failed to parse value_json for lm_value.id={row.get('id')}")
                    row['value_json'] = None
            result.append(row)
        
        return result

    async def get_aggregated_metrics(
        self,
        metric_codes: List[str],
        start_date: datetime,
        end_date: datetime,
        group_by: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Получает агрегированные метрики за период.
        
        Args:
            metric_codes: Список кодов метрик для агрегации
            start_date: Начало периода
            end_date: Конец периода
            group_by: Поле для группировки (опционально, например 'metric_code')
            
        Returns:
            Список агрегированных значений
        """
        if not metric_codes:
            return []
        
        # Build placeholders for metric_codes
        placeholders = ', '.join(['%s'] * len(metric_codes))
        
        select_fields = []
        if group_by:
            select_fields.append(f"{group_by}")
        
        select_fields.extend([
            "COUNT(*) as count",
            "AVG(value_numeric) as avg_value",
            "MIN(value_numeric) as min_value",
            "MAX(value_numeric) as max_value",
            "STDDEV(value_numeric) as stddev_value"
        ])
        
        query = f"""
        SELECT {', '.join(select_fields)}
        FROM lm_value
        WHERE metric_code IN ({placeholders})
        AND created_at BETWEEN %s AND %s
        """
        
        if group_by:
            query += f" GROUP BY {group_by}"
        
        params = tuple(metric_codes) + (start_date, end_date)
        
        rows = await self.db_manager.execute_with_retry(
            query, 
            params, 
            fetchall=True
        )
        
        return rows or []

    async def delete_lm_values_by_call(self, history_id: int) -> int:
        """
        Удаляет все метрики LM для конкретного звонка.
        
        Args:
            history_id: ID звонка из call_history
            
        Returns:
            Количество удаленных записей
        """
        query = "DELETE FROM lm_value WHERE history_id = %s"
        
        result = await self.db_manager.execute_with_retry(query, (history_id,))
        
        logger.info(f"Deleted LM values for history_id={history_id}")
        return result if result else 0

    async def get_metric_statistics(
        self,
        metric_code: str,
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, Any]:
        """
        Получает статистику по конкретной метрике за период.
        
        Args:
            metric_code: Код метрики
            start_date: Начало периода
            end_date: Конец периода
            
        Returns:
            Словарь со статистикой (count, avg, min, max, stddev, percentiles)
        """
        query = """
        SELECT 
            COUNT(*) as count,
            AVG(value_numeric) as avg_value,
            MIN(value_numeric) as min_value,
            MAX(value_numeric) as max_value,
            STDDEV(value_numeric) as stddev_value
        FROM lm_value
        WHERE metric_code = %s
        AND created_at BETWEEN %s AND %s
        AND value_numeric IS NOT NULL
        """
        
        stats = await self.db_manager.execute_with_retry(
            query,
            (metric_code, start_date, end_date),
            fetchone=True
        )
        
        return stats or {}

    async def get_followup_calls(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Получает звонки, требующие фоллоу-ап.
        
        Returns:
            Список звонков с флагом followup_needed=true
        """
        from datetime import datetime, timedelta
        
        start_date = datetime.now() - timedelta(days=7)
        
        query = """
            SELECT DISTINCT
                lv.history_id,
                lv.created_at,
                (SELECT value_label FROM lm_value 
                 WHERE history_id = lv.history_id 
                 AND metric_code = 'churn_risk_level' 
                 LIMIT 1) as churn_risk_level
            FROM lm_value lv
            WHERE lv.metric_code = 'followup_needed_flag'
            AND lv.value_label = 'true'
            AND lv.created_at >= %s
            ORDER BY lv.created_at DESC
            LIMIT %s
        """
        
        rows = await self.db_manager.execute_with_retry(
            query,
            (start_date, limit),
            fetchall=True
        ) or []
        
        return [dict(row) for row in rows]

