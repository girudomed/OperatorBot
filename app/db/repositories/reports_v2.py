# Файл: app/db/repositories/reports_v2.py

"""
Репозиторий для работы с отчетами в таблице reports_v2.
"""

import json
from typing import Any, Dict, Optional

from app.db.manager import DatabaseManager
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)


class ReportsV2Repository:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    @staticmethod
    def _normalize_status(value: str) -> str:
        normalized = (value or "").strip().lower()
        if normalized == "ready":
            return "ready"
        if normalized in {"error", "failed", "failure", "empty"}:
            return "failed"
        logger.warning("reports_v2: неизвестный status=%r, используем 'failed'", value)
        return "failed"

    async def save_report(
        self,
        *,
        user_id: Optional[int],
        operator_key: str,
        operator_name: Optional[str],
        date_from,
        date_to,
        period_label: Optional[str],
        scoring_version: str,
        filters_json: Dict[str, Any],
        metrics_json: Dict[str, Any],
        report_text: str,
        cache_key: str,
        status: str,
        generated_at,
        error_text: Optional[str],
    ) -> bool:
        status = self._normalize_status(status)
        query = """
            INSERT INTO reports_v2 (
                user_id,
                operator_key,
                operator_name,
                date_from,
                date_to,
                period_label,
                scoring_version,
                filters_json,
                metrics_json,
                report_text,
                cache_key,
                status,
                generated_at,
                error_text
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) AS new
            ON DUPLICATE KEY UPDATE
                operator_name = new.operator_name,
                period_label = new.period_label,
                scoring_version = new.scoring_version,
                filters_json = new.filters_json,
                metrics_json = new.metrics_json,
                report_text = new.report_text,
                status = new.status,
                generated_at = new.generated_at,
                error_text = new.error_text
        """
        params = (
            user_id,
            operator_key,
            operator_name,
            date_from,
            date_to,
            period_label,
            scoring_version,
            json.dumps(filters_json, ensure_ascii=False),
            json.dumps(metrics_json, ensure_ascii=False),
            report_text,
            cache_key,
            status,
            generated_at,
            error_text,
        )
        try:
            await self.db_manager.execute_query(query, params)
            return True
        except Exception:
            logger.exception("reports_v2: ошибка сохранения")
            raise

    async def get_ready_report_by_cache_key(self, cache_key: str) -> Optional[Dict[str, Any]]:
        query = """
            SELECT *
            FROM reports_v2
            WHERE cache_key = %s
              AND status = 'ready'
            LIMIT 1
        """
        result = await self.db_manager.execute_query(query, (cache_key,), fetchone=True)
        return dict(result) if result else None
