"""
Репозиторий для словарей и срабатываний LM rule-engine.
"""

from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta

from app.db.manager import DatabaseManager
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)


class LMDictionaryRepository:
    """Работа с lm_dictionary_terms и lm_dictionary_hits."""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    async def get_terms(
        self,
        dict_code: str,
        version: str = "v1",
        *,
        active_only: bool = True,
    ) -> List[Dict[str, Any]]:
        """Возвращает активные термины для словаря."""
        query = """
            SELECT
                id,
                dict_code,
                term,
                match_type,
                weight,
                is_negative,
                is_active,
                comment,
                version
            FROM lm_dictionary_terms
            WHERE dict_code = %s
              AND version = %s
        """
        params: List[Any] = [dict_code, version]
        if active_only:
            query += " AND is_active = TRUE"

        query += " ORDER BY weight DESC, term ASC"

        rows = await self.db_manager.execute_with_retry(
            query,
            tuple(params),
            fetchall=True,
            query_name="lm_dictionary.get_terms",
        ) or []
        return [dict(row) for row in rows]

    async def save_hits(
        self,
        history_id: int,
        dict_code: str,
        hits: List[Dict[str, Any]],
        dict_version: str,
    ) -> None:
        """Сохраняет факты срабатывания словаря для звонка."""
        if not hits:
            return

        insert_query = """
            INSERT INTO lm_dictionary_hits (
                history_id,
                dict_code,
                term,
                match_type,
                weight,
                hit_count,
                snippet,
                dict_version,
                detected_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """

        for hit in hits:
            if not hit.get("term"):
                continue
            detected_raw = hit.get("detected_at")
            if isinstance(detected_raw, str):
                try:
                    detected_at = datetime.fromisoformat(detected_raw)
                except ValueError:
                    detected_at = datetime.utcnow()
            else:
                detected_at = detected_raw or datetime.utcnow()

            params = (
                history_id,
                dict_code,
                hit.get("term"),
                hit.get("match_type", "phrase"),
                int(hit.get("weight") or 0),
                int(hit.get("hit_count") or 1),
                hit.get("snippet"),
                dict_version,
                detected_at,
            )
            try:
                await self.db_manager.execute_with_retry(
                    insert_query,
                    params,
                    commit=True,
                    query_name="lm_dictionary.save_hit",
                )
            except Exception:
                logger.exception(
                    "Не удалось сохранить словарный хит history_id=%s term=%s",
                    history_id,
                    hit.get("term"),
                )

    async def get_recent_hits(
        self,
        dict_code: str,
        days: int = 7,
        *,
        limit: int = 20000,
    ) -> List[Dict[str, Any]]:
        """Возвращает свежие срабатывания словаря для анализа весов."""
        if days <= 0:
            days = 1
        cutoff = datetime.utcnow() - timedelta(days=days)
        query = (
            "SELECT history_id, dict_code, term, match_type, weight, hit_count, "
            "snippet, dict_version, detected_at "
            "FROM lm_dictionary_hits "
            "WHERE dict_code = %s AND detected_at >= %s "
            "ORDER BY detected_at DESC "
            "LIMIT %s"
        )
        rows = await self.db_manager.execute_with_retry(
            query,
            (dict_code, cutoff, limit),
            fetchall=True,
            query_name="lm_dictionary.get_recent_hits",
        ) or []
        return [dict(row) for row in rows]
