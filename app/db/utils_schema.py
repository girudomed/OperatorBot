# Файл: app/db/utils_schema.py

"""
Утилиты для безопасной работы со схемой базы данных.

Позволяют проверять наличие колонок, чтобы строить совместимые запросы
даже при расхождениях между тестовой и продовой схемами.
"""

from __future__ import annotations

from typing import Dict, Optional

from app.logging_config import get_watchdog_logger

from app.config import DB_CONFIG
from app.db.manager import DatabaseManager

_COLUMN_EXISTS_CACHE: Dict[str, bool] = {}
logger = get_watchdog_logger(__name__)

REQUIRED_SCHEMA: Dict[str, tuple[str, ...]] = {
    "UsersTelegaBot": (
        "id",
        "user_id",
        "status",
        "role_id",
        "full_name",
        "username",
        "extension",
        "approved_by",
        "blocked_at",
        "operator_name",
    ),
    "roles_reference": (
        "role_id",
        "role_name",
        "can_manage_users",
    ),
    "call_history": (
        "history_id",
        "context_start_time_dt",
        "context_start_time",
        "caller_number",
        "called_number",
        "caller_info",
        "called_info",
        "talk_duration",
        "await_sec",
        "recording_id",
        "transcript",
    ),
    "call_scores": (
        "history_id",
        "call_score",
        "caller_number",
        "called_number",
        "caller_info",
        "called_info",
        "call_category",
        "call_date",
        "transcript",
        "outcome",
        "is_target",
    ),
    "users": (
        "user_id",
        "extension",
        "full_name",
    ),
}


def _col_cache_key(db_name: str, table: str, column: str) -> str:
    return f"{db_name}:{table}:{column}"


async def has_column(
    db_manager: DatabaseManager,
    table: str,
    column: str,
    db_name: Optional[str] = None,
) -> bool:
    """
    Проверяет наличие колонки в таблице через information_schema.
    Результаты кэшируются в памяти процесса, чтобы не нагружать БД.
    """
    database_name = db_name or DB_CONFIG.get("db")
    if not database_name:
        raise ValueError("Database name must be provided to check columns.")

    cache_key = _col_cache_key(database_name, table, column)
    if cache_key in _COLUMN_EXISTS_CACHE:
        return _COLUMN_EXISTS_CACHE[cache_key]

    query = """
        SELECT 1
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s
        LIMIT 1
    """
    rows = await db_manager.execute_with_retry(
        query,
        params=(database_name, table, column),
        fetchall=True,
    )
    exists = bool(rows)
    _COLUMN_EXISTS_CACHE[cache_key] = exists
    return exists


def clear_schema_cache() -> None:
    """Полностью очищает кэш проверок наличия колонок."""
    _COLUMN_EXISTS_CACHE.clear()


async def validate_schema(
    db_manager: DatabaseManager,
    *,
    schema: Optional[Dict[str, tuple[str, ...]]] = None,
    db_name: Optional[str] = None,
) -> None:
    """Проверяет, что необходимые таблицы и колонки существуют в БД."""
    schema_map = schema or REQUIRED_SCHEMA
    database_name = db_name or DB_CONFIG.get("db")
    if not database_name:
        raise RuntimeError("DATABASE name is not configured")

    missing: list[str] = []
    for table, columns in schema_map.items():
        for column in columns:
            exists = await has_column(db_manager, table, column, database_name)
            if not exists:
                missing.append(f"{table}.{column}")

    if missing:
        msg = (
            "Несовместимая схема базы данных. Отсутствуют столбцы: "
            + ", ".join(sorted(missing))
        )
        logger.error("[SCHEMA] %s", msg)
        raise RuntimeError(msg)

    logger.info(
        "[SCHEMA] Проверка схемы выполнена успешно (таблицы: %s)",
        ", ".join(sorted(schema_map.keys())),
    )
