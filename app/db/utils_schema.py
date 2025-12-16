# Файл: app/db/utils_schema.py

"""
Утилиты для безопасной работы со схемой базы данных.

Позволяют проверять наличие колонок, чтобы строить совместимые запросы
даже при расхождениях между тестовой и продовой схемами.
"""

from __future__ import annotations

from typing import Dict, Optional

from app.config import DB_CONFIG
from app.db.manager import DatabaseManager

_COLUMN_EXISTS_CACHE: Dict[str, bool] = {}


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
