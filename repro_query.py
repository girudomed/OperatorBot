import asyncio
import logging
from typing import List, Optional, Set
from decimal import Decimal

# Mocking parts of the app to test query building and logging
class MockDB:
    async def execute_with_retry(self, query, params=None, fetchall=False):
        print(f"DEBUG SQL: {query}")
        print(f"DEBUG PARAMS: {params}")
        # Logic to simulate DatabaseManager.execute_query logging
        query_preview = " ".join(query.split()) if isinstance(query, str) else str(query)
        print(f"LOG: [DB] Executing query: {query_preview} | params={params}")
        return []

def _build_call_scores_query(
    base_select: List[str],
    optional_columns: List[str],
    *,
    available_columns: Optional[Set[str]],
) -> str:
    select_parts = list(base_select)
    if available_columns is None:
        select_parts.extend([f"cs.{name}" for name in optional_columns])
    else:
        for name in optional_columns:
            if name in available_columns:
                select_parts.append(f"cs.{name}")
            else:
                select_parts.append(f"NULL AS {name}")
    select_clause = ",\n            ".join(select_parts)
    return f"""
    SELECT
        {select_clause}
    FROM mangoapi_db.call_scores cs
    WHERE
        cs.is_target = 1
        AND (cs.called_info = %s OR cs.caller_info = %s)
        AND cs.score_date BETWEEN %s AND %s
    """

async def test_repro():
    base_select = ["cs.id", "cs.history_id"]
    optional_columns = ["objection_present"]
    columns = {"id", "history_id", "objection_present"}
    
    query = _build_call_scores_query(base_select, optional_columns, available_columns=columns)
    db = MockDB()
    await db.execute_with_retry(query, ("ext", "ext", "2023-01-01", "2023-01-02"))

if __name__ == "__main__":
    asyncio.run(test_repro())
