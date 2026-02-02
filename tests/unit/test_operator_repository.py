import pytest
from datetime import datetime, timedelta

from app.db.repositories.operators import OperatorRepository


class _DummyDBManager:
    """Минимальный мок db_manager для проверки SQL-форматирования."""

    def __init__(self):
        self.calls = []

    async def execute_with_retry(
        self,
        query,
        params=None,
        fetchone=False,
        fetchall=False,
    ):
        self._validate_query_interpolation(query, params)
        self.calls.append((query, params, fetchone, fetchall))
        if fetchone:
            return {}
        if fetchall:
            return []
        return None

    def _validate_query_interpolation(self, query: str, params):
        placeholder_count = query.count("%s")
        if not placeholder_count:
            return
        normalized = self._normalize_params(params, placeholder_count)
        try:
            query % normalized
        except Exception as exc:  # pragma: no cover - перехватываем ValueError
            raise AssertionError(
                "SQL содержит неэкранированные проценты или неверные плейсхолдеры"
            ) from exc

    @staticmethod
    def _normalize_params(params, placeholder_count: int):
        if params is None:
            values = tuple(None for _ in range(placeholder_count))
        elif isinstance(params, tuple):
            values = params
        elif isinstance(params, list):
            values = tuple(params)
        else:
            values = (params,)
        if len(values) < placeholder_count:
            values = values + tuple(None for _ in range(placeholder_count - len(values)))
        elif len(values) > placeholder_count:
            values = values[:placeholder_count]
        return values


@pytest.mark.asyncio
async def test_get_quality_summary_queries_are_safe():
    db = _DummyDBManager()
    repo = OperatorRepository(db)
    start = datetime(2024, 1, 1)
    end = start + timedelta(days=7)

    result = await repo.get_quality_summary(start, end)

    assert result["total_calls"] == 0
    assert len(db.calls) == 2


def test_build_call_scores_query_adds_null_for_missing_columns():
    base_select = ["cs.id", "cs.history_id"]
    optional_columns = ["objection_present", "booking_attempted"]
    query = OperatorRepository._build_call_scores_query(
        base_select,
        optional_columns,
        available_columns={"objection_present"},
    )
    assert "cs.objection_present" in query
    assert "NULL AS booking_attempted" in query


def test_build_call_scores_query_keeps_optional_when_unknown():
    base_select = ["cs.id"]
    optional_columns = ["objection_present"]
    query = OperatorRepository._build_call_scores_query(
        base_select,
        optional_columns,
        available_columns=None,
    )
    assert "cs.objection_present" in query
