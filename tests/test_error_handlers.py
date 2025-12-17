import pytest
from unittest.mock import AsyncMock

from app.db.repositories.admin import AdminRepository
from app.db.repositories.lm_repository import LMRepository


class _DummyDB:
    def __init__(self):
        self.execute_with_retry = AsyncMock(side_effect=Exception("db failure"))


class _StopAsyncDB:
    def __init__(self):
        self.execute_with_retry = AsyncMock(side_effect=StopAsyncIteration)


@pytest.mark.asyncio
async def test_get_internal_id_handles_cursor_exhaustion():
    repo = AdminRepository(_StopAsyncDB())
    assert await repo._get_internal_id(telegram_id=123) is None


@pytest.mark.asyncio
async def test_get_internal_id_raises_on_unexpected_db_error():
    repo = AdminRepository(_DummyDB())
    with pytest.raises(RuntimeError):
        await repo._get_internal_id(telegram_id=123)


@pytest.mark.asyncio
async def test_save_lm_values_batch_swallows_db_error():
    repo = LMRepository(_DummyDB())
    saved = await repo.save_lm_values_batch(
        [
            {
                "history_id": 1,
                "metric_code": "test",
                "metric_group": "operational",
                "lm_version": "v1",
                "calc_method": "rule",
                "value_numeric": 1.0,
            }
        ]
    )
    assert saved == 0


@pytest.mark.asyncio
async def test_save_lm_values_batch_skips_invalid_payload(monkeypatch):
    repo = LMRepository(_StopAsyncDB())  # DB не используется благодаря patch
    repo.save_lm_value = AsyncMock(
        side_effect=[TypeError("missing history_id"), None]
    )

    saved = await repo.save_lm_values_batch(
        [
            {
                "metric_code": "test",
            },
            {
                "history_id": 2,
                "metric_code": "ok",
                "metric_group": "operational",
                "lm_version": "v1",
                "calc_method": "rule",
                "value_numeric": 1.0,
            },
        ]
    )
    assert saved == 1
    assert repo.save_lm_value.call_count == 2
