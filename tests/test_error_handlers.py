import pytest
from unittest.mock import AsyncMock

from app.db.repositories.admin import AdminRepository
from app.db.repositories.lm_repository import LMRepository


class _DummyDB:
    def __init__(self):
        self.execute_with_retry = AsyncMock(side_effect=Exception("db failure"))


@pytest.mark.asyncio
async def test_get_internal_id_handles_db_failure():
    repo = AdminRepository(_DummyDB())
    internal_id = await repo._get_internal_id(telegram_id=123)
    assert internal_id is None


@pytest.mark.asyncio
async def test_save_lm_values_batch_handles_insert_errors():
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
