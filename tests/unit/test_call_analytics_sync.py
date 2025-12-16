import pytest
from datetime import date
from unittest.mock import AsyncMock

from app.services.call_analytics_sync import CallAnalyticsSyncService


@pytest.mark.asyncio
async def test_sync_new_uses_call_history_created_at():
    """Инкрементальное обновление должно выбирать данные из call_history."""
    fake_db = AsyncMock()
    fake_db.execute_with_retry = AsyncMock(return_value=0)

    service = CallAnalyticsSyncService(fake_db)
    service._schema_checked = True
    service._schema_valid = True

    await service.sync_new(since_date=date(2025, 1, 1), batch_size=100)

    assert fake_db.execute_with_retry.await_count == 1
    query = fake_db.execute_with_retry.await_args.kwargs.get("query") or fake_db.execute_with_retry.await_args.args[0]
    assert "call_history ch" in query
    assert "ch.created_at" in query
    assert "cs.history_id" in query
