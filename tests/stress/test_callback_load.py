from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.telegram.handlers.dashboard import DashboardHandler


@pytest.mark.asyncio
async def test_dashboard_callback_under_load() -> None:
    handler = DashboardHandler(db_manager=SimpleNamespace())
    handler._rate_limit_callback = AsyncMock(return_value=False)  # type: ignore[method-assign]
    handler._acquire_guard = AsyncMock(return_value=True)  # type: ignore[method-assign]
    handler._release_guard = lambda *_args, **_kwargs: None  # type: ignore[method-assign]
    handler._resolve_operator_name = AsyncMock(return_value="operator_1")  # type: ignore[method-assign]
    handler._show_single_dashboard = AsyncMock()  # type: ignore[method-assign]
    handler._show_all_operators_dashboard = AsyncMock()  # type: ignore[method-assign]

    semaphore = asyncio.Semaphore(100)

    async def run_one(i: int) -> None:
        async with semaphore:
            data = "dash:my:day" if i % 2 == 0 else "dash:all:day"
            query = SimpleNamespace(
                data=data,
                answer=AsyncMock(),
                edit_message_text=AsyncMock(),
            )
            update = SimpleNamespace(
                callback_query=query,
                effective_user=SimpleNamespace(id=i + 1),
            )
            context = SimpleNamespace(user_data={}, application=SimpleNamespace(bot_data={}))
            await handler.dashboard_callback(update, context)

    await asyncio.gather(*(run_one(i) for i in range(1000)))

    assert handler._show_single_dashboard.await_count == 500
    assert handler._show_all_operators_dashboard.await_count == 500
