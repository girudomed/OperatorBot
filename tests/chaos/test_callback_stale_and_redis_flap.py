from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telegram.error import BadRequest

from app.telegram.handlers.admin_panel import AdminPanelHandler
from app.telegram.handlers.dashboard import DashboardHandler
from app.telegram.utils.callback_data import AdminCB


@pytest.mark.asyncio
async def test_dashboard_stale_callbacks_do_not_crash_under_chaos() -> None:
    handler = DashboardHandler(db_manager=SimpleNamespace())

    async def run_one(i: int) -> bool:
        if i % 3 == 0:
            query = SimpleNamespace(
                answer=AsyncMock(
                    side_effect=BadRequest("Query is too old and response timeout expired or query id is invalid")
                )
            )
            return await handler.safe_answer_callback(query)
        query = SimpleNamespace(answer=AsyncMock())
        return await handler.safe_answer_callback(query)

    results = await asyncio.gather(*(run_one(i) for i in range(300)))

    assert sum(1 for ok in results if not ok) == 100
    assert sum(1 for ok in results if ok) == 200


@pytest.mark.asyncio
async def test_admin_panel_hash_miss_redis_flap_no_crash(monkeypatch) -> None:
    handler = AdminPanelHandler(
        admin_repo=SimpleNamespace(),
        permissions=SimpleNamespace(
            can_access_admin_panel=AsyncMock(return_value=True),
        ),
    )
    handler._safe_answer = AsyncMock(return_value=True)  # type: ignore[method-assign]
    handler._show_main_menu = AsyncMock()  # type: ignore[method-assign]
    handler._handle_new_callback = AsyncMock(return_value=True)  # type: ignore[method-assign]

    async def resolve_hash_async(_digest: str):
        # Имитируем флап: часть digest не резолвится (например, Redis timeout/miss).
        if _digest.endswith("0") or _digest.endswith("5"):
            return None
        return "adm:back"

    monkeypatch.setattr(AdminCB, "resolve_hash_async", resolve_hash_async)

    async def run_one(i: int) -> None:
        digest = f"deadbeefcafebab{i:02d}"[-16:]
        query = SimpleNamespace(data=f"adm:hd:{digest}", answer=AsyncMock())
        update = SimpleNamespace(callback_query=query, effective_user=SimpleNamespace(id=i + 1, username="u"))
        context = SimpleNamespace(application=SimpleNamespace(bot_data={}))
        await handler.handle_callback(update, context)

    await asyncio.gather(*(run_one(i) for i in range(100)))

    # Для части запросов меню перерисовывается после hash miss; для остальных callback отрабатывает штатно.
    assert handler._show_main_menu.await_count > 0
    assert handler._handle_new_callback.await_count > 0
