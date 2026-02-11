from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.telegram.handlers.admin_panel import AdminPanelHandler
from app.telegram.utils.callback_data import AdminCB


def _make_handler() -> AdminPanelHandler:
    permissions = SimpleNamespace(
        can_access_admin_panel=AsyncMock(return_value=True),
    )
    return AdminPanelHandler(admin_repo=SimpleNamespace(), permissions=permissions)


@pytest.mark.asyncio
async def test_admin_callback_uses_async_hash_resolve_only(monkeypatch) -> None:
    handler = _make_handler()
    handler._safe_answer = AsyncMock(return_value=True)  # type: ignore[method-assign]
    handler._handle_new_callback = AsyncMock(return_value=True)  # type: ignore[method-assign]
    handler._handle_unknown_callback = AsyncMock()  # type: ignore[method-assign]

    async_resolve = AsyncMock(return_value="adm:back")
    monkeypatch.setattr(AdminCB, "resolve_hash_async", async_resolve)

    def _forbidden_sync_resolve(_digest: str):
        raise AssertionError("sync resolve_hash must not be used in callback path")

    monkeypatch.setattr(AdminCB, "resolve_hash", _forbidden_sync_resolve)

    query = SimpleNamespace(data="adm:hd:deadbeefcafebabe", answer=AsyncMock())
    update = SimpleNamespace(callback_query=query, effective_user=SimpleNamespace(id=10, username="u"))
    context = SimpleNamespace(application=SimpleNamespace(bot_data={}))

    await handler.handle_callback(update, context)

    assert async_resolve.await_count == 1
    assert handler._handle_new_callback.await_count == 1
    assert handler._handle_unknown_callback.await_count == 0


@pytest.mark.asyncio
async def test_admin_callback_hash_miss_requests_menu_refresh(monkeypatch) -> None:
    handler = _make_handler()
    handler._safe_answer = AsyncMock(return_value=True)  # type: ignore[method-assign]
    handler._show_main_menu = AsyncMock()  # type: ignore[method-assign]
    handler._handle_unknown_callback = AsyncMock()  # type: ignore[method-assign]

    monkeypatch.setattr(AdminCB, "resolve_hash_async", AsyncMock(return_value=None))

    query = SimpleNamespace(data="adm:hd:deadbeefcafebabe", answer=AsyncMock())
    update = SimpleNamespace(callback_query=query, effective_user=SimpleNamespace(id=10, username="u"))
    context = SimpleNamespace(application=SimpleNamespace(bot_data={}))

    await handler.handle_callback(update, context)

    assert handler._show_main_menu.await_count == 1
    assert handler._handle_unknown_callback.await_count == 0
    assert handler._safe_answer.await_count == 2
    text = handler._safe_answer.await_args_list[1].args[1]
    assert "Клавиатура устарела" in text
