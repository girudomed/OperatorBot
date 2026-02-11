from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.telegram.handlers.admin_admins import AdminAdminsHandler
from app.telegram.utils.callback_data import AdminCB


@pytest.mark.asyncio
async def test_admin_admins_denies_without_manage_users_permission() -> None:
    handler = AdminAdminsHandler(
        admin_repo=SimpleNamespace(),
        permissions=SimpleNamespace(can_manage_users=AsyncMock(return_value=False)),
        notifications=SimpleNamespace(),
    )
    query = SimpleNamespace(
        data=AdminCB.create(AdminCB.ADMINS, AdminCB.LIST, 0),
        answer=AsyncMock(),
    )
    update = SimpleNamespace(
        callback_query=query,
        effective_user=SimpleNamespace(id=101, username="operator"),
    )
    context = SimpleNamespace()

    await handler.handle_callback(update, context)

    assert query.answer.await_count == 1
    _, kwargs = query.answer.await_args
    assert kwargs.get("show_alert") is True


@pytest.mark.asyncio
async def test_admin_admins_routes_when_permission_granted(monkeypatch) -> None:
    handler = AdminAdminsHandler(
        admin_repo=SimpleNamespace(),
        permissions=SimpleNamespace(can_manage_users=AsyncMock(return_value=True)),
        notifications=SimpleNamespace(),
    )
    routed = AsyncMock()
    monkeypatch.setattr(handler, "show_admins_list", routed)
    query = SimpleNamespace(
        data=AdminCB.create(AdminCB.ADMINS, AdminCB.LIST, 0),
        answer=AsyncMock(),
    )
    update = SimpleNamespace(
        callback_query=query,
        effective_user=SimpleNamespace(id=55, username="admin"),
    )
    context = SimpleNamespace()

    await handler.handle_callback(update, context)

    routed.assert_awaited_once_with(update, context)
