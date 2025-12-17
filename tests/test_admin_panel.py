import pytest
from unittest.mock import AsyncMock

from app.telegram.handlers.admin_panel import AdminPanelHandler
from app.telegram.utils.callback_data import AdminCB


class _StubRepo:
    async def get_users_counters(self):
        return {}


class _StubPermissions:
    async def has_permission(self, *_, **__):
        return True


class _DummyQuery:
    def __init__(self, data: str):
        self.data = data
        self.from_user = type("DummyUser", (), {"id": 1, "username": "tester"})()
        self.message = None
        self._answered = False

    async def answer(self, *_, **__):
        self._answered = True


class _DummyUpdate:
    def __init__(self, data: str):
        self.callback_query = _DummyQuery(data)
        self.effective_user = self.callback_query.from_user


class _DummyContext:
    class _App:
        bot_data = {}

    application = _App()


@pytest.mark.asyncio
async def test_handle_callback_routes_new_dashboard(monkeypatch):
    handler = AdminPanelHandler(_StubRepo(), _StubPermissions())
    mock_dashboard = AsyncMock()
    monkeypatch.setattr(handler, "_show_dashboard", mock_dashboard)

    update = _DummyUpdate(AdminCB.create(AdminCB.DASHBOARD))
    context = _DummyContext()

    await handler.handle_callback(update, context)

    mock_dashboard.assert_awaited_once_with(update, context)


@pytest.mark.asyncio
async def test_handle_callback_routes_commands_menu(monkeypatch):
    handler = AdminPanelHandler(_StubRepo(), _StubPermissions())
    mock_commands = AsyncMock()
    monkeypatch.setattr(handler, "_show_command_shortcuts", mock_commands)

    update = _DummyUpdate(AdminCB.create(AdminCB.COMMANDS))
    context = _DummyContext()

    await handler.handle_callback(update, context)

    mock_commands.assert_awaited_once_with(update, context)


@pytest.mark.asyncio
async def test_handle_callback_unknown_fallback(monkeypatch):
    handler = AdminPanelHandler(_StubRepo(), _StubPermissions())
    mock_unknown = AsyncMock()
    monkeypatch.setattr(handler, "_handle_unknown_callback", mock_unknown)

    update = _DummyUpdate("adm:unknown:data")
    context = _DummyContext()

    await handler.handle_callback(update, context)

    mock_unknown.assert_awaited_once_with(update.callback_query)


@pytest.mark.asyncio
async def test_handle_callback_passes_command_payload(monkeypatch):
    handler = AdminPanelHandler(_StubRepo(), _StubPermissions())
    mock_command = AsyncMock()
    monkeypatch.setattr(handler, "_handle_command_action", mock_command)

    payload = AdminCB.create(AdminCB.COMMAND, "set_role_page", "2")
    update = _DummyUpdate(payload)
    context = _DummyContext()

    await handler.handle_callback(update, context)

    mock_command.assert_awaited_once()
    call_args = mock_command.await_args
    assert call_args.args[0] == "set_role_page"
    assert call_args.args[1] == "2"
