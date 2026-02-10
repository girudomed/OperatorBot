from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telegram import InlineKeyboardMarkup
from telegram.error import BadRequest

from app.telegram.handlers.dashboard import DashboardHandler


def _make_handler() -> DashboardHandler:
    return DashboardHandler(db_manager=SimpleNamespace())


def test_dashboard_callback_payload_is_short_and_stable() -> None:
    handler = _make_handler()
    data = handler._build_callback("period_my", "month")
    assert data == "dash:period_my:month"
    assert len(data.encode("utf-8")) <= 64


def test_parse_legacy_dashboard_callback_with_operator_name() -> None:
    handler = _make_handler()
    callback_type, period = handler._parse_callback_data("dash_period_my_day_Очень_длинное_имя")
    assert callback_type == "period_my"
    assert period == "day"


@pytest.mark.asyncio
async def test_safe_answer_callback_stale_query_not_raised() -> None:
    handler = _make_handler()
    query = SimpleNamespace(
        answer=AsyncMock(
            side_effect=BadRequest("Query is too old and response timeout expired or query id is invalid")
        )
    )

    ok = await handler.safe_answer_callback(query)

    assert ok is False


@pytest.mark.asyncio
async def test_dashboard_callback_stale_query_short_circuit() -> None:
    handler = _make_handler()
    handler._rate_limit_callback = AsyncMock(return_value=False)  # type: ignore[method-assign]

    query = SimpleNamespace(
        data="dash:my:day",
        answer=AsyncMock(
            side_effect=BadRequest("Query is too old and response timeout expired or query id is invalid")
        ),
        edit_message_text=AsyncMock(),
    )
    update = SimpleNamespace(callback_query=query, effective_user=SimpleNamespace(id=101))
    context = SimpleNamespace(user_data={}, application=SimpleNamespace(bot_data={}))

    await handler.dashboard_callback(update, context)

    assert handler._rate_limit_callback.await_count == 0


@pytest.mark.asyncio
async def test_dashboard_command_does_not_embed_operator_name_in_callback() -> None:
    handler = _make_handler()
    long_name = "Оператор_" + ("ОченьДлинноеИмя" * 6)
    handler.user_repo.get_user_by_telegram_id = AsyncMock(  # type: ignore[attr-defined]
        return_value={"role_id": 1, "operator_name": long_name}
    )

    reply_text = AsyncMock()
    update = SimpleNamespace(
        effective_user=SimpleNamespace(id=42, full_name="Test User"),
        message=SimpleNamespace(reply_text=reply_text),
    )

    await handler.dashboard_command(update, SimpleNamespace())

    markup = reply_text.await_args.kwargs["reply_markup"]
    assert isinstance(markup, InlineKeyboardMarkup)
    all_callbacks = [btn.callback_data for row in markup.inline_keyboard for btn in row]
    assert all(len(cb.encode("utf-8")) <= 64 for cb in all_callbacks if cb)
    assert not any(long_name in cb for cb in all_callbacks if cb)
