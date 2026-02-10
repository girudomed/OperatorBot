from types import SimpleNamespace

import pytest

from app import main
from app.errors import AppError


class DummyUpdate:
    def __init__(self, update_id: int):
        self.update_id = update_id
        self.effective_user = SimpleNamespace(id=1, username="u")
        self.effective_chat = SimpleNamespace(id=2)
        self.message = None
        self.inline_query = None

        self.callback_answer_calls = 0

        class _Msg:
            async def reply_text(self, *_args, **_kwargs):
                return None

        class _Cb:
            def __init__(self, owner):
                self.owner = owner
                self.message = _Msg()

            async def answer(self, *_args, **_kwargs):
                self.owner.callback_answer_calls += 1

        self.callback_query = _Cb(self)
        self.effective_message = self.callback_query.message


@pytest.mark.asyncio
async def test_one_failure_one_notification_and_one_stacktrace(monkeypatch):
    monkeypatch.setattr(main, "Update", DummyUpdate)

    error_calls = []
    monkeypatch.setattr(main.logger, "error", lambda msg, *args, **kwargs: error_calls.append((msg, kwargs)))
    monkeypatch.setattr(main.logger, "warning", lambda *args, **kwargs: None)
    monkeypatch.setattr(main.logger, "debug", lambda *args, **kwargs: None)

    update = DummyUpdate(update_id=777)
    error = AppError("boom", user_message="oops", alert=False)
    ctx = SimpleNamespace(
        error=error,
        application=SimpleNamespace(
            bot_data={},
            bot=SimpleNamespace(send_message=lambda *args, **kwargs: None),
        ),
    )

    await main.telegram_error_handler(update, ctx)
    await main.telegram_error_handler(update, ctx)

    assert len(error_calls) == 1
    assert update.callback_answer_calls == 1
