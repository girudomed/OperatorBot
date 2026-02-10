import pytest

import app.utils.best_effort as be


@pytest.mark.asyncio
async def test_best_effort_async_logs_error_event(monkeypatch):
    calls = []

    monkeypatch.setattr(be.logger, "warning", lambda msg, *args, **kwargs: calls.append((msg, kwargs)))

    async def fail():
        raise RuntimeError("oops")

    result = await be.best_effort_async("op", fail(), on_error_result=None)
    assert result.status == "error"
    assert calls
    assert calls[0][1]["extra"]["event"] == "best_effort"
    assert calls[0][1]["extra"]["status"] == "error"


def test_best_effort_sync_logs_success_event(monkeypatch):
    calls = []

    monkeypatch.setattr(be.logger, "info", lambda msg, *args, **kwargs: calls.append((msg, kwargs)))

    result = be.best_effort_sync("op", lambda: 42)
    assert result.status == "success"
    assert result.value == 42
    assert calls[0][1]["extra"]["event"] == "best_effort"
    assert calls[0][1]["extra"]["status"] == "success"
