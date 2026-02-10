from __future__ import annotations

import logging

from app import main
from app.logging_config import install_polling_noise_filter, is_polling_noise_record
import httpx
from telegram.error import NetworkError, TelegramError


def _fmt(message: str, args: tuple[object, ...]) -> str:
    if args:
        return message % args
    return message


def test_transient_remote_protocol_error_logs_warning(monkeypatch):
    calls: dict[str, list[tuple[str, tuple[object, ...], dict[str, object]]]] = {
        "warning": [],
        "error": [],
    }

    monkeypatch.setattr(
        main.logger,
        "warning",
        lambda msg, *args, **kwargs: calls["warning"].append((msg, args, kwargs)),
    )
    monkeypatch.setattr(
        main.logger,
        "error",
        lambda msg, *args, **kwargs: calls["error"].append((msg, args, kwargs)),
    )

    callback = main.make_polling_error_callback(now_fn=lambda: 100.0, throttle_window_sec=60.0)
    err = NetworkError("transient network")
    err.__cause__ = httpx.RemoteProtocolError("Server disconnected without sending a response.")
    callback(err)

    assert len(calls["warning"]) == 1
    assert len(calls["error"]) == 0
    msg, args, _ = calls["warning"][0]
    rendered = _fmt(msg, args)
    assert "Transient polling network error (remote_disconnect)" in rendered
    assert "Suppressed repeats since last log: 0" in rendered


def test_transient_network_errors_are_throttled(monkeypatch):
    warnings: list[tuple[str, tuple[object, ...], dict[str, object]]] = []
    errors: list[tuple[str, tuple[object, ...], dict[str, object]]] = []

    monkeypatch.setattr(
        main.logger,
        "warning",
        lambda msg, *args, **kwargs: warnings.append((msg, args, kwargs)),
    )
    monkeypatch.setattr(
        main.logger,
        "error",
        lambda msg, *args, **kwargs: errors.append((msg, args, kwargs)),
    )

    ticks = iter([0.0, 10.0, 20.0, 61.0])
    callback = main.make_polling_error_callback(
        now_fn=lambda: next(ticks),
        throttle_window_sec=60.0,
    )
    err = NetworkError("transient network")
    err.__cause__ = httpx.RemoteProtocolError("Server disconnected without sending a response.")

    callback(err)
    callback(err)
    callback(err)
    callback(err)

    assert len(errors) == 0
    assert len(warnings) == 2
    rendered_second = _fmt(warnings[1][0], warnings[1][1])
    assert "Suppressed repeats since last log: 2" in rendered_second


def test_non_transient_error_logs_error_with_exc_info(monkeypatch):
    warnings: list[tuple[str, tuple[object, ...], dict[str, object]]] = []
    errors: list[tuple[str, tuple[object, ...], dict[str, object]]] = []

    monkeypatch.setattr(
        main.logger,
        "warning",
        lambda msg, *args, **kwargs: warnings.append((msg, args, kwargs)),
    )
    monkeypatch.setattr(
        main.logger,
        "error",
        lambda msg, *args, **kwargs: errors.append((msg, args, kwargs)),
    )

    callback = main.make_polling_error_callback(now_fn=lambda: 10.0, throttle_window_sec=60.0)
    err = TelegramError("bad request")
    callback(err)

    assert len(warnings) == 0
    assert len(errors) == 1
    _, _, kwargs = errors[0]
    exc_info = kwargs.get("exc_info")
    assert isinstance(exc_info, tuple)
    assert len(exc_info) == 3
    assert exc_info[1] is err


def test_callback_is_fail_safe_when_logger_warning_raises(monkeypatch):
    fallback_calls: list[tuple[str, tuple[object, ...], dict[str, object]]] = []

    def explode(*_args, **_kwargs):
        raise RuntimeError("logger failed")

    class DummyFallbackLogger:
        def exception(self, msg, *args, **kwargs):
            fallback_calls.append((msg, args, kwargs))

    monkeypatch.setattr(main.logger, "warning", explode)
    monkeypatch.setattr(main, "polling_callback_logger", DummyFallbackLogger())

    callback = main.make_polling_error_callback(now_fn=lambda: 10.0, throttle_window_sec=60.0)
    err = NetworkError("timeout while polling")
    err.__cause__ = httpx.TimeoutException("timeout")
    callback(err)

    assert len(fallback_calls) == 1
    assert "Polling error callback failed unexpectedly." in fallback_calls[0][0]


def test_updater_filter_suppresses_polling_traceback():
    err = NetworkError("transient network")
    err.__cause__ = httpx.ConnectError("connection dropped")
    record = logging.LogRecord(
        name="telegram.ext.Updater",
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg="Exception happened while polling for updates.",
        args=(),
        exc_info=(type(err), err, None),
    )
    filt = main._UpdaterPollingNoiseFilter()
    assert filt.filter(record) is False


def test_updater_filter_keeps_other_messages():
    err = TelegramError("bad request")
    record = logging.LogRecord(
        name="telegram.ext.Updater",
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg="Some other updater error",
        args=(),
        exc_info=(type(err), err, None),
    )
    filt = main._UpdaterPollingNoiseFilter()
    assert filt.filter(record) is True


def test_is_polling_noise_record_matches_expected_message():
    record = logging.LogRecord(
        name="telegram.ext.Updater",
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg="Exception happened while polling for updates.",
        args=(),
        exc_info=None,
    )
    assert is_polling_noise_record(record) is True


def test_is_polling_noise_record_matches_self_gen_throw_line():
    record = logging.LogRecord(
        name="telegram.ext.Updater",
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg="self.gen.throw(typ, value, traceback)",
        args=(),
        exc_info=None,
    )
    assert is_polling_noise_record(record) is True


def test_install_polling_noise_filter_adds_handler_filters():
    root = logging.getLogger()
    original_handlers = list(root.handlers)
    handler = logging.StreamHandler()
    root.handlers = [handler]
    try:
        install_polling_noise_filter()
        assert handler.filters
    finally:
        root.handlers = original_handlers
