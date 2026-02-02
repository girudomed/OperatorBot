from app.telegram.handlers.reports import _ReportHandler


def test_safe_int_handles_invalid_values():
    assert _ReportHandler._safe_int("10") == 10
    assert _ReportHandler._safe_int("bad", default=0) == 0
    assert _ReportHandler._safe_int(None, default=None) is None
