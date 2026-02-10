# tests/test_watch_dog.py
"""Тесты для модуля watch_dog"""

import logging
import pytest
import re
import sys
from datetime import datetime
from zoneinfo import ZoneInfo
from watch_dog.filters import SensitiveDataFilter
from watch_dog.logger import _TraceTimestampFormatter

class TestSensitiveDataFilter:
    """Тесты фильтрации чувствительных данных"""
    
    @pytest.fixture
    def log_filter(self):
        return SensitiveDataFilter()
    
    @pytest.fixture
    def log_record(self):
        return logging.LogRecord(
            name="test", level=logging.INFO, pathname=__file__, lineno=10,
            msg="Test message", args=(), exc_info=None
        )

    def test_phone_masking(self, log_filter, log_record):
        """Тест маскировки телефонов"""
        phones = [
            ("79991234567", "79...4567"),
            ("8 (999) 123-45-67", "8 (99...5-67"),
            ("+7 999 123 45 67", "+7 99...45 67"),
        ]
        
        for original, expected_part in phones:
            log_record.msg = f"User phone is {original}"
            log_filter.filter(log_record)
            assert original not in log_record.msg
            # Проверяем что последние 4 цифры остались
            assert original[-4:] in log_record.msg

    def test_email_masking(self, log_filter, log_record):
        """Тест маскировки email"""
        email = "user@example.com"
        log_record.msg = f"Contact {email} for support"
        log_filter.filter(log_record)
        assert email not in log_record.msg
        assert "***EMAIL***" in log_record.msg

    def test_env_secret_masking(self, monkeypatch):
        """Тест маскировки секретов из переменных окружения"""
        secret = "super_secret_token_123"
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", secret)
        
        # Пересоздаем фильтр чтобы подтянуть env
        custom_filter = SensitiveDataFilter()
        
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname=__file__, lineno=10,
            msg=f"Token is {secret}", args=(), exc_info=None
        )
        
        custom_filter.filter(record)
        assert secret not in record.msg
        assert "***SECRET***" in record.msg

    def test_args_masking(self, log_filter):
        """Тест маскировки в аргументах лога"""
        phone = "79991234567"
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname=__file__, lineno=10,
            msg="User %s connected", args=(phone,), exc_info=None
        )
        
        log_filter.filter(record)
        assert phone not in record.args[0]
        assert phone[-4:] in record.args[0]

if __name__ == "__main__":
    pytest.main([__file__, "-v"])


def test_trace_formatter_adds_timestamp_on_each_multiline_line():
    formatter = _TraceTimestampFormatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    formatter.converter = lambda *args: datetime.now(ZoneInfo("Europe/Moscow")).timetuple()

    try:
        raise RuntimeError("boom")
    except RuntimeError:
        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname=__file__,
            lineno=100,
            msg="Incident happened",
            args=(),
            exc_info=sys.exc_info(),
        )

    rendered = formatter.format(record)
    lines = [line for line in rendered.splitlines() if line]
    assert len(lines) > 1
    ts_prefix = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")
    assert all(ts_prefix.match(line) for line in lines)
