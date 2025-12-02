"""
Unit tests for CallLookupService.
"""

import pytest
from datetime import date, datetime
from unittest.mock import Mock
from app.services.call_lookup import CallLookupService

class TestCallLookupService:
    """Тесты для CallLookupService"""

    @pytest.fixture
    def service(self):
        db_manager = Mock()
        return CallLookupService(db_manager)

    def test_normalize_phone_input(self, service):
        """Тест нормализации телефонного номера"""
        # Тест с 8
        assert service._normalize_phone_input("8 (999) 123-45-67") == "79991234567"
        # Тест с 7
        assert service._normalize_phone_input("+7 (999) 123-45-67") == "79991234567"
        # Тест без кода страны (10 цифр) -> добавляем 7
        assert service._normalize_phone_input("9991234567") == "79991234567"
        # Тест с лишними символами
        assert service._normalize_phone_input("8-999-123-45-67") == "79991234567"
        
        # Ошибка если нет цифр
        with pytest.raises(ValueError):
            service._normalize_phone_input("abc")

    def test_resolve_period(self, service):
        """Тест разрешения периода"""
        # Daily
        start, end = service._resolve_period("daily")
        today = datetime.today().date()
        assert start.date() == today
        assert end.date() == today
        
        # Custom
        custom_start = date(2024, 1, 1)
        custom_end = date(2024, 1, 31)
        start, end = service._resolve_period("custom", custom_start, custom_end)
        assert start.date() == custom_start
        assert end.date() == custom_end
        
        # Custom invalid
        with pytest.raises(ValueError):
            service._resolve_period("custom", custom_end, custom_start)
