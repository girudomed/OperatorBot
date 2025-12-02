"""
Unit tests for MetricsService.
"""

import pytest
from unittest.mock import Mock
from app.services.metrics_service import MetricsService

class TestMetricsService:
    @pytest.fixture
    def mock_repo(self):
        return Mock()

    @pytest.fixture
    def service(self, mock_repo):
        return MetricsService(mock_repo)

    def test_calculate_avg_score(self, service):
        """Тест расчета среднего балла"""
        # Empty
        assert service._calculate_avg_score([]) == 0.0
        
        # With data
        data = [
            {"call_score": 5.0},
            {"call_score": "4.5"},
            {"call_score": 4.0},
            {"call_score": None}, # Should be ignored
        ]
        assert service._calculate_avg_score(data) == pytest.approx(4.5, rel=0.01)

    def test_calculate_conversion(self, service):
        """Тест расчета конверсии"""
        # 10 leads, 2 booked -> 20%
        assert service._calculate_conversion(10, 2) == 20.0
        
        # 0 leads -> 0%
        assert service._calculate_conversion(0, 0) == 0.0
        
        # booked > leads (should not happen but math works) -> >100%
        assert service._calculate_conversion(5, 10) == 200.0
