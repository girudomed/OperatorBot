"""
Unit tests for MetricsService aligned with the new logic.
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, AsyncMock

from app.services.metrics_service import MetricsService


class TestMetricsService:
    @pytest.fixture
    def mock_repo(self):
        repo = Mock()
        repo.get_quality_summary = AsyncMock()
        return repo

    @pytest.fixture
    def service(self, mock_repo):
        return MetricsService(mock_repo)

    def test_calculate_avg_score(self, service):
        """Проверяем усреднение рейтингов с разными типами значений."""
        assert service._calculate_avg_score([]) == 0.0
        
        data = [
            {"call_score": 5.0},
            {"call_score": "4.5"},
            {"call_score": 4.0},
            {"call_score": None},
        ]
        assert service._calculate_avg_score(data) == pytest.approx(4.5, rel=0.01)

    @pytest.mark.asyncio
    async def test_calculate_operator_metrics(self, service):
        """Проверяем базовый сценарий расчета операторских метрик."""
        call_history = [{
            "history_id": 1,
            "talk_duration": 120,
            "called_info": "101 Dr. Smith",
            "caller_info": "+79991234567",
        }]
        call_scores = [{
            "history_id": 1,
            "transcript": "Sample text",
            "call_category": "Запись на услугу (успешная)",
            "call_score": 4.5,
            "talk_duration": 120,
        }]

        start = datetime(2025, 1, 1)
        end = datetime(2025, 1, 2)

        metrics = await service.calculate_operator_metrics(
            call_history_data=call_history,
            call_scores_data=call_scores,
            extension="101",
            start_date=start,
            end_date=end
        )

        assert metrics["total_calls"] == 1
        assert metrics["accepted_calls"] == 1
        assert metrics["missed_calls"] == 0
        assert metrics["booked_services"] == 1
        assert metrics["conversion_rate_leads"] == pytest.approx(100.0)
        assert metrics["avg_call_rating"] == pytest.approx(4.5)

    @pytest.mark.asyncio
    async def test_calculate_quality_summary(self, service, mock_repo):
        """Проверяем агрегирование сводных метрик качества."""
        mock_repo.get_quality_summary.return_value = {
            "total_calls": 100,
            "missed_calls": 20,
            "avg_score": 4.2,
            "total_leads": 40,
            "leads_no_record": 30,
            "booked_leads": 10,
            "cancellations": 5,
        }

        start = datetime(2025, 1, 1)
        end = datetime(2025, 1, 7)
        result = await service.calculate_quality_summary(
            period="custom",
            start_date=start,
            end_date=end
        )

        assert result["total_calls"] == 100
        assert result["missed_calls"] == 20
        assert result["lead_conversion"] == pytest.approx(25.0)
        assert result["cancellations"] == 5

    @pytest.mark.asyncio
    async def test_calculate_quality_summary_zero_leads(self, service, mock_repo):
        """Конверсия должна корректно обнуляться при отсутствии лидов."""
        mock_repo.get_quality_summary.return_value = {
            "total_calls": 50,
            "missed_calls": 5,
            "avg_score": 4.8,
            "total_leads": 0,
            "leads_no_record": 0,
            "booked_leads": 0,
            "cancellations": 1,
        }

        result = await service.calculate_quality_summary(
            period="daily",
            start_date=datetime(2025, 2, 1),
            end_date=datetime(2025, 2, 1),
        )

        assert result["lead_conversion"] == 0.0
        assert result["missed_rate"] == pytest.approx(10.0)
