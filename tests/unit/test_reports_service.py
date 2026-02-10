"""
Unit tests for ReportService error handling.
"""

import datetime
from unittest.mock import AsyncMock, Mock

import pytest

from app.errors import OpenAIIntegrationError
from app.services.reports import ReportService


@pytest.mark.asyncio
async def test_generate_report_no_data_saves_empty_status():
    service = ReportService(Mock())
    service.repo = Mock()
    service.report_repo_v2 = Mock()
    service.openai = Mock()

    service.repo.get_extension_by_user_id = AsyncMock(return_value="101")
    service.repo.get_name_by_extension = AsyncMock(return_value="Тест Оператор")
    service.repo.get_call_scores = AsyncMock(return_value=[])
    service.report_repo_v2.get_ready_report_by_cache_key = AsyncMock(return_value=None)
    service.report_repo_v2.save_report = AsyncMock(return_value=True)

    service.openai.generate_recommendations = AsyncMock(return_value="Сгенерированный отчет")

    result = await service.generate_report(user_id=1, period="daily", date_range="2026-02-01")

    assert "Сгенерированный отчет" in result
    service.report_repo_v2.save_report.assert_called_once()
    _, kwargs = service.report_repo_v2.save_report.call_args
    assert kwargs["status"] == "ready"
    assert kwargs["error_text"] is None


@pytest.mark.asyncio
async def test_generate_report_gpt_empty_response_saves_error():
    service = ReportService(Mock())
    service.repo = Mock()
    service.report_repo_v2 = Mock()
    service.openai = Mock()

    service.repo.get_extension_by_user_id = AsyncMock(return_value="101")
    service.repo.get_name_by_extension = AsyncMock(return_value="Тест Оператор")
    service.repo.get_call_scores = AsyncMock(return_value=[{"call_score": 5, "outcome": "record"}])
    service.report_repo_v2.get_ready_report_by_cache_key = AsyncMock(return_value=None)
    service.report_repo_v2.save_report = AsyncMock(return_value=True)

    service.openai.generate_recommendations = AsyncMock(
        side_effect=OpenAIIntegrationError("bad request", retryable=True)
    )

    result = await service.generate_report(user_id=2, period="daily", date_range="2026-02-01")

    assert "Произошла ошибка" in result
    service.report_repo_v2.save_report.assert_called_once()
    _, kwargs = service.report_repo_v2.save_report.call_args
    assert kwargs["status"] == "error"
    assert kwargs["error_text"] == "empty_gpt_response"
