import pytest
from unittest.mock import AsyncMock
from bot.services.reports import ReportService

@pytest.fixture
def mock_db_manager():
    """Фикстура для мокирования DB Manager."""
    return AsyncMock()

@pytest.fixture
def report_service(mock_db_manager):
    """Фикстура для инициализации ReportService с моками."""
    return ReportService(mock_db_manager)