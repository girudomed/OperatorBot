import sys
from pathlib import Path
import pytest
from unittest.mock import AsyncMock

# Ensure project root is on sys.path for CI environments
ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.services.reports import ReportService

@pytest.fixture
def mock_db_manager():
    """Фикстура для мокирования DB Manager."""
    return AsyncMock()

@pytest.fixture
def report_service(mock_db_manager):
    """Фикстура для инициализации ReportService с моками."""
    return ReportService(mock_db_manager)
