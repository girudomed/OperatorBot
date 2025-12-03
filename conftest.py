import sys
from pathlib import Path
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

# Ensure project root is on sys.path for CI environments
ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.services.reports import ReportService

@pytest.fixture(autouse=True)
def mock_openai_service():
    """Автоматический мок OpenAIService для всех тестов."""
    with patch("app.services.openai_service.OpenAIService") as mock_cls:
        instance = MagicMock()
        instance.generate_recommendations = AsyncMock(return_value="Mocked OpenAI response")
        instance.process_batched_requests = AsyncMock(return_value="Mocked batch response")
        instance.split_text.side_effect = lambda text, max_length: [text]
        mock_cls.return_value = instance
        yield instance

@pytest.fixture
def mock_db_manager():
    """Фикстура для мокирования DB Manager."""
    return AsyncMock()

@pytest.fixture
def report_service(mock_db_manager):
    """Фикстура для инициализации ReportService с моками."""
    return ReportService(mock_db_manager)
