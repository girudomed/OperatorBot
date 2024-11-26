import pytest
from unittest.mock import AsyncMock
from openai_telebot import OpenAIReportGenerator

@pytest.fixture
def mock_db_manager():
    """Фикстура для мокирования DB Manager."""
    return AsyncMock()

@pytest.fixture
def report_generator(mock_db_manager):
    """Фикстура для инициализации OpenAIReportGenerator с моками."""
    return OpenAIReportGenerator(mock_db_manager)