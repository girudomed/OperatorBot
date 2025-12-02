"""
Интеграционные тесты для очереди задач и сервисов.
"""
import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from app.workers.task_worker import add_task, start_workers, TaskStatus, QueueFullError
from app.services.reports import ReportService
from app.db.manager import DatabaseManager


class TestQueueIntegration:
    """Интеграционные тесты для очереди задач."""

    @pytest.fixture
    async def mock_application(self):
        """Мок экземпляра приложения с необходимыми атрибутами в bot_data."""
        app = Mock()
        app.bot = AsyncMock()
        app.bot_data = {}
        
        # Мок DB Manager
        db_manager = AsyncMock(spec=DatabaseManager)
        db_manager.acquire = AsyncMock()
        app.bot_data["db_manager"] = db_manager
        
        # Мок Report Service
        report_service = AsyncMock(spec=ReportService)
        report_service.generate_report = AsyncMock(return_value="Test Report")
        app.bot_data["report_service"] = report_service
        
        app.bot_data["_task_queue_workers"] = []
        return app

    @pytest.mark.asyncio
    async def test_add_task_and_process(self, mock_application):
        """Тест добавления задачи в очередь и её обработки."""
        # Запускаем воркеры
        await start_workers(mock_application)
        
        # Добавляем задачу
        task_id, status = await add_task(
            mock_application,
            user_id=123,
            report_type="daily",
            period="daily",
            chat_id=456
        )
        
        assert task_id is not None
        assert status == TaskStatus.QUEUED
        
        # Даем время на обработку
        await asyncio.sleep(0.5)
        
        # Проверяем что report_service был вызван
        mock_application.bot_data["report_service"].generate_report.assert_called()

    @pytest.mark.asyncio
    async def test_queue_full_error(self, mock_application):
        """Тест переполнения очереди."""
        await start_workers(mock_application)
        
        # Патчим очередь в новом месте
        with patch('app.workers.task_worker.task_queue') as mock_queue:
            mock_queue.full.return_value = True
            
            with pytest.raises(QueueFullError):
                await add_task(
                    mock_application,
                    user_id=999,
                    report_type="daily",
                    period="daily"
                )


class TestServicesIntegration:
    """Интеграционные тесты для сервисов."""

    @pytest.mark.asyncio
    async def test_report_service_end_to_end(self):
        """E2E тест для ReportService (с моками БД)."""
        
        # Мок DB Manager
        mock_db = AsyncMock(spec=DatabaseManager)
        mock_db.execute_with_retry = AsyncMock()
        
        service = ReportService(mock_db)
        
        # Мокируем методы репозитория
        # Note: In the new architecture, repo is initialized inside service
        # We need to patch the repo instance on the service
        
        with patch.object(service.repo, 'get_extension_by_user_id', return_value='101'):
            with patch.object(service.repo, 'get_name_by_extension', return_value='Test Operator'):
                with patch.object(service.repo, 'get_call_data', return_value={
                    'call_history': [],
                    'call_scores': [],
                    'accepted_calls': [],
                    'missed_calls': []
                }):
                    report = await service.generate_report(
                        user_id=1,
                        period='daily'
                    )
                    
                    assert "Нет данных" in report or "Test Operator" in report


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
