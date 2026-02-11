import pytest
from unittest.mock import AsyncMock, MagicMock

from app.telegram.handlers.admin_stats import AdminStatsHandler


@pytest.fixture
def mock_admin_repo():
    repo = MagicMock()
    repo.get_pending_users = AsyncMock(return_value=[{"id": 1}, {"id": 2}])
    repo.get_admins = AsyncMock(return_value=[{"id": 10}])
    return repo


@pytest.fixture
def mock_metrics_service():
    service = MagicMock()
    service.calculate_quality_summary = AsyncMock(
        return_value={
            "total_calls": 100,
            "missed_calls": 20,
            "missed_rate": 20.0,
            "avg_score": 4.5,
            "total_leads": 30,
            "lead_conversion": 50.0,
            "cancellations": 3,
        }
    )
    return service


@pytest.fixture
def handler(mock_admin_repo, mock_metrics_service):
    permissions = MagicMock()
    permissions.can_view_all_stats = AsyncMock(return_value=True)
    return AdminStatsHandler(mock_admin_repo, mock_metrics_service, permissions)


class TestAdminStatsHandler:
    @pytest.mark.asyncio
    async def test_show_stats_renders_card(self, handler, mock_admin_repo, mock_metrics_service):
        """Проверяем, что show_stats выводит карточку и запрашивает данные."""
        query = MagicMock()
        query.answer = AsyncMock()
        query.edit_message_text = AsyncMock()
        query.message.reply_text = AsyncMock()
        query.data = "admin:stats"
        update = MagicMock()
        update.callback_query = query
        context = MagicMock()
        context.application.bot_data = {}

        await handler.show_stats(update, context)

        mock_admin_repo.get_pending_users.assert_awaited_once()
        mock_admin_repo.get_admins.assert_awaited_once()
        assert mock_metrics_service.calculate_quality_summary.await_count == 5
        query.answer.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_show_stats_denied_without_permission(self, handler, mock_admin_repo, mock_metrics_service):
        query = MagicMock()
        query.answer = AsyncMock()
        query.edit_message_text = AsyncMock()
        query.message.reply_text = AsyncMock()
        query.data = "adm:st"
        update = MagicMock()
        update.callback_query = query
        update.effective_user = MagicMock(id=22, username="user")
        context = MagicMock()
        context.application.bot_data = {}

        handler.permissions.can_view_all_stats = AsyncMock(return_value=False)

        await handler.show_stats(update, context)

        mock_admin_repo.get_pending_users.assert_not_awaited()
        mock_admin_repo.get_admins.assert_not_awaited()
        mock_metrics_service.calculate_quality_summary.assert_not_awaited()
