"""
Тесты для админской функциональности.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock

from app.db.repositories.admin import AdminRepository
from app.telegram.middlewares.permissions import PermissionsManager


class TestPermissionsManager:
    """Тесты для PermissionsManager."""
    
    @pytest.fixture
    def mock_db(self):
        """Мок DatabaseManager."""
        db = AsyncMock()
        db.execute_with_retry = AsyncMock()
        return db
    
    @pytest.fixture
    def permissions(self, mock_db):
        """Инстанс PermissionsManager."""
        return PermissionsManager(mock_db)
    
    @pytest.mark.asyncio
    async def test_get_user_role(self, permissions, mock_db):
        """Тест получения роли пользователя."""
        mock_db.execute_with_retry.return_value = {
            'role_id': 2,
            'status': 'approved'
        }
        
        role = await permissions.get_user_role(123)
        assert role == 'admin'
    
    @pytest.mark.asyncio
    async def test_get_user_role_not_approved(self, permissions, mock_db):
        """Тест что pending пользователи не получают роль."""
        mock_db.execute_with_retry.return_value = {
            'role': 'admin',
            'status': 'pending'
        }
        
        role = await permissions.get_user_role(123)
        assert role is None
    
    def test_is_supreme_admin_by_id(self, permissions, monkeypatch):
        """Тест проверки supreme admin по ID."""
        monkeypatch.setattr('app.telegram.middlewares.permissions.SUPREME_ADMIN_ID', '123')
        
        assert permissions.is_supreme_admin(123) is True
        assert permissions.is_supreme_admin(456) is False
    
    @pytest.mark.asyncio
    async def test_can_promote_superadmin_can_all(self, permissions, mock_db):
        """Тест что superadmin может повышать всех."""
        mock_db.execute_with_retry.return_value = {'role_id': 3, 'status': 'approved'}
        
        can_promote = await permissions.can_promote(123, 'admin')
        assert can_promote is True
        
        can_promote = await permissions.can_promote(123, 'superadmin')
        assert can_promote is True
    
    @pytest.mark.asyncio
    async def test_can_promote_admin_limited(self, permissions, mock_db):
        """Тест что admin не может повышать до superadmin."""
        mock_db.execute_with_retry.return_value = {'role_id': 2, 'status': 'approved'}
        
        can_promote = await permissions.can_promote(123, 'admin')
        assert can_promote is True
        
        can_promote = await permissions.can_promote(123, 'superadmin')
        assert can_promote is False

    @pytest.mark.asyncio
    async def test_can_access_admin_panel_for_admin(self, permissions, mock_db):
        """Admin получает доступ в /admin."""
        mock_db.execute_with_retry.return_value = {'role_id': 2, 'status': 'approved'}
        assert await permissions.can_access_admin_panel(123) is True

    @pytest.mark.asyncio
    async def test_can_access_admin_panel_for_pending(self, permissions, mock_db):
        """Pending не допускается в /admin."""
        mock_db.execute_with_retry.return_value = {'role_id': 2, 'status': 'pending'}
        assert await permissions.can_access_admin_panel(123) is False

    @pytest.mark.asyncio
    async def test_can_access_call_lookup_operator(self, permissions, mock_db):
        """Утверждённый оператор имеет доступ к /call_lookup."""
        mock_db.execute_with_retry.side_effect = [
            {'status': 'approved'},  # get_user_status
            {'role_id': 1, 'status': 'approved'},  # get_user_role
        ]
        result = await permissions.can_access_call_lookup(123)
        assert result is True
        mock_db.execute_with_retry.side_effect = None

    @pytest.mark.asyncio
    async def test_can_access_call_lookup_pending_denied(self, permissions, mock_db):
        """Pending пользователь не может читать расшифровки."""
        mock_db.execute_with_retry.side_effect = [
            {'status': 'pending'},
        ]
        result = await permissions.can_access_call_lookup(123)
        assert result is False
        mock_db.execute_with_retry.side_effect = None


class TestAdminRepository:
    """Тесты для AdminRepository."""
    
    @pytest.fixture
    def mock_db(self):
        db = AsyncMock()
        db.execute_with_retry = AsyncMock()
        return db
    
    @pytest.fixture
    def admin_repo(self, mock_db):
        return AdminRepository(mock_db)
    
    @pytest.mark.asyncio
    async def test_get_pending_users(self, admin_repo, mock_db):
        """Тест получения pending пользователей."""
        mock_db.execute_with_retry.return_value = [
            {'id': 1, 'username': 'user1', 'status': 'pending', 'role_id': 1},
            {'id': 2, 'username': 'user2', 'status': 'pending', 'role_id': 1}
        ]
        
        pending = await admin_repo.get_pending_users()
        assert len(pending) == 2
        assert pending[0]['status'] == 'pending'
    
    @pytest.mark.asyncio
    async def test_approve_user(self, admin_repo, mock_db):
        """Тест утверждения пользователя."""
        # Mock approver lookup
        mock_db.execute_with_retry.side_effect = [
            {'id': 10},  # approver DB id
            True,        # update result
            True         # log entry
        ]
        
        result = await admin_repo.approve_user(user_id=5, approver_id=999)
        assert result is True
    
    @pytest.mark.asyncio
    async def test_get_admins(self, admin_repo, mock_db):
        """Тест получения списка админов."""
        mock_db.execute_with_retry.return_value = [
            {'id': 1, 'role_id': 2, 'username': 'admin1'},
            {'id': 2, 'role_id': 3, 'username': 'super1'}
        ]
        
        admins = await admin_repo.get_admins()
        assert len(admins) == 2
        assert any(a['role'] == 'superadmin' for a in admins)

    @pytest.mark.asyncio
    async def test_get_users_counters(self, admin_repo, mock_db):
        """Тест агрегированного счётчика пользователей."""
        mock_db.execute_with_retry.return_value = {
            'total_users': 10,
            'pending_users': 3,
            'approved_users': 6,
            'blocked_users': 1,
            'admins_count': 2,
            'operators_count': 4,
        }

        counters = await admin_repo.get_users_counters()
        assert counters['total_users'] == 10
        assert counters['pending_users'] == 3
        assert counters['admins'] == 2
        assert counters['operators'] == 4


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
