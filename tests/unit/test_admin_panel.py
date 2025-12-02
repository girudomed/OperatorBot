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
            'role': 'admin',
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
        mock_db.execute_with_retry.return_value = {'role': 'superadmin', 'status': 'approved'}
        
        can_promote = await permissions.can_promote(123, 'admin')
        assert can_promote is True
        
        can_promote = await permissions.can_promote(123, 'superadmin')
        assert can_promote is True
    
    @pytest.mark.asyncio
    async def test_can_promote_admin_limited(self, permissions, mock_db):
        """Тест что admin не может повышать до superadmin."""
        mock_db.execute_with_retry.return_value = {'role': 'admin', 'status': 'approved'}
        
        can_promote = await permissions.can_promote(123, 'admin')
        assert can_promote is True
        
        can_promote = await permissions.can_promote(123, 'superadmin')
        assert can_promote is False


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
            {'id': 1, 'username': 'user1', 'status': 'pending'},
            {'id': 2, 'username': 'user2', 'status': 'pending'}
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
            True  # update result
        ]
        
        result = await admin_repo.approve_user(user_id=5, approver_id=999)
        assert result is True
    
    @pytest.mark.asyncio
    async def test_get_admins(self, admin_repo, mock_db):
        """Тест получения списка админов."""
        mock_db.execute_with_retry.return_value = [
            {'id': 1, 'role': 'admin', 'username': 'admin1'},
            {'id': 2, 'role': 'superadmin', 'username': 'super1'}
        ]
        
        admins = await admin_repo.get_admins()
        assert len(admins) == 2
        assert any(a['role'] == 'superadmin' for a in admins)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
