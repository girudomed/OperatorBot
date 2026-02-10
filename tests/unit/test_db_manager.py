"""
Unit tests for DatabaseManager.
"""

import pytest
import pytest_asyncio
from unittest.mock import Mock, AsyncMock, patch
from app.db.manager import DatabaseManager

class TestDatabaseManager:
    """Тесты для DatabaseManager"""
    
    @pytest_asyncio.fixture
    async def db_manager(self):
        """Фикстура для DatabaseManager"""
        manager = DatabaseManager()
        # Mock pool
        manager.pool = AsyncMock()
        manager.pool.acquire = AsyncMock()
        manager.pool.close = Mock()
        manager.pool.wait_closed = AsyncMock()
        yield manager
        if manager.pool:
            await manager.close_pool()

    @pytest.mark.asyncio
    async def test_create_pool(self):
        """Тест создания пула"""
        manager = DatabaseManager()
        with patch('aiomysql.create_pool', new_callable=AsyncMock) as mock_create_pool:
            await manager.create_pool()
            mock_create_pool.assert_called_once()
            assert manager.pool is not None

    @pytest.mark.asyncio
    async def test_execute_query_success(self, db_manager):
        """Тест успешного выполнения запроса"""
        mock_conn = AsyncMock()
        mock_cursor = AsyncMock()
        mock_cursor.__aenter__.return_value = mock_cursor
        mock_cursor.__aexit__.return_value = False
        mock_conn.cursor = Mock(return_value=mock_cursor)
        db_manager.pool.acquire = AsyncMock(return_value=mock_conn)
        
        # Mock result
        mock_cursor.fetchone.return_value = {"id": 1}
        
        result = await db_manager.execute_query("SELECT * FROM users", fetchone=True)
        
        assert result == {"id": 1}
        mock_cursor.execute.assert_called_with("SELECT * FROM users", None)

    @pytest.mark.asyncio
    async def test_execute_with_retry_success(self, db_manager):
        """Тест выполнения с ретраем (успех с первой попытки)"""
        db_manager.execute_query = AsyncMock(return_value="success")
        
        result = await db_manager.execute_with_retry("SELECT 1")
        assert result == "success"
        assert db_manager.execute_query.call_count == 1

    @pytest.mark.asyncio
    async def test_execute_with_retry_fail_then_success(self, db_manager):
        """Тест выполнения с ретраем (успех после ошибки)"""
        import aiomysql
        
        # Первая попытка - transient DB ошибка (2003), вторая - успех
        db_manager.execute_query = AsyncMock(side_effect=[
            aiomysql.Error(2003, "Connection refused"),
            "success"
        ])
        
        result = await db_manager.execute_with_retry("SELECT 1", retries=2, base_delay=0.01)
        assert result == "success"
        assert db_manager.execute_query.call_count == 2

    @pytest.mark.asyncio
    async def test_execute_with_retry_unknown_error_not_retried(self, db_manager):
        """Unknown DB errors не должны ретраиться автоматически."""
        import aiomysql

        db_manager.execute_query = AsyncMock(side_effect=aiomysql.Error("unknown failure"))

        with pytest.raises(Exception):
            await db_manager.execute_with_retry("SELECT 1", retries=3, base_delay=0.01)
        assert db_manager.execute_query.call_count == 1
