import asyncio
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

from app.db.manager import DatabaseManager

_manager: Optional[DatabaseManager] = None
_lock = asyncio.Lock()


async def get_db_manager() -> DatabaseManager:
    """
    Возвращает глобальный экземпляр DatabaseManager с инициализированным пулом.
    """
    global _manager
    if _manager is None:
        async with _lock:
            if _manager is None:
                manager = DatabaseManager()
                await manager.create_pool()
                _manager = manager
    return _manager


@asynccontextmanager
async def acquire_connection() -> AsyncIterator:
    """
    Возвращает соединение из пула в виде контекстного менеджера.
    """
    manager = await get_db_manager()
    async with manager.acquire() as connection:
        yield connection


async def execute_query(
    query: str,
    params=None,
    *,
    fetchone: bool = False,
    fetchall: bool = False,
    retries: int = 3,
):
    """
    Выполняет SQL-запрос с возможностью получения одной или всех записей.
    По умолчанию включает повторные попытки при ошибках.
    """
    manager = await get_db_manager()
    return await manager.execute_with_retry(
        query,
        params=params,
        fetchone=fetchone,
        fetchall=fetchall,
        retries=retries,
    )


async def close_db_manager():
    """
    Закрывает пул соединений и сбрасывает глобальный менеджер.
    """
    global _manager
    if _manager is not None:
        await _manager.close_pool()
        _manager = None

