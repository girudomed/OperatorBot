import asyncio
from typing import Dict


class JobGuard:
    """Дедупликация долгих задач (экспорт, синхронизация)."""

    def __init__(self) -> None:
        self._locks: Dict[str, asyncio.Lock] = {}

    async def acquire(self, key: str) -> bool:
        """
        Пытается взять lock для задачи.
        Возвращает False, если задача уже выполняется.
        """
        lock = self._locks.setdefault(key, asyncio.Lock())
        if lock.locked():
            return False
        await lock.acquire()
        return True

    def release(self, key: str) -> None:
        """Снимает lock после завершения задачи."""
        lock = self._locks.get(key)
        if lock and lock.locked():
            lock.release()
