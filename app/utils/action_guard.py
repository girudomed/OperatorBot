from __future__ import annotations

import asyncio
import time
from typing import Dict


class ActionGuard:
    """
    Позволяет сделать write-операции идемпотентными:
    - одновременно выполняется только один экземпляр action_key;
    - после успешного завершения повторные запросы блокируются на короткое время.
    """

    def __init__(self) -> None:
        self._locks: Dict[str, asyncio.Lock] = {}
        self._recent: Dict[str, float] = {}

    async def acquire(self, key: str, cooldown_seconds: float = 3.0) -> bool:
        """
        Пытается захватить action_key.
        Возвращает False, если операция уже выполняется или недавно завершилась.
        """
        now = time.monotonic()
        last = self._recent.get(key)
        if last and now - last < cooldown_seconds:
            return False

        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock

        if lock.locked():
            return False

        await lock.acquire()
        return True

    def release(self, key: str, *, success: bool) -> None:
        """Сбрасывает захват и при успехе фиксирует время выполнения."""
        lock = self._locks.get(key)
        if lock and lock.locked():
            lock.release()
        if success:
            self._recent[key] = time.monotonic()
        else:
            self._recent.pop(key, None)
        if lock and not lock.locked():
            self._locks.pop(key, None)
