from __future__ import annotations

import time
from typing import Any, MutableMapping


class RateLimiter:
    """Простейший in-memory rate limiter на основе меток времени."""

    def __init__(self) -> None:
        self._hits: dict[str, float] = {}

    def should_limit(self, key: str, cooldown_seconds: float) -> bool:
        """
        Возвращает True, если ключ срабатывал менее cooldown_seconds назад.
        В противном случае - сохраняет текущее время и возвращает False.
        """
        now = time.monotonic()
        last = self._hits.get(key)
        if last is not None and now - last < cooldown_seconds:
            return True
        self._hits[key] = now
        return False


def rate_limit_hit(
    bot_data: MutableMapping[str, Any],
    user_id: int,
    action: str,
    cooldown_seconds: float,
) -> bool:
    """
    Утилита для хендлеров.

    Args:
        bot_data: application.bot_data
        user_id: Telegram user_id
        action: произвольный ключ (например, 'admin_dashboard')
        cooldown_seconds: окно блокировки

    Returns:
        True если запрос нужно ограничить.
    """
    limiter = bot_data.get("rate_limiter")
    if not isinstance(limiter, RateLimiter):
        return False
    key = f"{user_id}:{action}"
    return limiter.should_limit(key, cooldown_seconds)
