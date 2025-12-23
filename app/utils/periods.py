# Файл: app/utils/periods.py

"""Утилиты для расчёта границ периодов в сутках."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional, Tuple


def calculate_period_bounds(days: int, *, reference: Optional[datetime] = None) -> Tuple[datetime, datetime]:
    """
    Возвращает границы периода в датах (start, end), где end эксклюзивна.

    Args:
        days: количество суток в периоде (положительное число).
        reference: опциональная дата/время, относительно которой строим период (по умолчанию — сейчас).

    Returns:
        Кортеж из двух datetime:
            start_dt — начало периода (00:00:00 включительно),
            end_dt — конец периода (00:00:00 следующего дня, эксклюзивно).
    """
    if days <= 0:
        raise ValueError("days must be positive")
    ref = reference or datetime.now()
    day_start = ref.replace(hour=0, minute=0, second=0, microsecond=0)
    period_end = day_start + timedelta(days=1)
    period_start = period_end - timedelta(days=days)
    return period_start, period_end
