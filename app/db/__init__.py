"""
Модуль работы с базой данных.
"""

from .manager import DatabaseManager
from .models import (
    UserRecord, OperatorRecord, CallRecord, CallHistoryRecord,
    ReportRecord, CallMetrics
)

__all__ = [
    "DatabaseManager",
    "UserRecord",
    "OperatorRecord",
    "CallRecord",
    "CallHistoryRecord",
    "ReportRecord",
    "CallMetrics",
]
