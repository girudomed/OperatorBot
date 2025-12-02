"""
Модуль работы с базой данных.
"""

from .manager import DatabaseManager
from .models import (
    UserRecord, OperatorRecord, CallRecord, CallHistoryRecord,
    RoleRecord, ReportRecord, CallMetrics
)

__all__ = [
    "DatabaseManager",
    "UserRecord",
    "OperatorRecord",
    "CallRecord",
    "CallHistoryRecord",
    "RoleRecord",
    "ReportRecord",
    "CallMetrics",
]
