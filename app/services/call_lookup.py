"""
Сервис поиска звонков.
"""

import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional, Tuple

from app.db.manager import DatabaseManager
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)


def _as_datetime_range(start: date, end: date) -> Tuple[datetime, datetime]:
    """Преобразование диапазона дат в datetime для SQL."""
    return (
        datetime.combine(start, time.min),
        datetime.combine(end, time.max),
    )


def _normalize_phone_sql(column: str) -> str:
    """
    Возвращает SQL-выражение, которое удаляет из номера пробелы и спецсимволы.
    Используется для сравнения с нормализованным номером.
    """
    expr = column
    for char in ("+", "-", "(", ")", " "):
        expr = f"REPLACE({expr}, '{char}', '')"
    return expr


@dataclass
class CallLookupResult:
    history_id: int
    call_time: datetime
    caller_info: Optional[str]
    called_info: Optional[str]
    talk_duration: Optional[int]
    record_url: Optional[str]
    score: Optional[float]
    transcript: Optional[str]


class CallLookupService:
    """
    Сервис поиска звонков и расшифровок по номеру телефона.

    Позволяет искать звонки за выбранный период, нормализует номера и
    пишет все обращения в call_access_logs для аудита.
    """

    DEFAULT_LIMIT = 5
    MAX_LIMIT = 25

    def __init__(
        self,
        db_manager: DatabaseManager,
    ):
        self.db_manager = db_manager

    def _normalize_phone_input(self, phone: str) -> str:
        digits = re.sub(r"\D+", "", phone or "")
        if not digits:
            raise ValueError("Номер телефона не содержит цифр.")

        # Приводим российские номера к формату 7XXXXXXXXXX
        if digits.startswith("8") and len(digits) == 11:
            digits = "7" + digits[1:]
        elif digits.startswith("8") and len(digits) > 11:
            digits = "7" + digits[-10:]
        elif digits.startswith("9") and len(digits) == 10:
            digits = "7" + digits
        
        return digits[-10:] if len(digits) > 10 else digits

    def _resolve_period(
        self,
        period: Optional[str],
        custom_start: Optional[date] = None,
        custom_end: Optional[date] = None,
    ) -> Tuple[datetime, datetime]:
        if period == "custom" and custom_start and custom_end:
            start_dt, end_dt = _as_datetime_range(custom_start, custom_end)
            if start_dt > end_dt:
                raise ValueError("Начальная дата должна быть раньше конечной.")
            return start_dt, end_dt

        # Простая реализация parse_period, чтобы не зависеть от db_manager.parse_period
        today = datetime.today().date()
        if period == "daily":
            start_date, end_date = today, today
        elif period == "weekly":
            start_date = today - timedelta(days=today.weekday())
            end_date = today
        elif period == "biweekly":
            start_date = today - timedelta(days=14)
            end_date = today
        elif period == "monthly":
            start_date = today.replace(day=1)
            end_date = today
        elif period == "half_year":
            start_date = today - timedelta(days=183)
            end_date = today
        elif period == "yearly":
            start_date = today - timedelta(days=365)
            end_date = today
        else:
            # Default to monthly if unknown or None
            start_date = today.replace(day=1)
            end_date = today
            
        return _as_datetime_range(start_date, end_date)

    async def lookup_calls(
        self,
        *,
        phone: str,
        period: Optional[str] = "monthly",
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        requesting_user_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        normalized_phone = self._normalize_phone_input(phone)
        if len(normalized_phone) < 6:
            raise ValueError("Номер должен содержать минимум 6 цифр для поиска.")

        limit_value = limit or self.DEFAULT_LIMIT
        limit_value = max(1, min(limit_value, self.MAX_LIMIT))
        offset_value = max(0, offset or 0)

        start_dt, end_dt = self._resolve_period(period, None, None)
        normalized_like = f"%{normalized_phone}%"

        called_expr = _normalize_phone_sql("COALESCE(ch.called_info, '')")
        caller_expr = _normalize_phone_sql("COALESCE(ch.caller_info, '')")

        query = f"""
            SELECT
                ch.id AS history_id,
                ch.call_time,
                ch.caller_info,
                ch.called_info,
                ch.talk_duration,
                ch.record_url,
                cs.score,
                cs.transcript
            FROM call_history ch
            LEFT JOIN call_scores cs ON cs.history_id = ch.id
            WHERE (
                {called_expr} LIKE %s
                OR {caller_expr} LIKE %s
            )
            AND ch.call_time BETWEEN %s AND %s
            ORDER BY ch.call_time DESC
            LIMIT %s OFFSET %s
        """

        params = (
            normalized_like,
            normalized_like,
            start_dt,
            end_dt,
            limit_value,
            offset_value,
        )

        rows = await self.db_manager.execute_with_retry(
            query,
            params=params,
            fetchall=True,
        )
        results: List[CallLookupResult] = []
        for row in rows or []:
            results.append(
                CallLookupResult(
                    history_id=row.get("history_id"),
                    call_time=row.get("call_time"),
                    caller_info=row.get("caller_info"),
                    called_info=row.get("called_info"),
                    talk_duration=row.get("talk_duration"),
                    record_url=row.get("record_url"),
                    score=row.get("score"),
                    transcript=row.get("transcript"),
                )
            )

        await self._log_access(
            requesting_user_id=requesting_user_id,
            normalized_phone=normalized_phone,
            result_count=len(results),
        )

        return {
            "normalized_phone": normalized_phone,
            "limit": limit_value,
            "offset": offset_value,
            "count": len(results),
            "period": period or "monthly",
            "items": [result.__dict__ for result in results],
        }

    async def fetch_call_details(self, history_id: int) -> Optional[Dict[str, Any]]:
        query = """
            SELECT
                ch.id AS history_id,
                ch.call_time,
                ch.caller_info,
                ch.called_info,
                ch.talk_duration,
                ch.record_url,
                cs.score,
                cs.transcript
            FROM call_history ch
            LEFT JOIN call_scores cs ON cs.history_id = ch.id
            WHERE ch.id = %s
            LIMIT 1
        """
        result = await self.db_manager.execute_with_retry(
            query,
            params=(history_id,),
            fetchone=True,
        )
        return result

    async def _log_access(
        self,
        *,
        requesting_user_id: Optional[int],
        normalized_phone: str,
        result_count: int,
    ) -> None:
        query = """
            INSERT INTO call_access_logs (user_id, phone_normalized, result_count, created_at)
            VALUES (%s, %s, %s, NOW())
        """
        try:
            await self.db_manager.execute_with_retry(
                query,
                params=(
                    requesting_user_id,
                    normalized_phone,
                    result_count,
                ),
            )
        except Exception as exc:
            # Логируем, но не срываем основной поток операции
            logger.warning(
                "Не удалось записать call_access_logs: %s", exc, exc_info=True
            )
