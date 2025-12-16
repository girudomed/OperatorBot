# Файл: app/db/repositories/reports.py

"""
Репозиторий для работы с отчетами.

СХЕМА ТАБЛИЦЫ reports (продакшн):
- PK: report_id (не id!)
- period VARCHAR(20) — строка: 'day', 'week', 'month'
- report_date VARCHAR(50) — строка: '2025-12-05'
- user_id INT NULL — связь с UsersTelegaBot (что именно — уточнить)
- name VARCHAR(255) — имя оператора
- report_text TEXT
- Агрегаты: total_calls, accepted_calls, booked_services, etc.
"""

from typing import List, Dict, Any, Optional
from datetime import date, datetime

from app.db.manager import DatabaseManager
from app.db.models import ReportRecord
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)


class ReportRepository:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    async def save_report_to_db(
        self,
        user_id: int,
        total_calls: int = 0,
        accepted_calls: int = 0,
        booked_services: int = 0,
        conversion_rate: float = 0.0,
        avg_call_rating: float = 0.0,
        total_cancellations: int = 0,
        cancellation_rate: float = 0.0,
        total_conversation_time: int = 0,
        avg_conversation_time: float = 0.0,
        avg_spam_time: float = 0.0,
        total_spam_time: int = 0,
        total_navigation_time: int = 0,
        avg_navigation_time: float = 0.0,
        complaint_calls: int = 0,
        complaint_rating: float = 0.0,
        recommendations: str = "",
        # Новые поля
        name: Optional[str] = None,
        period: str = "day",
        report_date: Optional[str] = None,
        missed_calls: int = 0,
        missed_rate: float = 0.0,
        total_leads: int = 0,
        report_text: str = ""
    ) -> bool:
        """
        Сохранение отчета в базу данных (старый API — для обратной совместимости).
        
        Args:
            user_id: ID пользователя (telegram_id или UsersTelegaBot.id — уточнить)
            ...метрики...
        """
        logger.info(f"[REPORTS] Saving report for user_id={user_id}")

        # report_date = строка YYYY-MM-DD
        if not report_date:
            report_date = datetime.now().strftime('%Y-%m-%d')

        query = """
            INSERT INTO reports (
                user_id, name, period, report_date, report_text,
                total_calls, accepted_calls, booked_services,
                conversion_rate, avg_call_rating,
                total_cancellations, cancellation_rate,
                total_conversation_time, avg_conversation_time,
                avg_spam_time, total_spam_time,
                avg_navigation_time,
                complaint_calls, complaint_rating,
                missed_calls, missed_rate, total_leads
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s
            )
            ON DUPLICATE KEY UPDATE
                name = COALESCE(VALUES(name), name),
                report_text = COALESCE(VALUES(report_text), report_text),
                total_calls = VALUES(total_calls),
                accepted_calls = VALUES(accepted_calls),
                booked_services = VALUES(booked_services),
                conversion_rate = VALUES(conversion_rate),
                avg_call_rating = VALUES(avg_call_rating),
                total_cancellations = VALUES(total_cancellations),
                cancellation_rate = VALUES(cancellation_rate),
                total_conversation_time = VALUES(total_conversation_time),
                avg_conversation_time = VALUES(avg_conversation_time),
                avg_spam_time = VALUES(avg_spam_time),
                total_spam_time = VALUES(total_spam_time),
                avg_navigation_time = VALUES(avg_navigation_time),
                complaint_calls = VALUES(complaint_calls),
                complaint_rating = VALUES(complaint_rating),
                missed_calls = VALUES(missed_calls),
                missed_rate = VALUES(missed_rate),
                total_leads = VALUES(total_leads)
        """

        params = (
            user_id, name, period, report_date, report_text or recommendations,
            total_calls, accepted_calls, booked_services,
            conversion_rate, avg_call_rating,
            total_cancellations, cancellation_rate,
            total_conversation_time, avg_conversation_time,
            avg_spam_time, total_spam_time,
            avg_navigation_time,
            complaint_calls, complaint_rating,
            missed_calls, missed_rate, total_leads
        )

        try:
            await self.db_manager.execute_query(query, params)
            logger.info(f"[REPORTS] Report saved for user_id={user_id}")
            return True
        except Exception as e:
            logger.error(f"[REPORTS] Error saving report: {e}")
            return False

    async def get_report_by_id(self, report_id: int) -> Optional[Dict[str, Any]]:
        """Получить отчёт по report_id (PK)."""
        query = "SELECT * FROM reports WHERE report_id = %s"
        result = await self.db_manager.execute_query(query, (report_id,), fetchone=True)
        return dict(result) if result else None

    async def get_report(
        self,
        user_id: int,
        period: str,
        report_date: str
    ) -> Optional[Dict[str, Any]]:
        """Получить отчёт по user_id + period + report_date."""
        query = """
            SELECT *
            FROM reports
            WHERE user_id = %s 
              AND period = %s 
              AND report_date = %s
        """
        result = await self.db_manager.execute_query(
            query, (user_id, period, report_date), fetchone=True
        )
        return dict(result) if result else None

    async def get_reports_for_date(self, report_date: str) -> List[Dict[str, Any]]:
        """
        Получение всех отчетов за дату.
        
        Args:
            report_date: строка в формате YYYY-MM-DD
        """
        query = """
            SELECT *
            FROM reports
            WHERE report_date = %s
            ORDER BY name
        """
        result = await self.db_manager.execute_query(
            query, (report_date,), fetchall=True
        )
        if not result:
            logger.warning(f"[REPORTS] No reports found for {report_date}")
            return []
        return [dict(row) for row in result]

    async def get_reports_for_today(self) -> List[Dict[str, Any]]:
        """Получение всех отчетов за текущий день."""
        today_str = datetime.now().strftime('%Y-%m-%d')
        return await self.get_reports_for_date(today_str)

    async def get_user_reports(
        self,
        user_id: int,
        limit: int = 30
    ) -> List[Dict[str, Any]]:
        """Получить последние отчёты пользователя."""
        query = """
            SELECT *
            FROM reports
            WHERE user_id = %s
            ORDER BY created_at DESC
            LIMIT %s
        """
        result = await self.db_manager.execute_query(
            query, (user_id, limit), fetchall=True
        )
        return [dict(row) for row in result] if result else []

    async def get_reports_by_period(
        self,
        period: str,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Получить отчёты по типу периода."""
        query = """
            SELECT *
            FROM reports
            WHERE period = %s
            ORDER BY created_at DESC
            LIMIT %s
        """
        result = await self.db_manager.execute_query(
            query, (period, limit), fetchall=True
        )
        return [dict(row) for row in result] if result else []

    async def delete_report(self, report_id: int) -> bool:
        """Удалить отчёт по report_id."""
        query = "DELETE FROM reports WHERE report_id = %s"
        try:
            await self.db_manager.execute_query(query, (report_id,))
            return True
        except Exception as e:
            logger.error(f"[REPORTS] Error deleting report: {e}")
            return False
