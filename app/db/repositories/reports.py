"""
Репозиторий для работы с отчетами.
"""

from typing import List, Dict, Any

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
        total_calls: int, 
        accepted_calls: int, 
        booked_services: int, 
        conversion_rate: float,
        avg_call_rating: float, 
        total_cancellations: int, 
        cancellation_rate: float, 
        total_conversation_time: int,
        avg_conversation_time: float, 
        avg_spam_time: float, 
        total_spam_time: int, 
        total_navigation_time: int,
        avg_navigation_time: float, 
        complaint_calls: int, 
        complaint_rating: float, 
        recommendations: str
    ) -> None:
        """Сохранение отчета в базу данных."""
        logger.debug(f"Saving report to DB for user_id: {user_id}")

        query = """
        INSERT INTO reports (user_id, report_date, total_calls, accepted_calls, booked_services, conversion_rate,
                             avg_call_rating, total_cancellations, cancellation_rate, total_conversation_time,
                             avg_conversation_time, avg_spam_time, total_spam_time, total_navigation_time,
                             avg_navigation_time, complaint_calls, complaint_rating, recommendations)
        VALUES (%s, CURRENT_DATE, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            total_calls=VALUES(total_calls),
            accepted_calls=VALUES(accepted_calls),
            booked_services=VALUES(booked_services),
            conversion_rate=VALUES(conversion_rate),
            avg_call_rating=VALUES(avg_call_rating),
            total_cancellations=VALUES(total_cancellations),
            cancellation_rate=VALUES(cancellation_rate),
            total_conversation_time=VALUES(total_conversation_time),
            avg_conversation_time=VALUES(avg_conversation_time),
            avg_spam_time=VALUES(avg_spam_time),
            total_spam_time=VALUES(total_spam_time),
            total_navigation_time=VALUES(total_navigation_time),
            avg_navigation_time=VALUES(avg_navigation_time),
            complaint_calls=VALUES(complaint_calls),
            complaint_rating=VALUES(complaint_rating),
            recommendations=VALUES(recommendations)
        """
        params = (
            user_id, total_calls, accepted_calls, booked_services, conversion_rate, avg_call_rating,
            total_cancellations, cancellation_rate, total_conversation_time, avg_conversation_time,
            avg_spam_time, total_spam_time, total_navigation_time, avg_navigation_time,
            complaint_calls, complaint_rating, recommendations
        )
        await self.db_manager.execute_query(query, params)
        logger.info(f"Отчет для user_id {user_id} сохранен.")

    async def get_reports_for_today(self) -> List[ReportRecord]:
        """Получение всех отчетов за текущий день."""
        query = """
        SELECT user_id, report_date, total_calls, accepted_calls, booked_services, conversion_rate, avg_call_rating,
               total_cancellations, cancellation_rate, total_conversation_time, avg_conversation_time, avg_spam_time,
               total_spam_time, total_navigation_time, avg_navigation_time, complaint_calls,
               complaint_rating, recommendations
        FROM reports
        WHERE report_date = CURRENT_DATE
        """
        
        result = await self.db_manager.execute_query(query, fetchall=True)
        if not result:
            logger.warning("Отчеты за текущий день не найдены.")
            return []
        return result
