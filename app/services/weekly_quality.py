# Файл: app/services/weekly_quality.py

"""
Сервис еженедельных отчетов качества.
"""

from datetime import datetime
from typing import Any, Dict, Optional, Union

from app.db.manager import DatabaseManager
from app.services.metrics_service import MetricsService
from app.db.repositories.operators import OperatorRepository
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)


class WeeklyQualityService:
    """
    Сервис агрегированных отчётов качества для команды /weekly_quality.

    Строит метрики через MetricsService и форматирует их в текстовый отчёт.
    """

    def __init__(
        self,
        db_manager: DatabaseManager,
        metrics_service: Optional[MetricsService] = None,
    ) -> None:
        self.db_manager = db_manager
        
        if metrics_service:
            self.metrics_service = metrics_service
        else:
            repo = OperatorRepository(db_manager)
            self.metrics_service = MetricsService(repo)

    async def get_summary(
        self,
        *,
        period: str = "weekly",
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
    ) -> Dict[str, Any]:
        """
        Возвращает агрегированные метрики качества за указанный период.
        """
        summary = await self.metrics_service.calculate_quality_summary(
            period=period,
            start_date=start_date,
            end_date=end_date,
        )
        logger.debug(f"Получен агрегированный отчёт: {summary}")
        return summary

    async def get_text_report(
        self,
        *,
        period: str = "weekly",
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
    ) -> str:
        """
        Возвращает текстовый отчёт для отправки пользователю.
        """
        summary = await self.get_summary(
            period=period,
            start_date=start_date,
            end_date=end_date,
        )
        return self.format_summary(summary)

    def format_summary(self, summary: Dict[str, Any]) -> str:
        """
        Преобразует агрегированные метрики в удобочитаемый текст.
        Если доступны LM метрики, включает их в отчет.
        """
        start_date = summary.get("start_date")
        end_date = summary.get("end_date")
        header = "Отчёт качества"
        if start_date and end_date:
            header += f" за {start_date} — {end_date}"

        lines = [
            header,
            "",
            f"Всего звонков: {summary.get('total_calls', 0)}",
            f"Пропущено: {summary.get('missed_calls', 0)} "
            f"({summary.get('missed_rate', 0)}%)",
            f"Средняя оценка: {summary.get('avg_score', 0.0)}",
            f"Лиды: {summary.get('total_leads', 0)} "
            f"(конверсия в запись {summary.get('lead_conversion', 0)}%)",
            f"Записано лидов: {summary.get('booked_leads', 0)}",
            f"Отмены: {summary.get('cancellations', 0)}",
        ]
        
        # Add LM metrics if available
        if summary.get('lm_enabled') and summary.get('lm_metrics'):
            lines.append("")
            lines.append("--- Аналитика LM ---")
            
            lm_metrics = summary['lm_metrics']
            aggregates = lm_metrics.get('aggregates', {})
            
            # Conversion score
            if 'conversion_score' in aggregates:
                conv = aggregates['conversion_score']
                lines.append(f"Средний скор конверсии: {conv.get('avg', 'N/A')}")
            
            # Quality score
            if 'normalized_call_score' in aggregates:
                quality = aggregates['normalized_call_score']
                lines.append(f"Средний скор качества (LM): {quality.get('avg', 'N/A')}")
            
            # Risk distribution
            risk_dist = lm_metrics.get('risk_distribution', {})
            if risk_dist:
                total_risk_calls = sum(risk_dist.values())
                if total_risk_calls > 0:
                    high_risk = risk_dist.get('high', 0)
                    high_risk_pct = (high_risk / total_risk_calls * 100) if total_risk_calls else 0
                    lines.append(f"Высокий риск оттока: {high_risk} ({high_risk_pct:.1f}%)")
        
        return "\n".join(lines)
