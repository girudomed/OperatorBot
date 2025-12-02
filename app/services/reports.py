"""
Сервис генерации отчетов для операторов.
"""

import datetime
from typing import Optional, Tuple, Dict, Any

from app.services.openai_service import OpenAIService
from app.db.repositories.operators import OperatorRepository
from app.db.repositories.reports import ReportRepository
from app.services.metrics_service import MetricsService
from app.db.manager import DatabaseManager
from app.logging_config import get_watchdog_logger
from app.utils.error_handlers import log_async_exceptions

logger = get_watchdog_logger(__name__)


class ReportService:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.repo = OperatorRepository(db_manager)
        self.report_repo = ReportRepository(db_manager)
        self.openai = OpenAIService()
        self.metrics_service = MetricsService(self.repo)

    @log_async_exceptions
    async def generate_report(
        self, 
        user_id: int, 
        period: str = 'daily', 
        date_range: Optional[str] = None
    ) -> str:
        try:
            # 1. Resolve Dates
            start_date, end_date = self._resolve_dates(period, date_range)
            logger.info(f"Генерация отчета для {user_id} за {start_date} - {end_date}")

            # 2. Get Operator Info
            extension = await self.repo.get_extension_by_user_id(user_id)
            if not extension:
                return "Ошибка: Не удалось найти extension оператора."
            
            name = await self.repo.get_name_by_extension(extension)

            # 3. Get Call Data
            data = await self.repo.get_call_data(extension, start_date, end_date)
            if not data['call_history'] and not data['call_scores']:
                return f"Нет данных для оператора {name} за указанный период."

            # 4. Calculate Metrics
            metrics = await self.metrics_service.calculate_operator_metrics(
                call_history_data=data['call_history'],
                call_scores_data=data['call_scores'],
                extension=extension,
                start_date=start_date,
                end_date=end_date
            )

            # 5. Generate Recommendations (OpenAI)
            recommendations = await self._generate_recommendations(name, metrics, data['call_scores'])
            
            # Daily check: skip if no recommendations
            if period == 'daily' and not recommendations.strip():
                return "Нет рекомендаций для ежедневного отчета."

            # 6. Format Report
            report_text = self._format_report(name, start_date, end_date, metrics, recommendations)

            # 7. Save to DB
            await self.report_repo.save_report_to_db(
                user_id=user_id,
                total_calls=metrics.get('total_calls', 0),
                accepted_calls=metrics.get('accepted_calls', 0),
                booked_services=metrics.get('booked_services', 0),
                conversion_rate=metrics.get('conversion_rate_leads', 0.0),
                avg_call_rating=metrics.get('avg_call_rating', 0.0),
                total_cancellations=metrics.get('total_cancellations', 0),
                cancellation_rate=metrics.get('cancellation_rate', 0.0),
                total_conversation_time=int(metrics.get('total_conversation_time', 0)),
                avg_conversation_time=metrics.get('avg_conversation_time', 0.0),
                avg_spam_time=metrics.get('avg_time_spam', 0.0),
                total_spam_time=0, # TODO: Add to metrics if needed
                total_navigation_time=0, # TODO: Add to metrics if needed
                avg_navigation_time=metrics.get('avg_navigation_time', 0.0),
                complaint_calls=metrics.get('complaint_calls', 0),
                complaint_rating=metrics.get('complaint_rating', 0.0),
                recommendations=recommendations
            )

            return report_text

        except Exception as e:
            logger.error(f"Ошибка генерации отчета: {e}", exc_info=True)
            return "Произошла ошибка при генерации отчета."

    async def _generate_recommendations(
        self, 
        name: str, 
        metrics: Dict[str, Any], 
        scores_data: list
    ) -> str:
        # Extract 'result' fields for context
        results_text = "\n".join([row.get('result', '') for row in scores_data if row.get('result')])
        if not results_text:
            return "Нет данных для анализа рекомендаций."

        # Split if too large
        chunks = self.openai.split_text(results_text, 10000)
        
        # Initial analysis prompts
        prompts = []
        for chunk in chunks:
            prompt = (
                f"Данные звонков:\n{chunk}\n\n"
                f"Дай краткие рекомендации для оператора {name}. "
                "Укажи сильные/слабые стороны и шаги для улучшения."
            )
            prompts.append(prompt)

        # Get partial recommendations
        partial_recs = await self.openai.process_batched_requests(prompts)
        
        # Final summary
        final_prompt = (
            f"Обобщи следующие рекомендации для оператора {name} в единый связный вывод:\n"
            f"{partial_recs}"
        )
        return await self.openai.generate_recommendations(final_prompt)

    def _format_report(
        self, 
        name: str, 
        start: datetime.datetime, 
        end: datetime.datetime, 
        metrics: Dict[str, Any], 
        recommendations: str
    ) -> str:
        period_str = f"{start.strftime('%d.%m.%Y')} - {end.strftime('%d.%m.%Y')}"
        
        lines = [
            f"📊 **Отчет для оператора: {name}**",
            f"📅 Период: {period_str}",
            "",
            "**Основные показатели:**",
            f"📞 Всего звонков: {metrics.get('total_calls', 0)}",
            f"✅ Принято: {metrics.get('accepted_calls', 0)}",
            f"❌ Пропущено: {metrics.get('missed_calls', 0)}",
            f"⭐ Средняя оценка: {metrics.get('avg_call_rating', 0.0)}",
            "",
            "**Рекомендации:**",
            recommendations
        ]
        return "\n".join(lines)

    def _resolve_dates(
        self, 
        period: str, 
        date_range: Optional[str]
    ) -> Tuple[datetime.datetime, datetime.datetime]:
        now = datetime.datetime.now()
        
        if period == 'daily':
            if date_range:
                try:
                    dt = datetime.datetime.strptime(date_range, '%Y-%m-%d')
                except ValueError:
                    dt = datetime.datetime.strptime(date_range, '%d/%m/%Y')
                return dt.replace(hour=0, minute=0, second=0), dt.replace(hour=23, minute=59, second=59)
            return now.replace(hour=0, minute=0, second=0), now.replace(hour=23, minute=59, second=59)
            
        elif period == 'weekly':
            start = now - datetime.timedelta(days=now.weekday())
            return start.replace(hour=0, minute=0, second=0), now
            
        elif period == 'monthly':
            start = now.replace(day=1, hour=0, minute=0, second=0)
            return start, now
            
        # Default fallback
        return now.replace(hour=0, minute=0, second=0), now
