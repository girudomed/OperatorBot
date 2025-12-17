# Файл: app/services/reports.py

"""
Сервис генерации отчетов для операторов.
"""

import datetime
from typing import Optional, Tuple, Dict, Any
from datetime import date as date_type

from app.services.openai_service import OpenAIService
from app.db.repositories.operators import OperatorRepository
from app.db.repositories.reports import ReportRepository
from app.db.repositories.analytics import AnalyticsRepository
from app.services.metrics_service import MetricsService
from app.services.recommendations import RecommendationsService
from app.db.manager import DatabaseManager
from app.logging_config import get_watchdog_logger
from app.utils.error_handlers import log_async_exceptions

logger = get_watchdog_logger(__name__)


class ReportService:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.repo = OperatorRepository(db_manager)
        self.report_repo = ReportRepository(db_manager)
        self.analytics_repo = AnalyticsRepository(db_manager)
        self.openai = OpenAIService()
        self.metrics_service = MetricsService(self.repo)
        self.recommendations_service = RecommendationsService()

    @log_async_exceptions
    async def generate_report(
        self,
        user_id: int,
        period: str = 'daily',
        date_range: Optional[str] = None,
        extension: Optional[str] = None,
    ) -> str:
        try:
            # 1. Resolve Dates
            start_date, end_date = self._resolve_dates(period, date_range)
            logger.info(f"Генерация отчета для {user_id} за {start_date} - {end_date}")

            # 2. Get Operator Info
            resolved_extension = extension or await self.repo.get_extension_by_user_id(user_id)
            if not resolved_extension:
                logger.warning(
                    "report: не найден extension для пользователя %s",
                    user_id,
                )
                return "Ошибка: Не удалось найти extension оператора."
            
            name = await self.repo.get_name_by_extension(resolved_extension)

            # 3. Get Call Data (старая логика для обратной совместимости)
            data = await self.repo.get_call_data(resolved_extension, start_date, end_date)
            if not data['call_history'] and not data['call_scores']:
                logger.warning(
                    "report: нет данных по звонкам для %s (extension=%s, period=%s-%s)",
                    user_id,
                    resolved_extension,
                    start_date,
                    end_date,
                )
                return f"Нет данных для оператора {name} за указанный период."

            # 4. Calculate Metrics (старая логика)
            metrics = await self.metrics_service.calculate_operator_metrics(
                call_history_data=data['call_history'],
                call_scores_data=data['call_scores'],
                extension=resolved_extension,
                start_date=start_date,
                end_date=end_date
            )

            # 5. НОВОЕ: Получаем дашборд метрики для более детального анализа
            try:
                dashboard_metrics = await self.analytics_repo.get_live_dashboard_single(
                    operator_name=name,
                    period_type='day' if period == 'daily' else 'week' if period == 'weekly' else 'month'
                )
            except Exception as e:
                logger.warning(f"Не удалось получить dashboard метрики: {e}", exc_info=True)
                dashboard_metrics = None

            # 6. НОВОЕ: Генерируем рекомендации через новый сервис
            recommendations = await self._generate_recommendations_new(
                name=name, 
                metrics=metrics,
                dashboard_metrics=dashboard_metrics,
                start_date=start_date.date() if isinstance(start_date, datetime.datetime) else start_date,
                end_date=end_date.date() if isinstance(end_date, datetime.datetime) else end_date
            )
            
            # Daily check: skip if no recommendations
            if period == 'daily' and not recommendations.strip():
                return "Нет рекомендаций для ежедневного отчета."

            # 7. Format Report (обновленный формат)
            report_text = self._format_report_new(
                name=name,
                start=start_date,
                end=end_date,
                metrics=metrics,
                dashboard_metrics=dashboard_metrics,
                recommendations=recommendations
            )

            # 8. Save to DB
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
                total_spam_time=0,
                total_navigation_time=0,
                avg_navigation_time=metrics.get('avg_navigation_time', 0.0),
                complaint_calls=metrics.get('complaint_calls', 0),
                complaint_rating=metrics.get('complaint_rating', 0.0),
                recommendations=recommendations
            )

            # 9. НОВОЕ: Сохраняем рекомендации в отдельную таблицу
            try:
                await self.analytics_repo.save_recommendations(
                    operator_name=name,
                    report_date=start_date.date() if isinstance(start_date, datetime.datetime) else start_date,
                    recommendations=recommendations,
                    call_samples_analyzed=len(data.get('call_scores', []))
                )
            except Exception as e:
                logger.warning(f"Не удалось сохранить рекомендации в новую таблицу: {e}", exc_info=True)

            return report_text

        except Exception as e:
            logger.error(f"Ошибка генерации отчета: {e}", exc_info=True)
            return "Произошла ошибка при генерации отчета."

    async def _generate_recommendations_new(
        self,
        name: str,
        metrics: Dict[str, Any],
        dashboard_metrics: Optional[Dict[str, Any]],
        start_date: date_type,
        end_date: date_type
    ) -> str:
        """
        НОВАЯ логика генерации рекомендаций через RecommendationsService.
        
        Использует:
        1. Проблемные звонки из analytics_repo
        2. Dashboard метрики
        3. LLM через recommendations_service
        """
        try:
            # Получаем звонки для анализа
            calls_data = await self.analytics_repo.get_calls_for_recommendations(
                operator_name=name,
                date_from=start_date,
                date_to=end_date,
                limit=10
            )
            
            # Собираем статистику для контекста
            stats = {
                'accepted_calls': dashboard_metrics.get('accepted_calls', 0) if dashboard_metrics else metrics.get('accepted_calls', 0),
                'records': dashboard_metrics.get('records_count', 0) if dashboard_metrics else metrics.get('booked_services', 0),
                'conversion_rate': dashboard_metrics.get('conversion_rate', 0) if dashboard_metrics else metrics.get('conversion_rate_leads', 0),
                'avg_score_all': dashboard_metrics.get('avg_score_all', 0) if dashboard_metrics else metrics.get('avg_call_rating', 0),
                'complaint_calls': dashboard_metrics.get('complaint_calls', 0) if dashboard_metrics else metrics.get('complaint_calls', 0)
            }
            
            # Генерируем через новый сервис
            recommendations = await self.recommendations_service.generate_operator_recommendations(
                operator_name=name,
                calls_data=calls_data,
                stats=stats
            )
            
            return recommendations
            
        except Exception as e:
            logger.error(f"Ошибка генерации рекомендаций через новый сервис: {e}", exc_info=True)
            # Fallback на старую логику
            return await self._generate_recommendations_fallback(name, metrics)

            return await self._generate_recommendations_fallback(name, metrics)
    
    async def _generate_recommendations_fallback(
        self,
        name: str,
        metrics: Dict[str, Any]
    ) -> str:
        """Fallback на старую логику, если новый сервис не работает."""
        try:
            # Используем старую логику через OpenAI
            results_text = f"Оценка: {metrics.get('avg_call_rating', 0)}, Конверсия: {metrics.get('conversion_rate_leads', 0)}%"
            
            prompt = (
                f"Данные оператора {name}:\n{results_text}\n\n"
                f"Дай краткие рекомендации для улучшения работы."
            )
            return await self.openai.generate_recommendations(prompt)
        except Exception as e:
            logger.error(f"Ошибка fallback рекомендаций: {e}", exc_info=True)
            return "Рекомендации временно недоступны."

    def _format_report_new(
        self, 
        name: str, 
        start: datetime.datetime, 
        end: datetime.datetime, 
        metrics: Dict[str, Any],
        dashboard_metrics: Optional[Dict[str, Any]],
        recommendations: str
    ) -> str:
        """
        НОВЫЙ формат отчета с dashboard метриками.
        """
        period_str = f"{start.strftime('%d.%m.%Y')} - {end.strftime('%d.%m.%Y')}"
        
        # Используем dashboard метрики если доступны, иначе старые
        if dashboard_metrics:
            total_calls = dashboard_metrics.get('accepted_calls', 0)
            records = dashboard_metrics.get('records_count', 0)
            leads = dashboard_metrics.get('leads_no_record', 0)
            conversion = dashboard_metrics.get('conversion_rate', 0)
            avg_score = dashboard_metrics.get('avg_score_all', 0)
            avg_score_leads = dashboard_metrics.get('avg_score_leads', 0)
            cancel_calls = dashboard_metrics.get('cancel_calls', 0)
            complaint_calls = dashboard_metrics.get('complaint_calls', 0)
            avg_talk_time = dashboard_metrics.get('avg_talk_all', 0)
        else:
            total_calls = metrics.get('accepted_calls', 0)
            records = metrics.get('booked_services', 0)
            leads = metrics.get('total_leads', 0)
            conversion = metrics.get('conversion_rate_leads', 0)
            avg_score = metrics.get('avg_call_rating', 0)
            avg_score_leads = avg_score
            cancel_calls = metrics.get('total_cancellations', 0)
            complaint_calls = metrics.get('complaint_calls', 0)
            avg_talk_time = int(metrics.get('avg_conversation_time', 0))
        
        # Форматируем время
        talk_mins = avg_talk_time // 60
        talk_secs = avg_talk_time % 60
        
        lines = [
            f"📊 <b>Отчет для оператора: {name}</b>",
            f"📅 Период: {period_str}",
            "",
            "<b>1️⃣ Общая статистика:</b>",
            f"   • Всего звонков: {total_calls}",
            f"   • Лиды / Записи: {records}",
            f"   • Лиды без записи: {leads}",
            f"   • Конверсия: <b>{conversion}%</b>",
            "",
            "<b>2️⃣ Качество:</b>",
            f"   • Средняя оценка: {avg_score:.1f}/10",
            f"   • Оценка лидов: {avg_score_leads:.1f}/10",
            "",
            "<b>3️⃣ Время:</b>",
            f"   • Среднее время разговора: {talk_mins}:{talk_secs:02d}",
            "",
            "<b>4️⃣ Проблемы:</b>",
            f"   • Отмен/переносов: {cancel_calls}",
            f"   • Жалоб: {complaint_calls}",
            "",
            "<b>💡 Рекомендации:</b>",
            recommendations
        ]
        return "\n".join(lines)

    def _format_report(
        self, 
        name: str, 
        start: datetime.datetime, 
        end: datetime.datetime, 
        metrics: Dict[str, Any], 
        recommendations: str
    ) -> str:
        """СТАРЫЙ формат отчета (для обратной совместимости)."""
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
                except ValueError as exc:
                    logger.debug("Дата '%s' не соответствует формату YYYY-MM-DD: %s", date_range, exc)
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
