# Файл: app/db/repositories/analytics.py

"""
Repository для аналитики операторов и расчета метрик дашборда.
Реализует методы согласно документу МЛ_РАСЧЕТЫ.
"""

from __future__ import annotations

from typing import List, Dict, Any, Optional, Tuple
from datetime import date, datetime, timedelta, time

from app.db.manager import DatabaseManager
from app.db.models import DashboardMetrics, OperatorRecommendation
from app.db.repositories.call_analytics_repo import CallAnalyticsRepository
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)


class AnalyticsRepository:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.call_analytics_repo = CallAnalyticsRepository(db_manager)

    @staticmethod
    def _normalize_period(
        date_from: date | datetime,
        date_to: date | datetime,
    ) -> Tuple[datetime, datetime]:
        """
        Приводит границы периода к datetime (00:00:00 — 23:59:59).
        """
        start_dt = date_from if isinstance(date_from, datetime) else datetime.combine(date_from, time.min)
        end_dt = date_to if isinstance(date_to, datetime) else datetime.combine(date_to, time.max)
        return start_dt, end_dt

    # ========================================================================
    # Общая статистика по звонкам
    # ========================================================================

    async def get_operator_daily_stats(
        self,
        operator_name: str,
        date_from: date,
        date_to: date
    ) -> Dict[str, Any]:
        """
        Получить базовую статистику звонков оператора за период.
        
        Возвращает:
        - total_calls: всего звонков
        - accepted_calls: принято звонков
        - records: записей на услугу
        - leads_no_record: лидов без записи
        - wish_to_record: желающих записаться
        - conversion_rate: конверсия в запись (%)
        """
        logger.info(f"[ANALYTICS] Getting daily stats: operator={operator_name}, period={date_from} to {date_to}")
        
        try:
            period_start, period_end = self._normalize_period(date_from, date_to)
            # Используем call_analytics для быстрого доступа
            metrics = await self.call_analytics_repo.get_aggregated_metrics(
                operator_name, period_start, period_end
            )
            
            if not metrics:
                logger.warning(f"[ANALYTICS] No data in call_analytics for {operator_name}")
                # Fallback на call_scores если call_analytics пуст
                query = """
                SELECT 
                    COUNT(*) as accepted_calls,
                    SUM(CASE WHEN outcome = 'record' AND is_target = 1 THEN 1 ELSE 0 END) as records,
                    SUM(CASE WHEN outcome = 'lead_no_record' AND is_target = 1 THEN 1 ELSE 0 END) as leads_no_record,
                    SUM(CASE WHEN outcome IN ('record','lead_no_record') AND is_target = 1 THEN 1 ELSE 0 END) as wish_to_record
                FROM call_scores
                WHERE call_date BETWEEN %s AND %s
                  AND call_type = 'принятый'
                  AND (
                      (context_type = 'входящий' AND called_info = %s)
                      OR (context_type = 'исходящий' AND caller_info = %s)
                  )
                """
                
                result = await self.db_manager.execute_query(
                    query,
                    (period_start, period_end, operator_name, operator_name),
                    fetchone=True
                )
            else:
                # Используем данные из call_analytics
                result = metrics
            
            logger.debug(f"[ANALYTICS] Query executed, result: {result}")
            
            if not result or result.get('accepted_calls', 0) == 0:
                logger.warning(f"[ANALYTICS] No call data found for {operator_name} in period {date_from}-{date_to}")
                return {
                    'accepted_calls': 0,
                    'records': 0,
                    'leads_no_record': 0,
                    'wish_to_record': 0,
                    'conversion_rate': 0.0
                }
            
            accepted_calls = result.get('accepted_calls', 0)
            records = result.get('records', 0)
            leads_no_record = result.get('leads_no_record', 0)
            wish_to_record = result.get('wish_to_record', 0)
            
            # Конверсия = записи / желающие записаться
            conversion_rate = (records / wish_to_record * 100) if wish_to_record > 0 else 0.0
            
            logger.info(
                f"[ANALYTICS] Daily stats calculated: "
                f"calls={accepted_calls}, records={records}, "
                f"wish_to_record={wish_to_record}, conversion={conversion_rate:.2f}%"
            )
            
            return {
                'accepted_calls': accepted_calls,
                'records': records,
                'leads_no_record': leads_no_record,
                'wish_to_record': wish_to_record,
                'conversion_rate': round(conversion_rate, 2)
            }
        
        except Exception as e:
            logger.error(
                f"[ANALYTICS] Error getting daily stats for {operator_name}: {e}",
                exc_info=True
            )
            # Возвращаем пустую статистику при ошибке
            return {
                'accepted_calls': 0,
                'records': 0,
                'leads_no_record': 0,
                'wish_to_record': 0,
                'conversion_rate': 0.0
            }

    # ========================================================================
    # Метрики качества
    # ========================================================================

    async def get_quality_metrics(
        self,
        operator_name: str,
        date_from: date,
        date_to: date
    ) -> Dict[str, float]:
        """
        Получить метрики качества обработки звонков.
        
        Возвращает:
        - avg_score_all: средняя оценка всех звонков
        - avg_score_leads: средняя оценка звонков желающих записаться
        - avg_score_cancel: средняя оценка при отменах
        """
        logger.info(f"[ANALYTICS] Getting quality metrics for {operator_name}")
        
        try:
            period_start, period_end = self._normalize_period(date_from, date_to)
            query = """
            SELECT 
                AVG(CASE WHEN call_score IS NOT NULL THEN call_score END) as avg_score_all,
                AVG(CASE 
                    WHEN call_score IS NOT NULL 
                    AND is_target = 1 
                    AND outcome IN ('record','lead_no_record') 
                    THEN call_score 
                END) as avg_score_leads,
                AVG(CASE 
                    WHEN call_score IS NOT NULL 
                    AND call_category = 'Отмена записи'
                    AND is_target = 1
                    THEN call_score 
                END) as avg_score_cancel
            FROM call_scores
            WHERE call_date BETWEEN %s AND %s
              AND call_type = 'принятый'
              AND (
                  (context_type = 'входящий' AND called_info = %s)
                  OR (context_type = 'исходящий' AND caller_info = %s)
              )
            """
            
            result = await self.db_manager.execute_query(
                query,
                (period_start, period_end, operator_name, operator_name),
                fetchone=True
            )
            
            metrics = {
                'avg_score_all': round(result.get('avg_score_all', 0) or 0, 2),
                'avg_score_leads': round(result.get('avg_score_leads', 0) or 0, 2),
                'avg_score_cancel': round(result.get('avg_score_cancel', 0) or 0, 2)
            }
            
            logger.info(
                f"[ANALYTICS] Quality metrics: "
                f"avg_all={metrics['avg_score_all']}, "
                f"avg_leads={metrics['avg_score_leads']}, "
                f"avg_cancel={metrics['avg_score_cancel']}"
            )
            
            return metrics
        
        except Exception as e:
            logger.error(
                f"[ANALYTICS] Error getting quality metrics for {operator_name}: {e}",
                exc_info=True
            )
            return {
                'avg_score_all': 0.0,
                'avg_score_leads': 0.0,
                'avg_score_cancel': 0.0
            }

    # ========================================================================
    # Метрики отмен
    # ========================================================================

    async def get_cancellation_metrics(
        self,
        operator_name: str,
        date_from: date,
        date_to: date
    ) -> Dict[str, Any]:
        """
        Анализ отмен и переносов.
        
        Возвращает:
        - cancel_calls: количество отмен
        - reschedule_calls: количество переносов
        - cancel_share: доля отмен от (отмены + переносы), %
        """
        period_start, period_end = self._normalize_period(date_from, date_to)

        query = """
        SELECT 
            SUM(CASE WHEN call_category = 'Отмена записи' AND is_target = 1 THEN 1 ELSE 0 END) as cancel_calls,
            SUM(CASE WHEN call_category = 'Перенос записи' AND is_target = 1 THEN 1 ELSE 0 END) as reschedule_calls
        FROM call_scores
        WHERE call_date BETWEEN %s AND %s
          AND call_type = 'принятый'
          AND (
              (context_type = 'входящий' AND called_info = %s)
              OR (context_type = 'исходящий' AND caller_info = %s)
          )
        """
        
        result = await self.db_manager.execute_query(
            query,
            (period_start, period_end, operator_name, operator_name),
            fetchone=True
        )
        
        cancel_calls = result.get('cancel_calls', 0) or 0
        reschedule_calls = result.get('reschedule_calls', 0) or 0
        total_cancel_flow = cancel_calls + reschedule_calls
        
        cancel_share = (cancel_calls / total_cancel_flow * 100) if total_cancel_flow > 0 else 0.0
        
        return {
            'cancel_calls': cancel_calls,
            'reschedule_calls': reschedule_calls,
            'cancel_share': round(cancel_share, 2)
        }

    # ========================================================================
    # Метрики времени
    # ========================================================================

    async def get_time_metrics(
        self,
        operator_name: str,
        date_from: date,
        date_to: date
    ) -> Dict[str, Any]:
        """
        Метрики времени обработки звонков.
        
        Возвращает:
        - avg_talk_all: среднее время всех разговоров (сек)
        - total_talk_time: общее время разговоров (сек)
        - avg_talk_record: среднее время при записи (сек)
        - avg_talk_navigation: среднее время навигации (сек)
        - avg_talk_spam: среднее время со спамом (сек)
        """
        period_start, period_end = self._normalize_period(date_from, date_to)

        query = """
        SELECT 
            AVG(CASE WHEN talk_duration > 10 THEN talk_duration END) as avg_talk_all,
            SUM(CASE WHEN talk_duration > 0 THEN talk_duration ELSE 0 END) as total_talk_time,
            AVG(CASE 
                WHEN talk_duration > 10 
                AND call_category = 'Запись на услугу (успешная)' 
                THEN talk_duration 
            END) as avg_talk_record,
            AVG(CASE 
                WHEN talk_duration > 10 
                AND call_category = 'Навигация' 
                THEN talk_duration 
            END) as avg_talk_navigation,
            AVG(CASE 
                WHEN talk_duration > 10 
                AND call_category = 'Спам, реклама' 
                THEN talk_duration 
            END) as avg_talk_spam
        FROM call_scores
        WHERE call_date BETWEEN %s AND %s
          AND call_type = 'принятый'
          AND (
              (context_type = 'входящий' AND called_info = %s)
              OR (context_type = 'исходящий' AND caller_info = %s)
          )
        """
        
        result = await self.db_manager.execute_query(
            query,
            (period_start, period_end, operator_name, operator_name),
            fetchone=True
        )
        
        return {
            'avg_talk_all': int(result.get('avg_talk_all', 0) or 0),
            'total_talk_time': int(result.get('total_talk_time', 0) or 0),
            'avg_talk_record': int(result.get('avg_talk_record', 0) or 0),
            'avg_talk_navigation': int(result.get('avg_talk_navigation', 0) or 0),
            'avg_talk_spam': int(result.get('avg_talk_spam', 0) or 0)
        }

    # ========================================================================
    # Метрики жалоб
    # ========================================================================

    async def get_complaint_metrics(
        self,
        operator_name: str,
        date_from: date,
        date_to: date
    ) -> Dict[str, Any]:
        """
        Работа с жалобами.
        
        Возвращает:
        - complaint_calls: количество звонков с жалобами
        - avg_score_complaint: средняя оценка жалоб
        """
        period_start, period_end = self._normalize_period(date_from, date_to)

        query = """
        SELECT 
            COUNT(*) as complaint_calls,
            AVG(call_score) as avg_score_complaint
        FROM call_scores
        WHERE call_date BETWEEN %s AND %s
          AND call_type = 'принятый'
          AND call_category = 'Жалоба'
          AND is_target = 1
          AND (
              (context_type = 'входящий' AND called_info = %s)
              OR (context_type = 'исходящий' AND caller_info = %s)
          )
        """
        
        result = await self.db_manager.execute_query(
            query,
            (period_start, period_end, operator_name, operator_name),
            fetchone=True
        )
        
        return {
            'complaint_calls': result.get('complaint_calls', 0) or 0,
            'avg_score_complaint': round(result.get('avg_score_complaint', 0) or 0, 2)
        }

    # ========================================================================
    # Полный дашборд (агрегация всех метрик)
    # ========================================================================

    async def get_live_dashboard_single(
        self,
        operator_name: str,
        period_type: str = 'day'
    ) -> DashboardMetrics:
        """
        Получить полный дашборд для одного оператора.
        
        Args:
            operator_name: имя оператора
            period_type: 'day', 'week', 'month'
        
        Returns:
            DashboardMetrics со всеми метриками
        """
        logger.info(f"[ANALYTICS] Building dashboard: operator={operator_name}, period={period_type}")
        
        try:
            # Определяем период
            today = date.today()
            if period_type == 'day':
                date_from = today
                date_to = today
            elif period_type == 'week':
                date_from = today - timedelta(days=today.weekday())
                date_to = today
            else:  # month
                date_from = today.replace(day=1)
                date_to = today
            
            logger.debug(f"[ANALYTICS] Period calculated: {date_from} to {date_to}")
            
            # Собираем все метрики параллельно
            logger.debug(f"[ANALYTICS] Fetching metrics for {operator_name}...")
            
            stats = await self.get_operator_daily_stats(operator_name, date_from, date_to)
            quality = await self.get_quality_metrics(operator_name, date_from, date_to)
            cancellations = await self.get_cancellation_metrics(operator_name, date_from, date_to)
            time_metrics = await self.get_time_metrics(operator_name, date_from, date_to)
            complaints = await self.get_complaint_metrics(operator_name, date_from, date_to)
            
            # Объединяем в один dict
            dashboard: DashboardMetrics = {
                'operator_name': operator_name,
                'period_type': period_type,
                'period_start': date_from.isoformat(),
                'period_end': date_to.isoformat(),
                **stats,
                **quality,
                **cancellations,
                **time_metrics,
                **complaints
            }
            
            logger.info(
                f"[ANALYTICS] Dashboard built successfully for {operator_name}: "
                f"calls={dashboard.get('accepted_calls')}, conversion={dashboard.get('conversion_rate')}%"
            )
            
            return dashboard
        
        except Exception as e:
            logger.error(
                f"[ANALYTICS] Error building dashboard for {operator_name}: {e}",
                exc_info=True
            )
            # Возвращаем пустой дашборд при критической ошибке
            raise

    async def get_live_dashboard_all_operators(
        self,
        period_type: str = 'day'
    ) -> List[DashboardMetrics]:
        """
        Получить сводный дашборд по всем операторам.
        
        Returns:
            Список DashboardMetrics для каждого оператора
        """
        # Получаем список уникальных операторов
        operator_case = """
            CASE 
                WHEN context_type = 'входящий' THEN called_info
                ELSE caller_info
            END
        """
        query = f"""
        SELECT DISTINCT
            {operator_case} as operator_name
        FROM call_scores
        HAVING operator_name IS NOT NULL 
           AND operator_name != ''
        ORDER BY operator_name
        """
        
        operators_result = await self.db_manager.execute_query(query, fetchall=True)
        
        if not operators_result:
            return []
        
        # Получаем дашборд для каждого оператора
        dashboards = []
        for row in operators_result:
            operator_name = row.get('operator_name')
            if operator_name:
                dashboard = await self.get_live_dashboard_single(operator_name, period_type)
                dashboards.append(dashboard)
        
        return dashboards

    # ========================================================================
    # Звонки для рекомендаций
    # ========================================================================

    async def get_calls_for_recommendations(
        self,
        operator_name: str,
        date_from: date,
        date_to: date,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Получить звонки для анализа LLM и генерации рекомендаций.
        
        Выбирает звонки с:
        - низким call_score (< 7)
        - outcome IN ('lead_no_record', 'record') или категории Жалоба/Отмена
        
        Returns:
            Список звонков с полями: transcript, call_score, call_category, outcome, refusal_reason
        """
        period_start, period_end = self._normalize_period(date_from, date_to)

        query = """
        SELECT 
            id,
            history_id,
            transcript,
            call_score,
            call_category,
            outcome,
            refusal_reason,
            call_date
        FROM call_scores
        WHERE call_date BETWEEN %s AND %s
          AND (
              (context_type = 'входящий' AND called_info = %s)
              OR (context_type = 'исходящий' AND caller_info = %s)
          )
          AND (
              (call_score < 7 AND call_score IS NOT NULL)
              OR call_category IN ('Жалоба', 'Отмена записи', 'Лид (без записи)')
          )
          AND is_target = 1
          AND transcript IS NOT NULL
        ORDER BY call_score ASC, call_date DESC
        LIMIT %s
        """
        
        result = await self.db_manager.execute_query(
            query,
            (period_start, period_end, operator_name, operator_name, limit),
            fetchall=True
        )
        
        return result or []

    # ========================================================================
    # Сохранение рекомендаций
    # ========================================================================

    async def save_operator_recommendations(
        self,
        operator_name: str,
        report_date: date,
        recommendations: str,
        call_samples_analyzed: int
    ) -> bool:
        """
        Сохранить LLM-рекомендации для оператора в operator_recommendations.
        
        Args:
            operator_name: Имя оператора
            report_date: Дата отчета
            recommendations: Текст рекомендаций
            call_samples_analyzed: Количество проанализированных звонков
        
        Returns:
            True если успешно
        """
        logger.info(
            f"[ANALYTICS] Saving recommendations for {operator_name} "
            f"on {report_date}, samples={call_samples_analyzed}"
        )
        
        try:
            query = """
            INSERT INTO operator_recommendations 
                (operator_name, report_date, recommendations, call_samples_analyzed, generated_at)
            VALUES (%s, %s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE
                recommendations = VALUES(recommendations),
                call_samples_analyzed = VALUES(call_samples_analyzed),
                generated_at = NOW()
            """
            
            await self.db_manager.execute_query(
                query,
                (operator_name, report_date, recommendations, call_samples_analyzed)
            )
            
            logger.info(f"[ANALYTICS] Recommendations saved successfully for {operator_name}")
            return True
            
        except Exception as e:
            logger.error(
                f"[ANALYTICS] Error saving recommendations for {operator_name}: {e}",
                exc_info=True
            )
            return False

    async def get_operator_recommendations(
        self,
        operator_name: str,
        report_date: date
    ) -> Optional[Dict[str, Any]]:
        """
        Получить сохраненные рекомендации для оператора из operator_recommendations.
        
        Args:
            operator_name: Имя оператора
            report_date: Дата отчета (берет <= этой даты, последние)
        
        Returns:
            Dict с полями recommendations, call_samples_analyzed, generated_at
            или None если нет
        """
        logger.info(
            f"[ANALYTICS] Getting recommendations for {operator_name} on/before {report_date}"
        )
        
        try:
            query = """
            SELECT 
                recommendations,
                call_samples_analyzed,
                generated_at,
                report_date
            FROM operator_recommendations
            WHERE operator_name = %s
              AND report_date <= %s
            ORDER BY report_date DESC
            LIMIT 1
            """
            
            result = await self.db_manager.execute_query(
                query,
                (operator_name, report_date),
                fetchone=True
            )
            
            if result:
                logger.info(
                    f"[ANALYTICS] Found recommendations for {operator_name} "
                    f"from {result.get('report_date')}"
                )
                return dict(result)
            else:
                logger.info(f"[ANALYTICS] No recommendations found for {operator_name}")
                return None
                
        except Exception as e:
            logger.error(
                f"[ANALYTICS] Error getting recommendations for {operator_name}: {e}",
                exc_info=True
            )
            return None
