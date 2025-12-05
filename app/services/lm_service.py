"""
Сервис расчета LM метрик.

LM (Learning/Logic Model) - аналитический слой для расчета метрик по звонкам.
Рассчитывает 6 категорий метрик: операционные, конверсионные, качество, риски, прогнозы, вспомогательные.
"""

from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

from app.db.repositories.lm_repository import LMRepository
from app.db.models import CallRecord, CallHistoryRecord
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)

# LM Configuration
LM_VERSION = "lm_v2_refactored"
DEFAULT_CALC_METHOD = "rule"


class LMService:
    """Сервис расчета метрик LM."""
    
    def __init__(self, lm_repository: LMRepository, lm_version: str = LM_VERSION):
        self.repo = lm_repository
        self.lm_version = lm_version

    # ============================================================================
    # PRIVATE CALCULATION METHODS - OPERATIONAL
    # ============================================================================

    def _calculate_response_speed(
        self,
        call_history: CallHistoryRecord,
        call_score: Optional[CallRecord] = None
    ) -> float:
        """
        Рассчитывает скорость отклика оператора.
        
        Returns:
            85.0 для принятых звонков
            20.0 для пропущенных (talk_duration = 0)
        """
        talk_duration = float(call_history.get('talk_duration', 0))
        
        if talk_duration == 0:
            return 20.0  # Missed call - low score
        
        return 85.0  # Answered call - high score

    def _calculate_talk_efficiency(
        self,
        call_history: CallHistoryRecord,
        call_score: Optional[CallRecord] = None
    ) -> float:
        """
        Рассчитывает эффективность разговора по длительности.
        
        Оптимум: 60-180 секунд → 100
        Короткие (<30 сек) или длинные (>300 сек) → пониженный скор
        """
        talk_duration = float(call_history.get('talk_duration', 0))
        
        if talk_duration <= 0:
            return 0.0
        
        if talk_duration >= 60:
            return talk_duration / 3
        
        return talk_duration * 2

    def _calculate_queue_impact(
        self,
        call_history: CallHistoryRecord,
        call_score: Optional[CallRecord] = None
    ) -> float:
        """
        Рассчитывает влияние на очередь.
        Чем дольше звонок, тем больше влияние.
        """
        talk_duration = float(call_history.get('talk_duration', 0))
        if talk_duration <= 0:
            return 0.0
        
        impact = (talk_duration / 300) * 100
        return round(impact, 1)

    # ============================================================================
    # PRIVATE CALCULATION METHODS - CONVERSION
    # ============================================================================

    def _calculate_conversion_score(
        self,
        call_score: Optional[CallRecord]
    ) -> float:
        """
        Рассчитывает скор конверсии.
        
        Returns:
            100.0 - для записи (outcome='record')
            50.0 - для лида без записи
            0.0 - для info_only/non_target
        """
        if not call_score:
            return 0.0
        
        outcome = call_score.get('outcome', '')
        category = call_score.get('call_category')
        
        if outcome == 'record':
            return 100.0
        elif outcome == 'lead_no_record':
            return 50.0
        elif outcome == 'info_only' or category == 'Информационный':
            return 20.0
        else:
            return 0.0

    def _calculate_cross_sell_potential(
        self,
        call_history: CallHistoryRecord,
        call_score: Optional[CallRecord] = None
    ) -> float:
        """
        Оценивает потенциал кросс-продаж.
        
        Базируется на:
        - Длительности разговора (больше = заинтересованность)
        - Категории звонка
        """
        if not call_score:
            return 0.0
        
        outcome = call_score.get('outcome')
        call_success = call_score.get('call_success')
        
        if outcome == 'record' or call_success == 'record':
            return 70.0
        
        return 0.0

    def _calculate_lost_opportunity(
        self,
        call_score: Optional[CallRecord]
    ) -> float:
        """
        Оценивает упущенную возможность.
        
        Высокий скор = упущенная конверсия (лид без записи, отмена)
        """
        if not call_score:
            return 0.0
        
        outcome = call_score.get('outcome', '')
        category = (call_score.get('call_category') or '').lower()
        is_target = call_score.get('is_target', 0)
        
        if is_target == 1 and outcome != 'record':
            return 80.0
        
        if outcome == 'lead_no_record':
            return 80.0  # Упущенный лид
        elif outcome == 'cancel':
            return 70.0  # Отмена
        elif 'отмена' in category or 'перенос' in category:
            return 50.0
        else:
            return 0.0

    # ============================================================================
    # PRIVATE CALCULATION METHODS - QUALITY
    # ============================================================================

    def _calculate_checklist_coverage(
        self,
        call_score: Optional[CallRecord]
    ) -> float:
        """
        Рассчитывает покрытие чек-листа.
        
        number_checklist: 0-10 → 0-100%
        """
        if not call_score:
            return 0.0
        
        checklist = call_score.get('number_checklist', 0)
        if checklist is None:
            checklist = 0
        
        return min(100.0, float(checklist) * 10)

    def _calculate_normalized_score(
        self,
        call_score: Optional[CallRecord]
    ) -> float:
        """
        Нормализует оценку звонка к шкале 0-100.
        
        call_score: 0-10 → 0-100
        """
        if not call_score:
            return 0.0
        
        score = call_score.get('call_score', 0)
        if score is None:
            score = 0
        
        return float(score) * 10

    def _calculate_script_risk(
        self,
        call_score: Optional[CallRecord]
    ) -> float:
        """
        Рассчитывает риск отклонения от скрипта.
        
        Высокий риск = низкая оценка и/или проблемная категория
        """
        if not call_score:
            return 50.0
        
        score = float(call_score.get('call_score', 5) or 5)
        category = (call_score.get('call_category') or '').lower()
        
        # Базовый риск от оценки (инвертировано)
        base_risk = (10 - score) * 10  # 0-100
        
        # Жалобы увеличивают риск
        if 'жалоба' in category:
            return min(100.0, base_risk + 30.0)
        
        # Низкая оценка = высокий риск
        if score < 5:
            return 80.0
        elif score > 7:
            return 10.0
        
        return base_risk

    # ============================================================================
    # PRIVATE CALCULATION METHODS - RISK
    # ============================================================================

    def _calculate_churn_risk(
        self,
        call_score: Optional[CallRecord]
    ) -> Tuple[str, float]:
        """
        Рассчитывает уровень риска оттока.
        
        Returns:
            (level: str, score: float)
            level: 'low', 'medium', 'high'
            score: 0-100
        """
        if not call_score:
            return ('medium', 50.0)
        
        raw_category = call_score.get('call_category') or ''
        category = raw_category.lower()
        outcome = call_score.get('outcome', '')
        refusal = call_score.get('refusal_reason')
        
        if raw_category == 'Отмена записи':
            return ('high', 70.0)
        
        # Высокий риск
        if 'жалоба' in category:
            return ('high', 90.0)
        if outcome == 'cancel' or refusal:
            return ('high', 80.0)
        
        # Низкий риск
        if outcome == 'record':
            return ('low', 10.0)
        if 'запись' in category and 'успеш' in category:
            return ('low', 15.0)
        
        # Средний риск
        if outcome == 'lead_no_record':
            return ('medium', 50.0)
        
        return ('medium', 40.0)

    def _calculate_followup_needed(
        self,
        call_score: Optional[CallRecord]
    ) -> bool:
        """
        Определяет, нужен ли follow-up.
        
        Returns:
            True - если нужен follow-up
            False - если не нужен
        """
        if not call_score:
            return False
        
        outcome = call_score.get('outcome', '')
        category = (call_score.get('call_category') or '').lower()
        
        # Нужен follow-up для лидов без записи
        if outcome == 'lead_no_record':
            return True
        
        # Нужен для отмен
        if outcome == 'cancel' or 'отмена' in category:
            return True
        
        # Нужен для жалоб
        if 'жалоба' in category:
            return True
        
        return False

    def _calculate_complaint_risk(
        self,
        call_score: Optional[CallRecord]
    ) -> Tuple[float, bool]:
        """
        Определяет флаг риска жалобы и скор.
        """
        if not call_score:
            return (10.0, False)
        
        category = call_score.get('call_category')
        normalized_category = (category or '').lower()
        score = float(call_score.get('call_score', 0) or 0)
        
        if category == 'Жалоба' or 'жалоба' in normalized_category:
            return (100.0, True)
        if score <= 3:
            return (70.0, True)
        
        return (10.0, False)

    # ============================================================================
    # PRIVATE CALCULATION METHODS - FORECAST
    # ============================================================================

    def _forecast_conversion_probability(
        self,
        call_score: Optional[CallRecord]
    ) -> float:
        """
        Прогнозирует вероятность конверсии.
        
        Returns:
            Вероятность 0.0 - 1.0
        """
        if not call_score:
            return 0.1
        
        outcome = call_score.get('outcome', '')
        is_target = call_score.get('is_target', 0)
        
        if outcome == 'record':
            return 1.0
        
        if outcome == 'lead_no_record':
            return 0.35
        
        if is_target:
            return 0.25
        
        return 0.05

    def _forecast_second_call_probability(
        self,
        call_history: CallHistoryRecord,
        call_score: Optional[CallRecord] = None
    ) -> float:
        """
        Прогнозирует вероятность повторного звонка.
        """
        if not call_score:
            return 0.3
        
        outcome = call_score.get('outcome', '')
        category = (call_score.get('call_category') or '').lower()
        
        if outcome == 'lead_no_record':
            return 0.6
        
        if outcome == 'record':
            return 0.2  # Может позвонить для уточнения
        
        if 'жалоба' in category:
            return 0.7
        if 'навигация' in category:
            return 0.60
        
        return 0.3

    def _forecast_complaint_probability(
        self,
        call_score: Optional[CallRecord]
    ) -> float:
        """
        Прогнозирует вероятность жалобы.
        """
        if not call_score:
            return 0.05
        
        category = (call_score.get('call_category') or '').lower()
        score = float(call_score.get('call_score', 5) or 5)
        
        if 'жалоба' in category:
            return 1.0
        
        if score < 3:
            return 0.4
        elif score < 5:
            return 0.2
        
        return 0.05

    # ============================================================================
    # PRIVATE - DETERMINE CALCULATION PROFILE
    # ============================================================================

    def _determine_calc_profile(
        self, 
        call_history: CallHistoryRecord, 
        call_score: Optional[CallRecord]
    ) -> str:
        """Determine calculation profile/scenario."""
        call_date = call_history.get('call_date')
        
        if call_date:
            if isinstance(call_date, datetime):
                hour = call_date.hour
                weekday = call_date.weekday()
                
                if hour >= 22 or hour < 6:
                    return 'night_shift_v1'
                elif weekday >= 5:  # Sat/Sun
                    return 'weekend_v1'
        
        return 'default_v1'

    # ============================================================================
    # 1. OPERATIONAL METRICS (metric_group = 'operational')
    # ============================================================================

    def calculate_operational_metrics(
        self,
        call_history: CallHistoryRecord,
        call_score: Optional[CallRecord] = None
    ) -> List[Dict[str, Any]]:
        """
        Рассчитывает операционные метрики.
        """
        response_speed = self._calculate_response_speed(call_history, call_score)
        talk_efficiency = self._calculate_talk_efficiency(call_history, call_score)
        queue_impact = self._calculate_queue_impact(call_history, call_score)
        
        return [
            {
                'metric_code': 'response_speed_score',
                'metric_group': 'operational',
                'value_numeric': response_speed,
                'calc_method': 'rule'
            },
            {
                'metric_code': 'talk_time_efficiency',
                'metric_group': 'operational',
                'value_numeric': talk_efficiency,
                'calc_method': 'rule'
            },
            {
                'metric_code': 'queue_impact_index',
                'metric_group': 'operational',
                'value_numeric': queue_impact,
                'calc_method': 'rule'
            }
        ]

    # ============================================================================
    # 2. CONVERSION METRICS (metric_group = 'conversion')
    # ============================================================================

    def calculate_conversion_metrics(
        self,
        call_history: CallHistoryRecord,
        call_score: Optional[CallRecord] = None
    ) -> List[Dict[str, Any]]:
        """
        Рассчитывает конверсионные метрики.
        """
        conversion_score = self._calculate_conversion_score(call_score)
        lost_opportunity = self._calculate_lost_opportunity(call_score)
        cross_sell = self._calculate_cross_sell_potential(call_history, call_score)
        
        return [
            {
                'metric_code': 'conversion_score',
                'metric_group': 'conversion',
                'value_numeric': conversion_score,
                'calc_method': 'rule'
            },
            {
                'metric_code': 'lost_opportunity_score',
                'metric_group': 'conversion',
                'value_numeric': lost_opportunity,
                'calc_method': 'rule'
            },
            {
                'metric_code': 'cross_sell_potential',
                'metric_group': 'conversion',
                'value_numeric': cross_sell,
                'calc_method': 'rule'
            }
        ]

    # ============================================================================
    # 3. QUALITY METRICS (metric_group = 'quality')
    # ============================================================================

    def calculate_quality_metrics(
        self,
        call_history: CallHistoryRecord,
        call_score: Optional[CallRecord] = None
    ) -> List[Dict[str, Any]]:
        """
        Рассчитывает метрики качества.
        """
        checklist = self._calculate_checklist_coverage(call_score)
        normalized = self._calculate_normalized_score(call_score)
        script_risk = self._calculate_script_risk(call_score)
        
        return [
            {
                'metric_code': 'checklist_coverage_ratio',
                'metric_group': 'quality',
                'value_numeric': checklist,
                'calc_method': 'rule'
            },
            {
                'metric_code': 'normalized_call_score',
                'metric_group': 'quality',
                'value_numeric': normalized,
                'calc_method': 'rule'
            },
            {
                'metric_code': 'script_risk_index',
                'metric_group': 'quality',
                'value_numeric': script_risk,
                'calc_method': 'rule'
            }
        ]

    # ============================================================================
    # 4. RISK METRICS (metric_group = 'risk')
    # ============================================================================

    def calculate_risk_metrics(
        self,
        call_history: CallHistoryRecord,
        call_score: Optional[CallRecord] = None
    ) -> List[Dict[str, Any]]:
        """
        Рассчитывает метрики рисков.
        """
        churn_level, churn_score = self._calculate_churn_risk(call_score)
        followup_needed = self._calculate_followup_needed(call_score)
        complaint_score, complaint_flag = self._calculate_complaint_risk(call_score)
        
        return [
            {
                'metric_code': 'churn_risk_level',
                'metric_group': 'risk',
                'value_label': churn_level,
                'value_numeric': churn_score,
                'calc_method': 'rule'
            },
            {
                'metric_code': 'complaint_risk_flag',
                'metric_group': 'risk',
                'value_label': 'true' if complaint_flag else 'false',
                'value_numeric': complaint_score,
                'calc_method': 'rule'
            },
            {
                'metric_code': 'followup_needed_flag',
                'metric_group': 'risk',
                'value_label': 'true' if followup_needed else 'false',
                'value_numeric': 1.0 if followup_needed else 0.0,
                'calc_method': 'rule'
            }
        ]

    # ============================================================================
    # 5. FORECAST METRICS (metric_group = 'forecast')
    # ============================================================================

    def calculate_forecast_metrics(
        self,
        call_history: CallHistoryRecord,
        call_score: Optional[CallRecord] = None
    ) -> List[Dict[str, Any]]:
        """
        Рассчитывает прогнозные метрики.
        """
        conversion_prob = self._forecast_conversion_probability(call_score)
        second_call_prob = self._forecast_second_call_probability(call_history, call_score)
        complaint_prob = self._forecast_complaint_probability(call_score)
        
        return [
            {
                'metric_code': 'conversion_prob_forecast',
                'metric_group': 'forecast',
                'value_numeric': conversion_prob,
                'calc_method': 'rule'
            },
            {
                'metric_code': 'second_call_prob',
                'metric_group': 'forecast',
                'value_numeric': second_call_prob,
                'calc_method': 'rule'
            },
            {
                'metric_code': 'complaint_prob',
                'metric_group': 'forecast',
                'value_numeric': complaint_prob,
                'calc_method': 'rule'
            }
        ]

    # ============================================================================
    # 6. AUXILIARY METRICS (metric_group = 'aux')
    # ============================================================================

    def calculate_auxiliary_metrics(
        self,
        call_history: CallHistoryRecord,
        call_score: Optional[CallRecord] = None,
        calc_source: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Рассчитывает вспомогательные метрики.
        """
        metrics = []
        
        # 6.1 LM Version tracking
        metrics.append({
            'metric_code': 'lm_version_tag',
            'metric_group': 'aux',
            'value_label': self.lm_version,
            'calc_method': 'meta'
        })
        
        # 6.2 Calculation Profile
        calc_profile = self._determine_calc_profile(call_history, call_score)
        if calc_profile:
            metrics.append({
                'metric_code': 'calc_profile',
                'metric_group': 'aux',
                'value_label': calc_profile,
                'calc_method': 'meta'
            })
        
        return metrics

    # ============================================================================
    # ORCHESTRATION: Calculate all metrics for a call
    # ============================================================================

    async def calculate_all_metrics(
        self,
        history_id: int,
        call_history: CallHistoryRecord,
        call_score: Optional[CallRecord] = None,
        calc_source: str = "batch_calculation"
    ) -> int:
        """
        Рассчитывает все метрики LM для звонка и сохраняет в БД.
        """
        all_metrics = []
        
        # Calculate each category
        all_metrics.extend(self.calculate_operational_metrics(call_history, call_score))
        all_metrics.extend(self.calculate_conversion_metrics(call_history, call_score))
        all_metrics.extend(self.calculate_quality_metrics(call_history, call_score))
        all_metrics.extend(self.calculate_risk_metrics(call_history, call_score))
        all_metrics.extend(self.calculate_forecast_metrics(call_history, call_score))
        all_metrics.extend(self.calculate_auxiliary_metrics(call_history, call_score, calc_source))
        
        # Prepare for batch save
        values_to_save = []
        call_score_id = call_score.get('id') if call_score else None
        
        for metric in all_metrics:
            values_to_save.append({
                'history_id': history_id,
                'call_score_id': call_score_id,
                'metric_code': metric['metric_code'],
                'metric_group': metric['metric_group'],
                'value_numeric': metric.get('value_numeric'),
                'value_label': metric.get('value_label'),
                'value_json': metric.get('value_json'),
                'lm_version': self.lm_version,
                'calc_method': metric.get('calc_method', DEFAULT_CALC_METHOD),
                'calc_source': calc_source
            })
        
        # Save to database
        saved_count = await self.repo.save_lm_values_batch(values_to_save)
        
        logger.info(f"Calculated and saved {saved_count} LM metrics for history_id={history_id}")
        return saved_count
