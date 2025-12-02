"""
Сервис расчета LM метрик.

LM (Learning/Logic Model) - аналитический слой для расчета метрик по звонкам.
Рассчитывает 6 категорий метрик: операционные, конверсионные, качество, риски, прогнозы, вспомогательные.
"""

from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
import math

from app.db.repositories.lm_repository import LMRepository
from app.db.models import CallRecord, CallHistoryRecord
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)

# LM Configuration
LM_VERSION = "lm_v1"
DEFAULT_CALC_METHOD = "rule"


class LMService:
    """Сервис расчета метрик LM."""
    
    def __init__(self, lm_repository: LMRepository, lm_version: str = LM_VERSION):
        self.repo = lm_repository
        self.lm_version = lm_version

    # ============================================================================
    # 1. OPERATIONAL METRICS (metric_group = 'operational')
    # ============================================================================

    def calculate_operational_metrics(
        self,
        call_history: CallHistoryRecord,
        call_score: Optional[CallRecord] = None
    ) -> List[Dict[str, Any]]:
        """
        Рассчитывает операционные метрики: скорость реакции, эффективность разговора, влияние на очередь.
        
        Args:
            call_history: Данные из call_history
            call_score: Данные из call_scores (опционально)
            
        Returns:
            Список метрик для сохранения
        """
        metrics = []
        
        # 1.1 Response Speed Score (based on call pickup time, hour, SLA)
        # Simplified: higher score = faster response
        response_speed = self._calculate_response_speed(call_history, call_score)
        if response_speed is not None:
            metrics.append({
                'metric_code': 'response_speed_score',
                'metric_group': 'operational',
                'value_numeric': response_speed,
                'calc_method': 'rule'
            })
        
        # 1.2 Talk Time Efficiency (ratio of productive talk time)
        # Simplified: talk_duration vs total duration
        talk_efficiency = self._calculate_talk_efficiency(call_history, call_score)
        if talk_efficiency is not None:
            metrics.append({
                'metric_code': 'talk_time_efficiency',
                'metric_group': 'operational',
                'value_numeric': talk_efficiency,
                'calc_method': 'rule'
            })
        
        # 1.3 Queue Impact Index (load on the call center)
        queue_impact = self._calculate_queue_impact(call_history, call_score)
        if queue_impact is not None:
            metrics.append({
                'metric_code': 'queue_impact_index',
                'metric_group': 'operational',
                'value_numeric': queue_impact,
                'calc_method': 'rule'
            })
        
        return metrics

    def _calculate_response_speed(
        self, 
        call_history: CallHistoryRecord, 
        call_score: Optional[CallRecord]
    ) -> Optional[float]:
        """Response speed score (0-100): higher = better."""
        # Simplified heuristic: if call was answered, score is high
        # In real implementation, use await_sec or pickup time
        talk_duration = call_history.get('talk_duration') or 0
        
        if talk_duration > 0:
            # Call was answered
            return 85.0  # Base high score for answered calls
        else:
            # Call was not answered or missed
            return 20.0

    def _calculate_talk_efficiency(
        self, 
        call_history: CallHistoryRecord, 
        call_score: Optional[CallRecord]
    ) -> Optional[float]:
        """Talk efficiency score (0-100): ratio of productive talk time."""
        talk_duration = call_history.get('talk_duration') or 0
        
        if talk_duration <= 0:
            return 0.0
        
        # Simplified: assume calls >30 sec are productive
        if talk_duration > 30:
            return min(100.0, talk_duration / 3.0)  # Scale up to 100
        else:
            return talk_duration * 2.0  # Short calls get lower efficiency

    def _calculate_queue_impact(
        self, 
        call_history: CallHistoryRecord, 
        call_score: Optional[CallRecord]
    ) -> Optional[float]:
        """Queue impact index (0-100): higher = more load."""
        talk_duration = call_history.get('talk_duration') or 0
        
        # Simplified: longer calls = higher impact
        impact = min(100.0, (talk_duration / 300.0) * 100)  # 5 min = 100
        return impact

    # ============================================================================
    # 2. CONVERSION METRICS (metric_group = 'conversion')
    # ============================================================================

    def calculate_conversion_metrics(
        self,
        call_history: CallHistoryRecord,
        call_score: Optional[CallRecord] = None
    ) -> List[Dict[str, Any]]:
        """
        Рассчитывает конверсионные/бизнес метрики: конверсия, потери, cross-sell потенциал.
        """
        metrics = []
        
        if not call_score:
            return metrics
        
        # 2.1 Conversion Score (probability/score that call converted to booking)
        conversion_score = self._calculate_conversion_score(call_score)
        if conversion_score is not None:
            metrics.append({
                'metric_code': 'conversion_score',
                'metric_group': 'conversion',
                'value_numeric': conversion_score,
                'calc_method': 'rule'
            })
        
        # 2.2 Lost Opportunity Score (value of loss if not converted)
        lost_opportunity = self._calculate_lost_opportunity(call_score)
        if lost_opportunity is not None:
            metrics.append({
                'metric_code': 'lost_opportunity_score',
                'metric_group': 'conversion',
                'value_numeric': lost_opportunity,
                'calc_method': 'rule'
            })
        
        # 2.3 Cross-sell Potential (potential for additional services)
        cross_sell = self._calculate_cross_sell_potential(call_score)
        if cross_sell is not None:
            metrics.append({
                'metric_code': 'cross_sell_potential',
                'metric_group': 'conversion',
                'value_numeric': cross_sell,
                'calc_method': 'rule'
            })
        
        return metrics

    def _calculate_conversion_score(self, call_score: CallRecord) -> Optional[float]:
        """Conversion score (0-100): probability call converted to booking."""
        outcome = call_score.get('outcome', '')
        call_category = call_score.get('call_category', '')
        
        # Direct conversion indicators
        if outcome == 'record' or call_category == 'Запись на услугу (успешная)':
            return 100.0
        elif outcome == 'lead_no_record' or call_category == 'Лид (без записи)':
            return 50.0  # Potential conversion
        elif call_category in ['Навигация', 'Информационный']:
            return 20.0  # Some potential
        else:
            return 10.0  # Low conversion

    def _calculate_lost_opportunity(self, call_score: CallRecord) -> Optional[float]:
        """Lost opportunity score (0-100): value lost if not converted."""
        outcome = call_score.get('outcome', '')
        is_target = call_score.get('is_target', 0)
        
        # If target call but not converted = high loss
        if is_target == 1 and outcome != 'record':
            return 80.0
        elif is_target == 1:
            return 0.0  # No loss, converted
        else:
            return 20.0  # Minor loss for non-target

    def _calculate_cross_sell_potential(self, call_score: CallRecord) -> Optional[float]:
        """Cross-sell potential (0-100): potential for additional services."""
        outcome = call_score.get('outcome', '')
        requested_service = call_score.get('requested_service_name', '')
        
        # If already booked, high cross-sell potential
        if outcome == 'record':
            return 70.0
        elif requested_service:
            return 40.0  # Showed interest
        else:
            return 10.0  # Low potential

    # ============================================================================
    # 3. QUALITY METRICS (metric_group = 'quality')
    # ============================================================================

    def calculate_quality_metrics(
        self,
        call_history: CallHistoryRecord,
        call_score: Optional[CallRecord] = None
    ) -> List[Dict[str, Any]]:
        """
        Рассчитывает метрики качества: покрытие чек-листа, нормализованный скор, риск нарушения скрипта.
        """
        metrics = []
        
        if not call_score:
            return metrics
        
        # 3.1 Checklist Coverage Ratio (how well checklist was followed)
        checklist_coverage = self._calculate_checklist_coverage(call_score)
        if checklist_coverage is not None:
            metrics.append({
                'metric_code': 'checklist_coverage_ratio',
                'metric_group': 'quality',
                'value_numeric': checklist_coverage,
                'calc_method': 'rule'
            })
        
        # 3.2 Normalized Call Score (standardized 0-100 scale)
        normalized_score = self._calculate_normalized_score(call_score)
        if normalized_score is not None:
            metrics.append({
                'metric_code': 'normalized_call_score',
                'metric_group': 'quality',
                'value_numeric': normalized_score,
                'calc_method': 'rule'
            })
        
        # 3.3 Script Risk Index (risk of script deviation)
        script_risk = self._calculate_script_risk(call_score)
        if script_risk is not None:
            metrics.append({
                'metric_code': 'script_risk_index',
                'metric_group': 'quality',
                'value_numeric': script_risk,
                'calc_method': 'rule'
            })
        
        return metrics

    def _calculate_checklist_coverage(self, call_score: CallRecord) -> Optional[float]:
        """Checklist coverage ratio (0-100): percentage of checklist completed."""
        number_checklist = call_score.get('number_checklist', 0)
        
        if number_checklist is None:
            return 50.0  # Default/unknown
        
        # Assume max checklist items is 10-15
        coverage = min(100.0, (number_checklist / 10.0) * 100)
        return coverage

    def _calculate_normalized_score(self, call_score: CallRecord) -> Optional[float]:
        """Normalized call score (0-100): standardized scale."""
        call_score_value = call_score.get('call_score')
        
        if call_score_value is None:
            return None
        
        # Assume call_score is already 0-100 or 0-10 scale
        score_float = float(call_score_value)
        
        # Normalize to 0-100
        if score_float <= 10:
            normalized = score_float * 10.0
        else:
            normalized = score_float
        
        return max(0.0, min(100.0, normalized))

    def _calculate_script_risk(self, call_score: CallRecord) -> Optional[float]:
        """Script risk index (0-100): risk of script deviation."""
        call_score_value = call_score.get('call_score', 0)
        call_category = call_score.get('call_category', '')
        
        # Low score = high risk
        score_float = float(call_score_value) if call_score_value else 0
        
        if score_float <= 3:
            risk = 80.0
        elif score_float <= 5:
            risk = 50.0
        elif score_float <= 7:
            risk = 30.0
        else:
            risk = 10.0
        
        # Certain categories have higher inherent risk
        if call_category in ['Жалоба', 'Отмена записи']:
            risk = min(100.0, risk + 20.0)
        
        return risk

    # ============================================================================
    # 4. RISK METRICS (metric_group = 'risk')
    # ============================================================================

    def calculate_risk_metrics(
        self,
        call_history: CallHistoryRecord,
        call_score: Optional[CallRecord] = None
    ) -> List[Dict[str, Any]]:
        """
        Рассчитывает метрики рисков: риск оттока, риск жалобы, необходимость фоллоу-апа.
        """
        metrics = []
        
        if not call_score:
            return metrics
        
        # 4.1 Churn Risk Level (risk of customer not returning)
        churn_risk_level, churn_risk_score = self._calculate_churn_risk(call_score)
        if churn_risk_level:
            metrics.append({
                'metric_code': 'churn_risk_level',
                'metric_group': 'risk',
                'value_label': churn_risk_level,
                'value_numeric': churn_risk_score,
                'calc_method': 'rule'
            })
        
        # 4.2 Complaint Risk Flag (risk of complaint)
        complaint_risk, complaint_flag = self._calculate_complaint_risk(call_score)
        if complaint_flag is not None:
            metrics.append({
                'metric_code': 'complaint_risk_flag',
                'metric_group': 'risk',
                'value_label': 'true' if complaint_flag else 'false',
                'value_numeric': complaint_risk,
                'calc_method': 'rule'
            })
        
        # 4.3 Followup Needed Flag (needs follow-up call)
        followup_needed = self._calculate_followup_needed(call_score)
        if followup_needed is not None:
            metrics.append({
                'metric_code': 'followup_needed_flag',
                'metric_group': 'risk',
                'value_label': 'true' if followup_needed else 'false',
                'value_numeric': 1.0 if followup_needed else 0.0,
                'calc_method': 'rule'
            })
        
        return metrics

    def _calculate_churn_risk(self, call_score: CallRecord) -> Tuple[Optional[str], Optional[float]]:
        """Churn risk level and score."""
        outcome = call_score.get('outcome', '')
        refusal_reason = call_score.get('refusal_reason', '')
        call_category = call_score.get('call_category', '')
        
        score = 0.0
        
        # High churn risk indicators
        if call_category == 'Жалоба':
            score = 90.0
        elif call_category == 'Отмена записи':
            score = 70.0
        elif outcome in ['refusal', 'no_interest']:
            score = 60.0
        elif refusal_reason:
            score = 50.0
        elif outcome == 'record':
            score = 10.0  # Low risk if booked
        else:
            score = 30.0  # Medium default
        
        # Determine level
        if score >= 70:
            level = 'high'
        elif score >= 40:
            level = 'medium'
        else:
            level = 'low'
        
        return level, score

    def _calculate_complaint_risk(self, call_score: CallRecord) -> Tuple[Optional[float], bool]:
        """Complaint risk score and flag."""
        call_category = call_score.get('call_category', '')
        call_score_value = call_score.get('call_score', 0)
        
        score = 0.0
        
        # Direct complaint
        if call_category == 'Жалоба':
            score = 100.0
        # Low quality call
        elif call_score_value and float(call_score_value) < 3:
            score = 60.0
        # Cancellation
        elif call_category == 'Отмена записи':
            score = 40.0
        else:
            score = 10.0
        
        flag = score >= 50.0
        return score, flag

    def _calculate_followup_needed(self, call_score: CallRecord) -> bool:
        """Whether follow-up is needed."""
        outcome = call_score.get('outcome', '')
        call_category = call_score.get('call_category', '')
        
        # Followup needed for leads without booking
        if outcome == 'lead_no_record':
            return True
        
        # Followup for certain categories
        if call_category in ['Жалоба', 'Лид (без записи)']:
            return True
        
        return False

    # ============================================================================
    # 5. FORECAST METRICS (metric_group = 'forecast')
    # ============================================================================

    def calculate_forecast_metrics(
        self,
        call_history: CallHistoryRecord,
        call_score: Optional[CallRecord] = None
    ) -> List[Dict[str, Any]]:
        """
        Рассчитывает прогнозные метрики: вероятность конверсии, повторного звонка, жалобы.
        """
        metrics = []
        
        if not call_score:
            return metrics
        
        # 5.1 Conversion Probability Forecast (will this lead convert?)
        conversion_prob = self._forecast_conversion_probability(call_score)
        if conversion_prob is not None:
            metrics.append({
                'metric_code': 'conversion_prob_forecast',
                'metric_group': 'forecast',
                'value_numeric': conversion_prob,
                'calc_method': 'rule'
            })
        
        # 5.2 Second Call Probability (will they call again?)
        second_call_prob = self._forecast_second_call_probability(call_score)
        if second_call_prob is not None:
            metrics.append({
                'metric_code': 'second_call_prob',
                'metric_group': 'forecast',
                'value_numeric': second_call_prob,
                'calc_method': 'rule'
            })
        
        # 5.3 Complaint Probability (will this escalate to complaint?)
        complaint_prob = self._forecast_complaint_probability(call_score)
        if complaint_prob is not None:
            metrics.append({
                'metric_code': 'complaint_prob',
                'metric_group': 'forecast',
                'value_numeric': complaint_prob,
                'calc_method': 'rule'
            })
        
        return metrics

    def _forecast_conversion_probability(self, call_score: CallRecord) -> Optional[float]:
        """Forecast conversion probability (0-1)."""
        outcome = call_score.get('outcome', '')
        is_target = call_score.get('is_target', 0)
        
        # Already converted
        if outcome == 'record':
            return 1.0
        
        # Lead without booking
        if outcome == 'lead_no_record':
            return 0.35  # 35% chance of converting later
        
        # Target call
        if is_target == 1:
            return 0.20  # Some chance
        
        return 0.05  # Low chance

    def _forecast_second_call_probability(self, call_score: CallRecord) -> Optional[float]:
        """Forecast second call probability (0-1)."""
        outcome = call_score.get('outcome', '')
        call_category = call_score.get('call_category', '')
        
        # Navigation/info calls likely to call back
        if call_category in ['Навигация', 'Информационный']:
            return 0.60
        
        # Leads without booking
        if outcome == 'lead_no_record':
            return 0.45
        
        # Booked, less likely to call again (unless issue)
        if outcome == 'record':
            return 0.15
        
        return 0.25  # Default

    def _forecast_complaint_probability(self, call_score: CallRecord) -> Optional[float]:
        """Forecast complaint probability (0-1)."""
        call_category = call_score.get('call_category', '')
        call_score_value = call_score.get('call_score', 0)
        
        # Already a complaint
        if call_category == 'Жалоба':
            return 1.0
        
        # Low quality calls
        if call_score_value and float(call_score_value) < 3:
            return 0.40
        
        # Cancellations
        if call_category == 'Отмена записи':
            return 0.25
        
        return 0.05  # Low default

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
        Рассчитывает вспомогательные метрики: версия LM, профиль расчета.
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

    def _determine_calc_profile(
        self, 
        call_history: CallHistoryRecord, 
        call_score: Optional[CallRecord]
    ) -> str:
        """Determine calculation profile/scenario."""
        # Simplified: determine if night shift, weekend, campaign, etc.
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
        
        Args:
            history_id: ID из call_history
            call_history: Данные звонка из call_history
            call_score: Данные из call_scores (опционально)
            calc_source: Источник расчета
            
        Returns:
            Количество сохраненных метрик
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
