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
from app.ml.models import CallScorer, ChurnPredictor, UpsellRecommender

logger = get_watchdog_logger(__name__)

# LM Configuration
LM_VERSION = "lm_v2_refactored"
DEFAULT_CALC_METHOD = "rule"


class LMService:
    """Сервис расчета метрик LM."""
    
    def __init__(self, lm_repository: LMRepository, lm_version: str = LM_VERSION):
        self.repo = lm_repository
        self.lm_version = lm_version
        
        # Initialize ML Models
        self.scorer = CallScorer()
        self.churn_predictor = ChurnPredictor()
        self.upsell_recommender = UpsellRecommender()

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
        metrics_data = self.scorer.calculate_operational_metrics(call_history, call_score)
        
        result = []
        for code, value in metrics_data.items():
            result.append({
                'metric_code': code,
                'metric_group': 'operational',
                'value_numeric': value,
                'calc_method': 'rule'
            })
        return result

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
        metrics_data = self.upsell_recommender.predict_conversion_metrics(call_history, call_score)
        
        result = []
        for code, value in metrics_data.items():
            result.append({
                'metric_code': code,
                'metric_group': 'conversion',
                'value_numeric': value,
                'calc_method': 'rule'
            })
        return result

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
        metrics_data = self.scorer.calculate_quality_metrics(call_history, call_score)
        
        result = []
        for code, value in metrics_data.items():
            result.append({
                'metric_code': code,
                'metric_group': 'quality',
                'value_numeric': value,
                'calc_method': 'rule'
            })
        return result

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
        metrics_data = self.churn_predictor.predict_risk_metrics(call_history, call_score)
        
        result = []
        for code, data in metrics_data.items():
            result.append({
                'metric_code': code,
                'metric_group': 'risk',
                'value_label': data['label'],
                'value_numeric': data['value'],
                'calc_method': 'rule'
            })
        return result

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
        # Combine forecasts from different predictors
        metrics_data = {}
        metrics_data.update(self.churn_predictor.predict_forecast_metrics(call_history, call_score))
        metrics_data.update(self.upsell_recommender.predict_forecast_metrics(call_history, call_score))
        
        result = []
        for code, value in metrics_data.items():
            result.append({
                'metric_code': code,
                'metric_group': 'forecast',
                'value_numeric': value,
                'calc_method': 'rule'
            })
        return result

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
