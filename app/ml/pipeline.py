"""
ML Pipeline orchestration.
"""

from typing import Dict, Any, Optional
from app.db.models import CallRecord, CallHistoryRecord
from app.ml.models import CallScorer, ChurnPredictor, UpsellRecommender

class MLPipeline:
    """
    Orchestrates the ML inference process.
    Prepares data, calls models, and aggregates results.
    """
    
    def __init__(self):
        self.scorer = CallScorer()
        self.churn_predictor = ChurnPredictor()
        self.upsell_recommender = UpsellRecommender()
        
    def run_inference(
        self,
        call_history: CallHistoryRecord,
        call_score: Optional[CallRecord] = None
    ) -> Dict[str, Any]:
        """
        Runs full inference pipeline for a single call.
        Returns a dictionary of all calculated metrics.
        """
        results = {}
        
        # 1. Operational Metrics
        results.update(
            self.scorer.calculate_operational_metrics(call_history, call_score)
        )
        
        # 2. Quality Metrics
        results.update(
            self.scorer.calculate_quality_metrics(call_history, call_score)
        )
        
        # 3. Risk Metrics
        results.update(
            self.churn_predictor.predict_risk_metrics(call_history, call_score)
        )
        
        # 4. Conversion Metrics
        results.update(
            self.upsell_recommender.predict_conversion_metrics(call_history, call_score)
        )
        
        # 5. Forecast Metrics (Combined from predictors)
        results.update(
            self.churn_predictor.predict_forecast_metrics(call_history, call_score)
        )
        results.update(
            self.upsell_recommender.predict_forecast_metrics(call_history, call_score)
        )
        
        return results
