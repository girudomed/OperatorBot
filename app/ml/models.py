"""
ML Models for Operabot.

Contains heuristic (rule-based) models for scoring calls, predicting churn, and recommending upsells.
In the future, these can be replaced or enhanced with actual ML models (scikit-learn, PyTorch, etc.).
"""

from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
from app.db.models import CallRecord, CallHistoryRecord

class CallScorer:
    """
    Model for scoring call quality and operational metrics.
    """
    
    def calculate_operational_metrics(
        self,
        call_history: CallHistoryRecord,
        call_score: Optional[CallRecord] = None
    ) -> Dict[str, float]:
        """
        Calculates operational metrics: response speed, talk efficiency, queue impact.
        Returns a dict of {metric_code: value}.
        """
        metrics = {}
        
        # 1. Response Speed
        metrics['response_speed_score'] = self._calculate_response_speed(call_history)
        
        # 2. Talk Efficiency
        metrics['talk_time_efficiency'] = self._calculate_talk_efficiency(call_history)
        
        # 3. Queue Impact
        metrics['queue_impact_index'] = self._calculate_queue_impact(call_history)
        
        return metrics

    def calculate_quality_metrics(
        self,
        call_history: CallHistoryRecord,
        call_score: Optional[CallRecord] = None
    ) -> Dict[str, float]:
        """
        Calculates quality metrics: checklist coverage, normalized score, script risk.
        """
        metrics = {}
        
        if not call_score:
            return metrics
            
        # 1. Checklist Coverage
        metrics['checklist_coverage_ratio'] = self._calculate_checklist_coverage(call_score)
        
        # 2. Normalized Score
        norm_score = self._calculate_normalized_score(call_score)
        if norm_score is not None:
            metrics['normalized_call_score'] = norm_score
            
        # 3. Script Risk
        metrics['script_risk_index'] = self._calculate_script_risk(call_score)
        
        return metrics

    def _calculate_response_speed(self, call_history: CallHistoryRecord) -> float:
        """Response speed score (0-100)."""
        talk_duration = call_history.get('talk_duration') or 0
        if talk_duration > 0:
            return 85.0
        return 20.0

    def _calculate_talk_efficiency(self, call_history: CallHistoryRecord) -> float:
        """Talk efficiency score (0-100)."""
        talk_duration = call_history.get('talk_duration') or 0
        if talk_duration <= 0:
            return 0.0
        if talk_duration > 30:
            return min(100.0, talk_duration / 3.0)
        return talk_duration * 2.0

    def _calculate_queue_impact(self, call_history: CallHistoryRecord) -> float:
        """Queue impact index (0-100)."""
        talk_duration = call_history.get('talk_duration') or 0
        return min(100.0, (talk_duration / 300.0) * 100)

    def _calculate_checklist_coverage(self, call_score: CallRecord) -> float:
        """Checklist coverage ratio (0-100)."""
        number_checklist = call_score.get('number_checklist', 0)
        if number_checklist is None:
            return 50.0
        return min(100.0, (number_checklist / 10.0) * 100)

    def _calculate_normalized_score(self, call_score: CallRecord) -> Optional[float]:
        """Normalized call score (0-100)."""
        call_score_value = call_score.get('call_score')
        if call_score_value is None:
            return None
        score_float = float(call_score_value)
        if score_float <= 10:
            normalized = score_float * 10.0
        else:
            normalized = score_float
        return max(0.0, min(100.0, normalized))

    def _calculate_script_risk(self, call_score: CallRecord) -> float:
        """Script risk index (0-100)."""
        call_score_value = call_score.get('call_score', 0)
        call_category = call_score.get('call_category', '')
        
        score_float = float(call_score_value) if call_score_value else 0
        
        if score_float <= 3:
            risk = 80.0
        elif score_float <= 5:
            risk = 50.0
        elif score_float <= 7:
            risk = 30.0
        else:
            risk = 10.0
            
        if call_category in ['Жалоба', 'Отмена записи']:
            risk = min(100.0, risk + 20.0)
            
        return risk


class ChurnPredictor:
    """
    Model for predicting customer churn and complaint risks.
    """
    
    def predict_risk_metrics(
        self,
        call_history: CallHistoryRecord,
        call_score: Optional[CallRecord] = None
    ) -> Dict[str, Any]:
        """
        Calculates risk metrics: churn risk, complaint risk, followup needed.
        """
        metrics = {}
        if not call_score:
            return metrics
            
        # 1. Churn Risk
        level, score = self._calculate_churn_risk(call_score)
        metrics['churn_risk_level'] = {'label': level, 'value': score}
        
        # 2. Complaint Risk
        comp_score, comp_flag = self._calculate_complaint_risk(call_score)
        metrics['complaint_risk_flag'] = {'label': 'true' if comp_flag else 'false', 'value': comp_score}
        
        # 3. Followup Needed
        followup = self._calculate_followup_needed(call_score)
        metrics['followup_needed_flag'] = {'label': 'true' if followup else 'false', 'value': 1.0 if followup else 0.0}
        
        return metrics

    def predict_forecast_metrics(
        self,
        call_history: CallHistoryRecord,
        call_score: Optional[CallRecord] = None
    ) -> Dict[str, float]:
        """
        Calculates forecast metrics: complaint probability, second call probability.
        """
        metrics = {}
        if not call_score:
            return metrics
            
        metrics['complaint_prob'] = self._forecast_complaint_probability(call_score)
        metrics['second_call_prob'] = self._forecast_second_call_probability(call_score)
        
        return metrics

    def _calculate_churn_risk(self, call_score: CallRecord) -> Tuple[str, float]:
        outcome = call_score.get('outcome', '')
        refusal_reason = call_score.get('refusal_reason', '')
        call_category = call_score.get('call_category', '')
        
        score = 30.0 # Default medium
        
        if call_category == 'Жалоба':
            score = 90.0
        elif call_category == 'Отмена записи':
            score = 70.0
        elif outcome in ['refusal', 'no_interest']:
            score = 60.0
        elif refusal_reason:
            score = 50.0
        elif outcome == 'record':
            score = 10.0
            
        if score >= 70:
            level = 'high'
        elif score >= 40:
            level = 'medium'
        else:
            level = 'low'
            
        return level, score

    def _calculate_complaint_risk(self, call_score: CallRecord) -> Tuple[float, bool]:
        call_category = call_score.get('call_category', '')
        call_score_value = call_score.get('call_score', 0)
        
        score = 10.0
        if call_category == 'Жалоба':
            score = 100.0
        elif call_score_value and float(call_score_value) < 3:
            score = 60.0
        elif call_category == 'Отмена записи':
            score = 40.0
            
        return score, score >= 50.0

    def _calculate_followup_needed(self, call_score: CallRecord) -> bool:
        outcome = call_score.get('outcome', '')
        call_category = call_score.get('call_category', '')
        
        if outcome == 'lead_no_record':
            return True
        if call_category in ['Жалоба', 'Лид (без записи)']:
            return True
        return False

    def _forecast_complaint_probability(self, call_score: CallRecord) -> float:
        call_category = call_score.get('call_category', '')
        call_score_value = call_score.get('call_score', 0)
        
        if call_category == 'Жалоба':
            return 1.0
        if call_score_value and float(call_score_value) < 3:
            return 0.40
        if call_category == 'Отмена записи':
            return 0.25
        return 0.05

    def _forecast_second_call_probability(self, call_score: CallRecord) -> float:
        outcome = call_score.get('outcome', '')
        call_category = call_score.get('call_category', '')
        
        if call_category in ['Навигация', 'Информационный']:
            return 0.60
        if outcome == 'lead_no_record':
            return 0.45
        if outcome == 'record':
            return 0.15
        return 0.25


class UpsellRecommender:
    """
    Model for recommending upsells and predicting conversion.
    """
    
    def predict_conversion_metrics(
        self,
        call_history: CallHistoryRecord,
        call_score: Optional[CallRecord] = None
    ) -> Dict[str, float]:
        """
        Calculates conversion metrics: conversion score, lost opportunity, cross-sell potential.
        """
        metrics = {}
        if not call_score:
            return metrics
            
        # 1. Conversion Score
        metrics['conversion_score'] = self._calculate_conversion_score(call_score)
        
        # 2. Lost Opportunity
        metrics['lost_opportunity_score'] = self._calculate_lost_opportunity(call_score)
        
        # 3. Cross-sell Potential
        metrics['cross_sell_potential'] = self._calculate_cross_sell_potential(call_score)
        
        return metrics

    def predict_forecast_metrics(
        self,
        call_history: CallHistoryRecord,
        call_score: Optional[CallRecord] = None
    ) -> Dict[str, float]:
        """
        Calculates forecast metrics: conversion probability.
        """
        metrics = {}
        if not call_score:
            return metrics
            
        metrics['conversion_prob_forecast'] = self._forecast_conversion_probability(call_score)
        return metrics

    def _calculate_conversion_score(self, call_score: CallRecord) -> float:
        outcome = call_score.get('outcome', '')
        call_category = call_score.get('call_category', '')
        
        if outcome == 'record' or call_category == 'Запись на услугу (успешная)':
            return 100.0
        elif outcome == 'lead_no_record' or call_category == 'Лид (без записи)':
            return 50.0
        elif call_category in ['Навигация', 'Информационный']:
            return 20.0
        return 10.0

    def _calculate_lost_opportunity(self, call_score: CallRecord) -> float:
        outcome = call_score.get('outcome', '')
        is_target = call_score.get('is_target', 0)
        
        if is_target == 1 and outcome != 'record':
            return 80.0
        elif is_target == 1:
            return 0.0
        return 20.0

    def _calculate_cross_sell_potential(self, call_score: CallRecord) -> float:
        outcome = call_score.get('outcome', '')
        requested_service = call_score.get('requested_service_name', '')
        
        if outcome == 'record':
            return 70.0
        elif requested_service:
            return 40.0
        return 10.0

    def _forecast_conversion_probability(self, call_score: CallRecord) -> float:
        outcome = call_score.get('outcome', '')
        is_target = call_score.get('is_target', 0)
        
        if outcome == 'record':
            return 1.0
        if outcome == 'lead_no_record':
            return 0.35
        if is_target == 1:
            return 0.20
        return 0.05
