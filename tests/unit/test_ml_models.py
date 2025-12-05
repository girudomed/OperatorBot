"""
Unit tests for ML models.
"""

import pytest
from app.ml.models import CallScorer, ChurnPredictor, UpsellRecommender


class TestCallScorer:
    """Тесты для CallScorer."""
    
    def setup_method(self):
        self.scorer = CallScorer()
    
    def test_response_speed_answered(self):
        """Звонок с разговором — высокий скор."""
        call = {'talk_duration': 120}
        result = self.scorer._calculate_response_speed(call)
        assert result == 85.0
    
    def test_response_speed_missed(self):
        """Пропущенный звонок — низкий скор."""
        call = {'talk_duration': 0}
        result = self.scorer._calculate_response_speed(call)
        assert result == 20.0
    
    def test_talk_efficiency_long_call(self):
        """Длинный звонок — высокая эффективность."""
        call = {'talk_duration': 300}
        result = self.scorer._calculate_talk_efficiency(call)
        assert result == 100.0
    
    def test_talk_efficiency_short_call(self):
        """Короткий звонок — низкая эффективность."""
        call = {'talk_duration': 10}
        result = self.scorer._calculate_talk_efficiency(call)
        assert result == 20.0
    
    def test_queue_impact(self):
        """5 минут = 100% нагрузка."""
        call = {'talk_duration': 300}
        result = self.scorer._calculate_queue_impact(call)
        assert result == 100.0


class TestChurnPredictor:
    """Тесты для ChurnPredictor."""
    
    def setup_method(self):
        self.predictor = ChurnPredictor()
    
    def test_high_churn_risk_complaint(self):
        """Жалоба — высокий риск оттока."""
        score = {'call_category': 'Жалоба', 'outcome': ''}
        level, value = self.predictor._calculate_churn_risk(score)
        assert level == 'high'
        assert value == 90.0
    
    def test_low_churn_risk_record(self):
        """Запись — низкий риск оттока."""
        score = {'call_category': '', 'outcome': 'record'}
        level, value = self.predictor._calculate_churn_risk(score)
        assert level == 'low'
        assert value == 10.0
    
    def test_complaint_flag_true(self):
        """Жалоба — флаг true."""
        score = {'call_category': 'Жалоба', 'call_score': 8}
        risk, flag = self.predictor._calculate_complaint_risk(score)
        assert flag is True
        assert risk == 100.0


class TestUpsellRecommender:
    """Тесты для UpsellRecommender."""
    
    def setup_method(self):
        self.recommender = UpsellRecommender()
    
    def test_conversion_score_record(self):
        """Запись на услугу — 100% конверсия."""
        score = {'outcome': 'record', 'call_category': ''}
        result = self.recommender._calculate_conversion_score(score)
        assert result == 100.0
    
    def test_conversion_score_lead(self):
        """Лид без записи — 50%."""
        score = {'outcome': 'lead_no_record', 'call_category': ''}
        result = self.recommender._calculate_conversion_score(score)
        assert result == 50.0
    
    def test_cross_sell_potential_record(self):
        """После записи — высокий cross-sell потенциал."""
        score = {'outcome': 'record', 'requested_service_name': ''}
        result = self.recommender._calculate_cross_sell_potential(score)
        assert result == 70.0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
