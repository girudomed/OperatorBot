"""
Unit tests for LM Service.
"""

import pytest
from unittest.mock import Mock, AsyncMock, MagicMock
from datetime import datetime

from app.services.lm_service import LMService
from app.db.repositories.lm_repository import LMRepository


class TestLMService:
    @pytest.fixture
    def mock_lm_repo(self):
        repo = Mock(spec=LMRepository)
        repo.save_lm_values_batch = AsyncMock(return_value=10)
        return repo

    @pytest.fixture
    def lm_service(self, mock_lm_repo):
        return LMService(mock_lm_repo, lm_version="test_v1")

    @pytest.fixture
    def sample_call_history(self):
        return {
            'history_id': 123,
            'call_date': datetime(2025, 12, 1, 10, 30),
            'talk_duration': 120,
            'call_type': 'incoming',
            'called_info': '101 Test Operator',
            'caller_info': '+1234567890',
            'caller_number': '+1234567890',
            'called_number': '101'
        }

    @pytest.fixture
    def sample_call_score(self):
        return {
            'id': 456,
            'history_id': 123,
            'call_score': 8.5,
            'is_target': 1,
            'outcome': 'record',
            'call_category': 'Запись на услугу (успешная)',
            'requested_service_name': 'Консультация',
            'number_checklist': 8,
            'refusal_reason': None
        }

    # ===== Operational Metrics Tests =====

    def test_calculate_operational_metrics(self, lm_service, sample_call_history, sample_call_score):
        """Test operational metrics calculation."""
        metrics = lm_service.calculate_operational_metrics(sample_call_history, sample_call_score)
        
        # Should return 3 operational metrics
        assert len(metrics) == 3
        assert all(m['metric_group'] == 'operational' for m in metrics)
        
        metric_codes = [m['metric_code'] for m in metrics]
        assert 'response_speed_score' in metric_codes
        assert 'talk_time_efficiency' in metric_codes
        assert 'queue_impact_index' in metric_codes
        
        # All should have numeric values
        assert all(m['value_numeric'] is not None for m in metrics)

    def test_response_speed_answered_call(self, lm_service, sample_call_history, sample_call_score):
        """Test response speed for answered call."""
        score, status = lm_service._calculate_response_speed(sample_call_history, sample_call_score)
        assert score == 5.0  # max балл в текущей шкале
        assert status == 'green'

    def test_response_speed_missed_call(self, lm_service, sample_call_history, sample_call_score):
        """Test response speed for missed call."""
        sample_call_history['talk_duration'] = 0
        score, status = lm_service._calculate_response_speed(sample_call_history, sample_call_score)
        assert score == 1.0
        assert status == 'red'

    def test_talk_efficiency(self, lm_service, sample_call_history, sample_call_score):
        """Test talk time efficiency calculation."""
        # Long call (2 minutes)
        score = lm_service._calculate_talk_efficiency(sample_call_history, sample_call_score)
        assert score > 0
        assert score <= 100.0

    # ===== Conversion Metrics Tests =====

    def test_calculate_conversion_metrics(self, lm_service, sample_call_history, sample_call_score):
        """Test conversion metrics calculation."""
        metrics = lm_service.calculate_conversion_metrics(sample_call_history, sample_call_score)
        
        # Should return 3 conversion metrics
        assert len(metrics) == 3
        assert all(m['metric_group'] == 'conversion' for m in metrics)
        
        metric_codes = [m['metric_code'] for m in metrics]
        assert 'conversion_score' in metric_codes
        assert 'lost_opportunity_score' in metric_codes
        assert 'cross_sell_potential' in metric_codes

    def test_conversion_score_booked(self, lm_service, sample_call_score):
        """Test conversion score for successfully booked call."""
        score = lm_service._calculate_conversion_score(sample_call_score)
        assert score == 100.0

    def test_conversion_score_lead_no_record(self, lm_service):
        """Test conversion score for lead without booking."""
        call_score = {'outcome': 'lead_no_record', 'call_category': 'Лид (без записи)'}
        score = lm_service._calculate_conversion_score(call_score)
        assert score == 50.0

    def test_lost_opportunity_only_lead_no_record_counts(self, lm_service, sample_call_history):
        """Losses should count только для целевых lead_no_record."""
        call_score = {
            'is_target': 1,
            'outcome': 'lead_no_record',
            'call_category': 'Лид (без записи)',
            'call_score': 6.0,
            'refusal_reason': '',
            'result': ''
        }
        score, meta = lm_service._calculate_lost_opportunity(sample_call_history, call_score)
        assert score > 0
        assert "lead_no_record" in " ".join(meta.get('reasons', []))

    def test_lost_opportunity_ignores_cancellations_and_info(self, lm_service, sample_call_history):
        """Целевой звонок с иным исходом не должен попадать в потери."""
        call_score = {
            'is_target': 1,
            'outcome': 'cancelled_by_patient',
            'call_category': 'Отмена записи',
            'call_score': 6.0,
            'refusal_reason': '',
            'result': ''
        }
        score, meta = lm_service._calculate_lost_opportunity(sample_call_history, call_score)
        assert score == 0
        assert meta == {}

    # ===== Quality Metrics Tests =====

    def test_calculate_quality_metrics(self, lm_service, sample_call_history, sample_call_score):
        """Test quality metrics calculation."""
        metrics = lm_service.calculate_quality_metrics(sample_call_history, sample_call_score)
        
        # Should return 3 quality metrics
        assert len(metrics) == 3
        assert all(m['metric_group'] == 'quality' for m in metrics)
        
        metric_codes = [m['metric_code'] for m in metrics]
        assert 'checklist_coverage_ratio' in metric_codes
        assert 'normalized_call_score' in metric_codes
        assert 'script_risk_index' in metric_codes

    def test_normalized_score(self, lm_service, sample_call_score):
        """Test call score normalization."""
        score = lm_service._calculate_normalized_score(sample_call_score)
        assert 0 <= score <= 100
        assert score == 85.0  # 8.5 * 10

    def test_script_risk_high_score(self, lm_service):
        """Test script risk for high quality call."""
        call_score = {'call_score': 9.0, 'call_category': 'Запись на услугу (успешная)'}
        risk = lm_service._calculate_script_risk(call_score)
        assert risk == 10.0  # Low risk for high quality

    def test_script_risk_low_score(self, lm_service):
        """Test script risk for low quality call."""
        call_score = {'call_score': 2.0, 'call_category': 'Навигация'}
        risk = lm_service._calculate_script_risk(call_score)
        assert risk == 80.0  # High risk for low quality

    def test_script_risk_complaint(self, lm_service):
        """Test script risk for complaint call."""
        call_score = {'call_score': 5.0, 'call_category': 'Жалоба'}
        risk = lm_service._calculate_script_risk(call_score)
        assert risk >= 50.0  # Complaints have higher risk

    # ===== Risk Metrics Tests =====

    def test_calculate_risk_metrics(self, lm_service, sample_call_history, sample_call_score):
        """Test risk metrics calculation."""
        metrics = lm_service.calculate_risk_metrics(sample_call_history, sample_call_score)
        
        # Should return 3 risk metrics
        assert len(metrics) == 3
        assert all(m['metric_group'] == 'risk' for m in metrics)
        
        metric_codes = [m['metric_code'] for m in metrics]
        assert 'churn_risk_level' in metric_codes
        assert 'complaint_risk_flag' in metric_codes
        assert 'followup_needed_flag' in metric_codes

    def test_churn_risk_complaint(self, lm_service):
        """Test churn risk for complaint."""
        call_score = {'call_category': 'Жалоба', 'outcome': '', 'refusal_reason': None}
        level, score = lm_service._calculate_churn_risk(call_score)
        assert level == 'HIGH'
        assert score == 90.0

    def test_churn_risk_booking(self, lm_service):
        """Test churn risk for successful booking."""
        call_score = {'call_category': 'Запись на услугу (успешная)', 'outcome': 'record', 'refusal_reason': None}
        level, score = lm_service._calculate_churn_risk(call_score)
        assert level == 'LOW'
        assert score == 10.0

    def test_complaint_gate_service_not_provided(self, lm_service, sample_call_history):
        """SERVICE_NOT_PROVIDED должен блокировать жалобу."""
        call_score = {
            'call_category': 'Лид (без записи)',
            'outcome': 'lead_no_record',
            'refusal_category_code': 'SERVICE_NOT_PROVIDED',
            'transcript': 'Здравствуйте. Нужна услуга. Нет, такого не делаем. Спасибо.'
        }
        score, flag, context = lm_service._calculate_complaint_risk(sample_call_history, call_score)
        assert flag is False
        assert context.get('gate') is True
        assert 'SERVICE_NOT_PROVIDED' in str(context.get('reasons'))

    def test_complaint_gate_service_not_provided_transcript(self, lm_service, sample_call_history):
        """Фраза в разговоре про отсутствие услуги должна снимать жалобу."""
        call_score = {
            'call_category': 'Лид (без записи)',
            'outcome': 'lead_no_record',
            'transcript': 'Можно записаться на холтер? Холтер мы не ставим. Спасибо, до свидания.'
        }
        score, flag, context = lm_service._calculate_complaint_risk(sample_call_history, call_score)
        assert flag is False
        assert context.get('gate') is True
        assert 'услуга отсутствует' in " ".join(context.get('reasons', []))

    def test_complaint_core_signal_required(self, lm_service, sample_call_history):
        """Без претензионных фраз жалоба не ставится."""
        call_score = {
            'call_category': 'Лид (без записи)',
            'outcome': 'lead_no_record',
            'transcript': 'Здравствуйте. Интересовала запись. Спасибо, не нужно.'
        }
        score, flag, context = lm_service._calculate_complaint_risk(sample_call_history, call_score)
        assert flag is False
        assert "Нет признаков" in " ".join(context.get('reasons', []))

    def test_complaint_threat_phrase(self, lm_service, sample_call_history):
        """Явная угроза должна включать жалобу."""
        call_score = {
            'call_category': 'Лид (без записи)',
            'outcome': 'lead_no_record',
            'transcript': 'Вы обязаны сделать как обещали. Я буду жаловаться руководству.',
            'utm_source_by_number': 'ПроДокторов'
        }
        score, flag, context = lm_service._calculate_complaint_risk(sample_call_history, call_score)
        assert flag is True
        assert score >= 80.0
        assert 'complaint_phrase' in (context.get('core_signals') or {})
        assert any("жал" in reason.lower() for reason in context.get('reasons', []))

    def test_followup_needed_lead(self, lm_service, sample_call_history):
        """Test follow-up flag for lead without booking."""
        call_score = {'outcome': 'lead_no_record', 'call_category': 'Лид (без записи)'}
        needed, context = lm_service._calculate_followup_needed(sample_call_history, call_score)
        assert needed is True
        assert context is not None
        assert "reason" in context

    def test_followup_not_needed_booking(self, lm_service, sample_call_history):
        """Test follow-up flag for successful booking."""
        call_score = {'outcome': 'record', 'call_category': 'Запись на услугу (успешная)'}
        needed, context = lm_service._calculate_followup_needed(sample_call_history, call_score)
        assert needed is False
        assert context is None

    def test_followup_service_not_provided_ai_reason(self, lm_service, sample_call_history):
        """AI-анализ с причиной 'услуга не предоставляется' должен снимать follow-up."""
        call_score = {
            'outcome': 'lead_no_record',
            'call_category': 'Лид (без записи)',
            'is_target': 1,
            'result': '🚫 Причина отказа: Услуга не предоставляется клиникой'
        }
        needed, context = lm_service._calculate_followup_needed(sample_call_history, call_score)
        assert needed is False
        assert context is None

    def test_followup_service_not_provided_transcript(self, lm_service, sample_call_history):
        """Фраза в транскрипте про отсутствие услуги снимает follow-up."""
        call_score = {
            'outcome': 'lead_no_record',
            'call_category': 'Лид (без записи)',
            'is_target': 1,
            'transcript': 'Здравствуйте. У вас есть услуга вызова врача на дом? Нет, такого нет, к сожалению.'
        }
        needed, context = lm_service._calculate_followup_needed(sample_call_history, call_score)
        assert needed is False
        assert context is None

    def test_followup_cancelled_patient_not_needed(self, lm_service, sample_call_history):
        """Отмена пациентом с отсутствующей услугой не должна попадать в follow-up."""
        call_score = {
            'outcome': 'cancelled_by_patient ',
            'call_category': 'Отмена записи',
            'is_target': 1,
            'refusal_reason': 'Услуга не предоставляется клиникой'
        }
        needed, context = lm_service._calculate_followup_needed(sample_call_history, call_score)
        assert needed is False
        assert context is None

    # ===== Forecast Metrics Tests =====

    def test_calculate_forecast_metrics(self, lm_service, sample_call_history, sample_call_score):
        """Test forecast metrics calculation."""
        metrics = lm_service.calculate_forecast_metrics(sample_call_history, sample_call_score)
        
        # Should return 3 forecast metrics
        assert len(metrics) == 3
        assert all(m['metric_group'] == 'forecast' for m in metrics)
        
        metric_codes = [m['metric_code'] for m in metrics]
        assert 'conversion_prob_forecast' in metric_codes
        assert 'second_call_prob' in metric_codes
        assert 'complaint_prob' in metric_codes
        
        # All probabilities should be between 0 and 1
        for metric in metrics:
            assert 0 <= metric['value_numeric'] <= 1.0

    def test_conversion_probability_booked(self, lm_service):
        """Test conversion probability for already booked call."""
        call_score = {'outcome': 'record', 'is_target': 1}
        prob = lm_service._forecast_conversion_probability(call_score)
        assert prob == 1.0

    def test_conversion_probability_lead(self, lm_service):
        """Test conversion probability for lead without booking."""
        call_score = {'outcome': 'lead_no_record', 'is_target': 1}
        prob = lm_service._forecast_conversion_probability(call_score)
        assert 0 < prob < 1.0
        assert prob == 0.35

    # ===== Auxiliary Metrics Tests =====

    def test_calculate_auxiliary_metrics(self, lm_service, sample_call_history, sample_call_score):
        """Test auxiliary metrics calculation."""
        metrics = lm_service.calculate_auxiliary_metrics(sample_call_history, sample_call_score)
        
        # Should return at least 2 auxiliary metrics
        assert len(metrics) >= 2
        assert all(m['metric_group'] == 'aux' for m in metrics)
        
        metric_codes = [m['metric_code'] for m in metrics]
        assert 'lm_version_tag' in metric_codes
        assert 'calc_profile' in metric_codes

    def test_calc_profile_night_shift(self, lm_service, sample_call_history):
        """Test calculation profile detection for night shift."""
        sample_call_history['call_date'] = datetime(2025, 12, 1, 23, 0)  # 11 PM
        call_score = None
        profile = lm_service._determine_calc_profile(sample_call_history, call_score)
        assert profile == 'night_shift_v1'

    def test_calc_profile_weekend(self, lm_service, sample_call_history):
        """Test calculation profile detection for weekend."""
        sample_call_history['call_date'] = datetime(2025, 12, 6, 10, 0)  # Saturday
        call_score = None
        profile = lm_service._determine_calc_profile(sample_call_history, call_score)
        assert profile == 'weekend_v1'

    def test_calc_profile_default(self, lm_service, sample_call_history):
        """Test calculation profile detection for regular hours."""
        sample_call_history['call_date'] = datetime(2025, 12, 1, 14, 0)  # 2 PM weekday
        call_score = None
        profile = lm_service._determine_calc_profile(sample_call_history, call_score)
        assert profile == 'default_v1'

    # ===== Integration Test for All Metrics =====

    @pytest.mark.asyncio
    async def test_calculate_all_metrics(self, lm_service, mock_lm_repo, sample_call_history, sample_call_score):
        """Test calculating all metrics for a call."""
        history_id = 123
        
        count = await lm_service.calculate_all_metrics(
            history_id=history_id,
            call_history=sample_call_history,
            call_score=sample_call_score,
            calc_source="test"
        )
        
        # Should have called batch save
        mock_lm_repo.save_lm_values_batch.assert_called_once()
        
        # Check that all metric groups are included
        call_args = mock_lm_repo.save_lm_values_batch.call_args[0][0]
        metric_groups = {m['metric_group'] for m in call_args}
        
        assert 'operational' in metric_groups
        assert 'conversion' in metric_groups
        assert 'quality' in metric_groups
        assert 'risk' in metric_groups
        assert 'forecast' in metric_groups
        assert 'aux' in metric_groups
        
        # Verify all metrics have history_id
        assert all(m['history_id'] == history_id for m in call_args)
        
        # Return value should be saved count
        assert count == 10
