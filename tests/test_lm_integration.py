"""
Integration test for LM analytics layer.

Tests end-to-end flow: call data → LM calculation → storage → retrieval
"""

import pytest
from datetime import datetime
from unittest.mock import AsyncMock, Mock

from app.db.manager import DatabaseManager
from app.db.repositories.lm_repository import LMRepository
from app.services.lm_service import LMService

pytestmark = pytest.mark.integration

@pytest.mark.asyncio
class TestLMIntegration:
    """Integration tests for the LM analytics layer."""
    
    @pytest.fixture
    def mock_db_manager(self):
        """Mock database manager for integration testing."""
        manager = Mock(spec=DatabaseManager)
        manager.execute_with_retry = AsyncMock()
        return manager
    
    @pytest.fixture
    def lm_repo(self, mock_db_manager):
        """LM repository instance."""
        return LMRepository(mock_db_manager)
    
    @pytest.fixture
    def lm_service(self, lm_repo):
        """LM service instance."""
        return LMService(lm_repo, lm_version="integration_test_v1")
    
    @pytest.fixture
    def sample_call_data(self):
        """Sample call history and score data."""
        call_history = {
            'history_id': 12345,
            'call_date': datetime(2025, 12, 1, 14, 30),
            'talk_duration': 150,
            'call_type': 'incoming',
            'called_info': '101 Dr. Smith',
            'caller_info': '+79001234567',
            'caller_number': '+79001234567',
            'called_number': '101'
        }
        
        call_score = {
            'id': 67890,
            'history_id': 12345,
            'call_score': 7.5,
            'is_target': 1,
            'outcome': 'record',
            'call_category': 'Запись на услугу (успешная)',
            'requested_service_name': 'Консультация терапевта',
            'requested_service_id': 42,
            'requested_doctor_name': 'Dr. Smith',
            'number_checklist': 9,
            'refusal_reason': None
        }
        
        return call_history, call_score
    
    async def test_end_to_end_lm_calculation(
        self,
        lm_service,
        lm_repo,
        mock_db_manager,
        sample_call_data
    ):
        """Test complete LM calculation and storage flow."""
        call_history, call_score = sample_call_data
        history_id = call_history['history_id']
        
        # Mock database responses for save operations
        responses = []
        for i in range(18):
            responses.extend([True, {'id': i + 1}])
        mock_db_manager.execute_with_retry.side_effect = responses
        
        # Calculate all metrics
        saved_count = await lm_service.calculate_all_metrics(
            history_id=history_id,
            call_history=call_history,
            call_score=call_score,
            calc_source="integration_test"
        )
        
        # Verify metrics were saved
        assert saved_count > 0
        
        # Verify database was called
        assert mock_db_manager.execute_with_retry.called
    
    async def test_metrics_retrieval_after_calculation(
        self,
        lm_service,
        lm_repo,
        mock_db_manager,
        sample_call_data
    ):
        """Test retrieving metrics after calculation."""
        call_history, call_score = sample_call_data
        history_id = call_history['history_id']
        
        # Mock save operations (simplified)
        save_responses = []
        for i in range(18):  # Expect ~18 metrics
            save_responses.extend([True, {'id': i + 1}])
        
        mock_db_manager.execute_with_retry.side_effect = save_responses
        
        # Calculate metrics
        await lm_service.calculate_all_metrics(
            history_id=history_id,
            call_history=call_history,
            call_score=call_score
        )
        
        # Mock retrieval
        mock_lm_values = [
            {
                'id': 1,
                'history_id': history_id,
                'metric_code': 'conversion_score',
                'metric_group': 'conversion',
                'value_numeric': 100.0,
                'value_label': None,
                'value_json': None
            },
            {
                'id': 2,
                'history_id': history_id,
                'metric_code': 'churn_risk_level',
                'metric_group': 'risk',
                'value_numeric': 10.0,
                'value_label': 'low',
                'value_json': None
            }
        ]
        
        mock_db_manager.execute_with_retry.reset_mock()
        mock_db_manager.execute_with_retry.return_value = mock_lm_values
        
        # Retrieve metrics
        metrics = await lm_repo.get_lm_values_by_call(history_id)
        
        # Verify retrieval
        assert len(metrics) == 2
        assert metrics[0]['metric_code'] == 'conversion_score'
        assert metrics[0]['value_numeric'] == 100.0
        assert metrics[1]['metric_code'] == 'churn_risk_level'
        assert metrics[1]['value_label'] == 'low'
    
    async def test_metric_aggregation(self, lm_repo, mock_db_manager):
        """Test metric aggregation across multiple calls."""
        # Mock aggregated data
        mock_aggregated = [
            {
                'metric_code': 'conversion_score',
                'count': 50,
                'avg_value': 72.5,
                'min_value': 30.0,
                'max_value': 100.0,
                'stddev_value': 18.2
            }
        ]
        
        mock_db_manager.execute_with_retry.return_value = mock_aggregated
        
        # Get aggregated metrics
        results = await lm_repo.get_aggregated_metrics(
            metric_codes=['conversion_score'],
            start_date=datetime(2025, 12, 1),
            end_date=datetime(2025, 12, 2),
            group_by='metric_code'
        )
        
        # Verify aggregation
        assert len(results) == 1
        assert results[0]['count'] == 50
        assert results[0]['avg_value'] == 72.5
    
    async def test_metric_recalculation(
        self,
        lm_service,
        lm_repo,
        mock_db_manager,
        sample_call_data
    ):
        """Test recalculating metrics for a call."""
        call_history, call_score = sample_call_data
        history_id = call_history['history_id']
        
        # First calculation
        save_responses_first = []
        for i in range(18):
            save_responses_first.extend([None, {'id': i + 1}])
        
        mock_db_manager.execute_with_retry.side_effect = save_responses_first
        
        await lm_service.calculate_all_metrics(
            history_id=history_id,
            call_history=call_history,
            call_score=call_score
        )
        
        # Delete old metrics
        mock_db_manager.execute_with_retry.reset_mock()
        mock_db_manager.execute_with_retry.return_value = 18  # Deleted count
        
        deleted = await lm_repo.delete_lm_values_by_call(history_id)
        assert deleted == 18
        
        # Recalculate
        save_responses_second = []
        for i in range(18):
            save_responses_second.extend([None, {'id': i + 100}])
        
        mock_db_manager.execute_with_retry.reset_mock()
        mock_db_manager.execute_with_retry.side_effect = save_responses_second
        
        saved_count = await lm_service.calculate_all_metrics(
            history_id=history_id,
            call_history=call_history,
            call_score=call_score,
            calc_source="recalculation"
        )
        
        assert saved_count > 0
    
    async def test_comprehensive_metric_coverage(
        self,
        lm_service,
        sample_call_data
    ):
        """Test that all metric groups are calculated."""
        call_history, call_score = sample_call_data
        
        # Calculate each group separately
        operational = lm_service.calculate_operational_metrics(call_history, call_score)
        conversion = lm_service.calculate_conversion_metrics(call_history, call_score)
        quality = lm_service.calculate_quality_metrics(call_history, call_score)
        risk = lm_service.calculate_risk_metrics(call_history, call_score)
        forecast = lm_service.calculate_forecast_metrics(call_history, call_score)
        auxiliary = lm_service.calculate_auxiliary_metrics(call_history, call_score)
        
        # Verify all groups return metrics
        assert len(operational) >= 3
        assert len(conversion) >= 3
        assert len(quality) >= 3
        assert len(risk) >= 3
        assert len(forecast) >= 3
        assert len(auxiliary) >= 2
        
        # Verify metric groups are correct
        assert all(m['metric_group'] == 'operational' for m in operational)
        assert all(m['metric_group'] == 'conversion' for m in conversion)
        assert all(m['metric_group'] == 'quality' for m in quality)
        assert all(m['metric_group'] == 'risk' for m in risk)
        assert all(m['metric_group'] == 'forecast' for m in forecast)
        assert all(m['metric_group'] == 'aux' for m in auxiliary)
        
        # Total metrics should be ~18
        total_metrics = len(operational) + len(conversion) + len(quality) + len(risk) + len(forecast) + len(auxiliary)
        assert total_metrics >= 17  # At least 17 metrics
