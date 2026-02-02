"""
Unit tests for LM Repository.
"""

import pytest
from unittest.mock import Mock, AsyncMock, MagicMock
from typing import Dict
from datetime import datetime, date
import json

from app.db.repositories.lm_repository import LMRepository
from app.db.manager import DatabaseManager


class TestLMRepository:
    @pytest.fixture
    def mock_db_manager(self):
        manager = Mock(spec=DatabaseManager)
        manager.execute_with_retry = AsyncMock()
        return manager

    @pytest.fixture
    def lm_repo(self, mock_db_manager):
        return LMRepository(mock_db_manager)

    def test_sanitize_value_numeric(self):
        """Test value_numeric sanitization rules."""
        sanitize = LMRepository._sanitize_value_numeric
        metric_code = "test_metric"
        history_id = 1

        assert sanitize(None, metric_code, history_id) is None
        assert sanitize("bad", metric_code, history_id) is None
        assert sanitize(float("nan"), metric_code, history_id) is None
        assert sanitize(float("inf"), metric_code, history_id) is None
        assert sanitize(float("-inf"), metric_code, history_id) is None

        # Rounds to 4 decimals
        assert sanitize(1.234567, metric_code, history_id) == 1.2346
        assert sanitize("2.34565", metric_code, history_id) == 2.3457

        # Clamp to DECIMAL(10,4) range
        assert sanitize(1_000_000, metric_code, history_id) == 999999.9999
        assert sanitize(-1_000_000, metric_code, history_id) == -999999.9999

    @pytest.mark.asyncio
    async def test_save_lm_value(self, lm_repo, mock_db_manager):
        """Test saving a single LM value."""
        # Mock successful insert and fetch
        mock_db_manager.execute_with_retry.side_effect = [
            True,  # INSERT/UPDATE result
            {'id': 100}  # SELECT result
        ]
        
        result_id = await lm_repo.save_lm_value(
            history_id=123,
            metric_code='conversion_score',
            metric_group='conversion',
            lm_version='test_v1',
            calc_method='rule',
            value_numeric=75.5
        )
        
        assert result_id == 100
        assert mock_db_manager.execute_with_retry.call_count == 2

    @pytest.mark.asyncio
    async def test_save_lm_value_with_json(self, lm_repo, mock_db_manager):
        """Test saving LM value with JSON data."""
        mock_db_manager.execute_with_retry.side_effect = [
            True,
            {'id': 101}
        ]
        
        json_data = {'flag1': True, 'flag2': False, 'score': 0.85}
        
        result_id = await lm_repo.save_lm_value(
            history_id=124,
            metric_code='feature_pack',
            metric_group='aux',
            lm_version='test_v1',
            calc_method='rule',
            value_json=json_data
        )
        
        assert result_id == 101
        
        # Verify JSON was serialized in the call
        call_args = mock_db_manager.execute_with_retry.call_args_list[0][0]
        params = call_args[1]
        # JSON should be in params[6] (value_json position)
        assert json.loads(params[6]) == json_data

    @pytest.mark.asyncio
    async def test_save_lm_value_no_values_error(self, lm_repo):
        """Test that error is raised when no values provided."""
        with pytest.raises(ValueError, match="At least one value must be provided"):
            await lm_repo.save_lm_value(
                history_id=125,
                metric_code='test_metric',
                metric_group='operational',
                lm_version='test_v1',
                calc_method='rule'
                # No value_numeric, value_label, or value_json
            )

    @pytest.mark.asyncio
    async def test_save_lm_values_batch(self, lm_repo, mock_db_manager):
        """Test batch saving of LM values."""
        # Mock successful saves
        mock_db_manager.execute_with_retry.side_effect = [
            True, {'id': 1},  # First metric
            True, {'id': 2},  # Second metric
            True, {'id': 3},  # Third metric
        ]
        
        values = [
            {
                'history_id': 123,
                'metric_code': 'conversion_score',
                'metric_group': 'conversion',
                'lm_version': 'test_v1',
                'calc_method': 'rule',
                'value_numeric': 75.0
            },
            {
                'history_id': 123,
                'metric_code': 'churn_risk_level',
                'metric_group': 'risk',
                'lm_version': 'test_v1',
                'calc_method': 'rule',
                'value_label': 'low',
                'value_numeric': 20.0
            },
            {
                'history_id': 123,
                'metric_code': 'lm_version_tag',
                'metric_group': 'aux',
                'lm_version': 'test_v1',
                'calc_method': 'meta',
                'value_label': 'test_v1'
            }
        ]
        
        count = await lm_repo.save_lm_values_batch(values)
        
        assert count == 3

    @pytest.mark.asyncio
    async def test_save_lm_values_batch_with_errors(self, lm_repo, mock_db_manager):
        """Test batch saving with some errors."""
        # Mock: first succeeds, second fails, third succeeds
        mock_db_manager.execute_with_retry.side_effect = [
            True, {'id': 1},  # First succeeds
            Exception("DB error"),  # Second fails
            True, {'id': 3},  # Third succeeds
        ]
        
        values = [
            {
                'history_id': 123,
                'metric_code': 'metric1',
                'metric_group': 'operational',
                'lm_version': 'test_v1',
                'calc_method': 'rule',
                'value_numeric': 10.0
            },
            {
                'history_id': 123,
                'metric_code': 'metric2',
                'metric_group': 'conversion',
                'lm_version': 'test_v1',
                'calc_method': 'rule',
                'value_numeric': 20.0
            },
            {
                'history_id': 123,
                'metric_code': 'metric3',
                'metric_group': 'quality',
                'lm_version': 'test_v1',
                'calc_method': 'rule',
                'value_numeric': 30.0
            }
        ]
        
        count = await lm_repo.save_lm_values_batch(values)
        
        # Should succeed for 2 out of 3
        assert count == 2

    @pytest.mark.asyncio
    async def test_get_lm_values_by_call(self, lm_repo, mock_db_manager):
        """Test retrieving LM values for a specific call."""
        mock_data = [
            {
                'id': 1,
                'history_id': 123,
                'metric_code': 'conversion_score',
                'metric_group': 'conversion',
                'value_numeric': 75.0,
                'value_label': None,
                'value_json': None
            },
            {
                'id': 2,
                'history_id': 123,
                'metric_code': 'churn_risk_level',
                'metric_group': 'risk',
                'value_numeric': 20.0,
                'value_label': 'low',
                'value_json': None
            }
        ]
        
        mock_db_manager.execute_with_retry.return_value = mock_data
        
        results = await lm_repo.get_lm_values_by_call(history_id=123)
        
        assert len(results) == 2
        assert results[0]['metric_code'] == 'conversion_score'
        assert results[1]['metric_code'] == 'churn_risk_level'

    @pytest.mark.asyncio
    async def test_get_lm_values_by_call_with_json(self, lm_repo, mock_db_manager):
        """Test retrieving LM values with JSON data."""
        json_data = {'flag1': True, 'score': 0.85}
        json_string = json.dumps(json_data)
        
        mock_data = [
            {
                'id': 1,
                'history_id': 123,
                'metric_code': 'feature_pack',
                'metric_group': 'aux',
                'value_numeric': None,
                'value_label': None,
                'value_json': json_string  # JSON as string from DB
            }
        ]
        
        mock_db_manager.execute_with_retry.return_value = mock_data
        
        results = await lm_repo.get_lm_values_by_call(history_id=123)
        
        assert len(results) == 1
        assert results[0]['value_json'] == json_data  # Should be parsed back to dict

    @pytest.mark.asyncio
    async def test_get_lm_values_by_metric(self, lm_repo, mock_db_manager):
        """Test retrieving values for a specific metric."""
        mock_data = [
            {
                'id': 1,
                'history_id': 123,
                'metric_code': 'conversion_score',
                'value_numeric': 75.0,
                'created_at': datetime(2025, 12, 1, 10, 0)
            },
            {
                'id': 2,
                'history_id': 124,
                'metric_code': 'conversion_score',
                'value_numeric': 80.0,
                'created_at': datetime(2025, 12, 1, 11, 0)
            }
        ]
        
        mock_db_manager.execute_with_retry.return_value = mock_data
        
        results = await lm_repo.get_lm_values_by_metric(
            metric_code='conversion_score',
            start_date=datetime(2025, 12, 1),
            end_date=datetime(2025, 12, 2)
        )
        
        assert len(results) == 2
        assert all(r['metric_code'] == 'conversion_score' for r in results)

    @pytest.mark.asyncio
    async def test_get_aggregated_metrics(self, lm_repo, mock_db_manager):
        """Test getting aggregated metrics."""
        mock_data = [
            {
                'metric_code': 'conversion_score',
                'count': 100,
                'avg_value': 72.5,
                'min_value': 50.0,
                'max_value': 100.0,
                'stddev_value': 12.3
            },
            {
                'metric_code': 'normalized_call_score',
                'count': 100,
                'avg_value': 68.2,
                'min_value': 40.0,
                'max_value': 95.0,
                'stddev_value': 15.7
            }
        ]
        
        mock_db_manager.execute_with_retry.return_value = mock_data
        
        results = await lm_repo.get_aggregated_metrics(
            metric_codes=['conversion_score', 'normalized_call_score'],
            start_date=datetime(2025, 12, 1),
            end_date=datetime(2025, 12, 2),
            group_by='metric_code'
        )
        
        assert len(results) == 2
        assert results[0]['metric_code'] == 'conversion_score'
        assert results[0]['avg_value'] == 72.5

    @pytest.mark.asyncio
    async def test_get_group_metrics(self, lm_repo, mock_db_manager):
        """Test aggregated metrics by group shortcut."""
        mock_rows = [
            {
                'metric_code': 'conversion_score',
                'avg_value': 75,
                'min_value': 50,
                'max_value': 90,
                'count_value': 10,
            }
        ]
        mock_db_manager.execute_with_retry.return_value = mock_rows

        result = await lm_repo.get_group_metrics('conversion', days=3)

        assert 'conversion_score' in result
        assert result['conversion_score']['avg'] == 75.0
        assert result['conversion_score']['count'] == 10

    @pytest.mark.asyncio
    async def test_get_weekly_metrics(self, lm_repo, mock_db_manager):
        """Test aggregated metrics grouped by week."""
        mock_rows = [
            {
                'period_key': '2024-W12',
                'period_start': date(2024, 3, 18),
                'period_end': date(2024, 3, 24),
                'metric_group': 'conversion',
                'metric_code': 'conversion_score',
                'avg_value': 70,
                'min_value': 50,
                'max_value': 90,
                'count_value': 5,
            },
            {
                'period_key': '2024-W12',
                'period_start': date(2024, 3, 18),
                'period_end': date(2024, 3, 24),
                'metric_group': 'quality',
                'metric_code': 'talk_time_efficiency',
                'avg_value': 4.2,
                'min_value': 3.9,
                'max_value': 4.8,
                'count_value': 5,
            },
        ]
        mock_db_manager.execute_with_retry.return_value = mock_rows

        result = await lm_repo.get_weekly_metrics(metric_groups=['conversion', 'quality'], weeks=2)

        assert len(result) == 1
        period = result[0]
        assert period['period_key'] == '2024-W12'
        assert period['metrics']['conversion']['conversion_score']['avg'] == 70.0
        assert period['metrics']['quality']['talk_time_efficiency']['count'] == 5

    @pytest.mark.asyncio
    async def test_get_lm_period_summary(self, lm_repo, mock_db_manager, monkeypatch):
        """Ensure period summary aggregates metrics, flags и дельты."""
        metric_payloads = {
            'conversion_score': [
                {'avg_value': 60, 'sample_count': 120},
                {'avg_value': 64, 'sample_count': 100},
            ],
            'lost_opportunity_score': [
                {'avg_value': 42, 'sample_count': 120, 'above_threshold': 18},
                {'avg_value': 30, 'sample_count': 100, 'above_threshold': 10},
            ],
            'cross_sell_potential': [
                {'avg_value': 55, 'sample_count': 120, 'above_threshold': 6},
                {'avg_value': 40, 'sample_count': 100, 'above_threshold': 3},
            ],
            'normalized_call_score': [
                {'avg_value': 75.5, 'sample_count': 120},
                {'avg_value': 70.0, 'sample_count': 100},
            ],
            'conversion_prob_forecast': [
                {'avg_value': 0.57, 'sample_count': 120},
                {'avg_value': 0.60, 'sample_count': 100},
            ],
            'second_call_prob': [
                {'avg_value': 0.38, 'sample_count': 120},
                {'avg_value': 0.30, 'sample_count': 100},
            ],
            'complaint_risk_flag': [
                {'avg_value': 65, 'sample_count': 120, 'above_threshold': 9},
                {'avg_value': 40, 'sample_count': 100, 'above_threshold': 4},
            ],
            'complaint_prob': [
                {'avg_value': 0.2, 'sample_count': 120, 'above_threshold': 7},
                {'avg_value': 0.1, 'sample_count': 100, 'above_threshold': 5},
            ],
            'script_risk_index': [
                {'avg_value': 35, 'sample_count': 120, 'above_threshold': 9, 'medium_band_count': 11},
                {'avg_value': 32, 'sample_count': 100, 'above_threshold': 4, 'medium_band_count': 8},
            ],
        }
        call_counts: Dict[str, int] = {}

        async def fake_fetch(metric_code, start_date, end_date, threshold=None, medium_band=None):
            idx = call_counts.get(metric_code, 0)
            call_counts[metric_code] = idx + 1
            return metric_payloads.get(metric_code, [{}]*2)[min(idx, 1)]

        monkeypatch.setattr(lm_repo, "_fetch_period_numeric", fake_fetch)

        flag_rows = [
            {
                'metric_code': 'followup_needed_flag',
                'true_count': 12,
                'sample_count': 120,
            },
            {
                'metric_code': 'complaint_risk_flag',
                'true_count': 3,
                'sample_count': 120,
            },
        ]
        churn_rows = [
            {
                'value_label': 'HIGH',
                'count_value': 5,
            }
        ]
        bookings = [
            {'call_category': '2ГИС', 'cnt': 20},
            {'call_category': 'Yandex', 'cnt': 15},
        ]
        utm_rows = [
            {'source_label': 'Гугл карты', 'cnt': 60},
            {'source_label': 'Яндекс карты', 'cnt': 30},
        ]
        mock_db_manager.execute_with_retry.side_effect = [
            flag_rows,
            churn_rows,
            {
                'total_cnt': 200,
                'target_cnt': 150,
                'lost_cnt': 40,
                'transcript_cnt': 160,
                'outcome_cnt': 180,
                'refusal_cnt': 50,
                'operator_cnt': 200,
                'utm_cnt': 190,
            },
            utm_rows,
            [
                {'loss_group': 'Цена / дорого', 'cnt': 10},
                {'loss_group': 'Не указано', 'cnt': 5},
            ],
            bookings,
        ]

        summary = await lm_repo.get_lm_period_summary(days=7)

        assert summary['call_count'] == 200
        assert summary['base']['target_calls'] == 150
        assert summary['base']['non_target_calls'] == 50
        assert summary['base']['lost_opportunity_count'] == 40
        assert summary['metrics']['conversion_score']['avg'] == 60
        assert summary['metrics']['conversion_score']['delta'] == -4
        assert summary['metrics']['lost_opportunity_score']['alert_count'] == 18
        assert summary['metrics']['complaint_risk_flag']['alert_count'] == 9
        assert summary['flags']['followup_needed_flag']['true_count'] == 12
        assert summary['churn']['high'] == 5
        assert summary['updated_at'] is not None
        assert summary['coverage']['transcript']['count'] == 160
        assert summary['coverage']['outcome']['percent'] == 90.0
        assert summary['bookings'][0]['call_category'] == '2ГИС'
        assert summary['loss_breakdown'][0]['label'] == 'Цена / дорого'
        assert summary['loss_breakdown'][0]['count'] == 10
        assert summary['utm_breakdown'][0]['label'] == 'Гугл карты'
        assert summary['utm_breakdown'][0]['count'] == 60
        assert summary['utm_breakdown'][0]['share'] == 30.0

    @pytest.mark.asyncio
    async def test_delete_lm_values_by_call(self, lm_repo, mock_db_manager):
        """Test deleting LM values for a call."""
        mock_db_manager.execute_with_retry.return_value = 5  # 5 records deleted
        
        count = await lm_repo.delete_lm_values_by_call(history_id=123)
        
        assert count == 5
        mock_db_manager.execute_with_retry.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_metric_statistics(self, lm_repo, mock_db_manager):
        """Test getting statistics for a metric."""
        mock_stats = {
            'count': 100,
            'avg_value': 72.5,
            'min_value': 50.0,
            'max_value': 100.0,
            'stddev_value': 12.3
        }
        
        mock_db_manager.execute_with_retry.return_value = mock_stats
        
        stats = await lm_repo.get_metric_statistics(
            metric_code='conversion_score',
            start_date=datetime(2025, 12, 1),
            end_date=datetime(2025, 12, 2)
        )
        
        assert stats['count'] == 100
        assert stats['avg_value'] == 72.5
        assert stats['stddev_value'] == 12.3
