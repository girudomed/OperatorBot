# LM Metrics Reference

This document provides a comprehensive reference for all LM (Learning/Logic Model) analytics metrics calculated by the OperatorBot system.

## Overview

The LM analytics layer transforms raw call data into actionable metrics organized into 6 categories:
- **Operational**: Load, efficiency, response metrics
- **Conversion**: Business and revenue metrics
- **Quality**: Call handling quality metrics
- **Risk**: Customer churn and complaint risks
- **Forecast**: Predictive metrics
- **Auxiliary**: Meta information about calculations

---

## Metric Categories

### 1. Operational Metrics (metric_group = 'operational')

These metrics help understand system load, operator efficiency, and operational performance.

#### response_speed_score
- **Type**: Numeric (0-100)
- **Calculation**: Based on call pickup speed and SLA compliance
- **Higher is Better**: Yes
- **Formula**: Simplified heuristic based on talk_duration > 0
  - Answered calls: 85.0
  - Missed calls: 20.0
- **Use Case**: Monitor operator responsiveness and call center efficiency
- **Example**: Score of 85 indicates fast response

#### talk_time_efficiency  
- **Type**: Numeric (0-100)
- **Calculation**: Ratio of productive talk time to total call duration
- **Higher is Better**: Yes
- **Formula**: 
  - Calls > 30 sec: `min(100, talk_duration / 3.0)`
  - Calls ≤ 30 sec: `talk_duration * 2.0`
- **Use Case**: Identify inefficient call handling
- **Example**: Score of 75 indicates 75% time efficiency

#### queue_impact_index
- **Type**: Numeric (0-100)
- **Calculation**: Impact of call on queue/system load
- **Higher is Worse**: Yes (higher = more load)
- **Formula**: `min(100, (talk_duration / 300) * 100)` (5 minutes = 100)
- **Use Case**: Capacity planning and load balancing
- **Example**: Long calls have higher impact on queue

---

### 2. Conversion Metrics (metric_group = 'conversion')

Business-focused metrics about lead conversion and revenue opportunities.

#### conversion_score
- **Type**: Numeric (0-100)
- **Calculation**: Probability/score that call converted to booking
- **Higher is Better**: Yes
- **Formula**:
  - Outcome = 'record': 100.0
  - Outcome = 'lead_no_record':  50.0
  - Navigation/Info calls: 20.0
  - Default: 10.0
- **Use Case**: Measure booking success rate
- **Example**: Score of 100 = successful booking

#### lost_opportunity_score
- **Type**: Numeric (0-100)
- **Calculation**: Value of opportunity lost if not converted
- **Higher is Worse**: Yes
- **Formula**:
  - Target call, not converted: 80.0
  - Target call, converted: 0.0
  - Non-target: 20.0
- **Use Case**: Quantify lost revenue potential
- **Example**: High score indicates significant lost opportunity

#### cross_sell_potential
- **Type**: Numeric (0-100)
- **Calculation**: Potential for selling additional services
- **Higher is Better**: Yes
- **Formula**:
  - Already booked: 70.0
  - Showed interest (requested_service): 40.0
  - No interest: 10.0
- **Use Case**: Identify upsell opportunities
- **Example**: Booked customers have 70% cross-sell potential

---

### 3. Quality Metrics (metric_group = 'quality')

Metrics measuring call handling quality and script adherence.

#### checklist_coverage_ratio
- **Type**: Numeric (0-100)
- **Calculation**: Percentage of checklist items completed
- **Higher is Better**: Yes
- **Formula**: `min(100, (number_checklist / 10) * 100)` (assumes max 10 items)
- **Use Case**: Ensure operators follow standard procedures
- **Example**: 80% means 8 out of 10 checklist items completed

#### normalized_call_score
- **Type**: Numeric (0-100)
- **Calculation**: Standardized call quality score
- **Higher is Better**: Yes
- **Formula**:
  - If call_score ≤ 10: `call_score * 10`
  - If call_score > 10: `call_score` (already 0-100)
  - Clamped to [0, 100]
- **Use Case**: Compare call quality across different scoring systems
- **Example**: Normalized score of 85 = high quality call

#### script_risk_index
- **Type**: Numeric (0-100)
- **Calculation**: Risk of script deviation or non-compliance
- **Higher is Worse**: Yes
- **Formula**: Based on call_score:
  - Score ≤ 3: 80.0
  - Score ≤ 5: 50.0
  - Score ≤ 7: 30.0
  - Score > 7: 10.0
  - +20 if category is 'Жалоба' or 'Отмена записи'
- **Use Case**: Flag calls needing quality review
- **Example**: High risk score suggests script deviation

---

### 4. Risk Metrics (metric_group = 'risk')

Early warning metrics for customer churn and complaints.

#### churn_risk_level
- **Type**: Label ('low', 'medium', 'high') + Numeric (0-100)
- **Calculation**: Risk of customer not returning
- **Formula**:
  - Complaint: 90.0 (high)
  - Cancellation: 70.0 (high)
  - Refusal/No interest: 60.0 (medium)
  - Has refusal_reason: 50.0 (medium)
  - Booked: 10.0 (low)
  - Default: 30.0 (medium)
- **Use Case**: Prioritize customer retention efforts
- **Example**: 'high' risk = needs immediate follow-up

#### complaint_risk_flag
- **Type**: Boolean (true/false) + Numeric (0-100)
- **Calculation**: Risk of escalation to complaint
- **Formula**:
  - Already complaint: 100.0 (true)
  - Low quality (score < 3): 60.0 (true)
  - Cancellation: 40.0 (false)
  - Default: 10.0 (false)
  - Flag = true if score ≥ 50
- **Use Case**: Prevent complaint escalation
- **Example**: true = likely to complain

#### followup_needed_flag
- **Type**: Boolean (true/false) + Numeric (1.0/0.0)
- **Calculation**: Whether follow-up call is needed
- **Formula**:
  - Outcome = 'lead_no_record': true
  - Category in ['Жалоба', 'Лид (без записи)']: true
  - Default: false
- **Use Case**: Schedule follow-up calls
- **Example**: true = operator should call back

---

### 5. Forecast Metrics (metric_group = 'forecast')

Predictive metrics for future behavior and outcomes.

#### conversion_prob_forecast
- **Type**: Numeric (0-1)
- **Calculation**: Probability of future conversion
- **Formula**:
  - Already converted: 1.0
  - Lead without booking: 0.35
  - Target call: 0.20
  - Default: 0.05
- **Use Case**: Predict future bookings
- **Example**: 0.35 = 35% chance of converting later

#### second_call_prob
- **Type**: Numeric (0-1)
- **Calculation**: Probability of customer calling again
- **Formula**:
  - Navigation/Info: 0.60
  - Lead without booking: 0.45
  - Already booked: 0.15
  - Default: 0.25
- **Use Case**: Estimate future call volume
- **Example**: 0.60 = 60% chance of calling back

#### complaint_prob
- **Type**: Numeric (0-1)
- **Calculation**: Probability of future complaint
- **Formula**:
  - Already complaint: 1.0
  - Low quality (score < 3): 0.40
  - Cancellation: 0.25
  - Default: 0.05
- **Use Case**: Proactive complaint prevention
- **Example**: 0.40 = 40% chance of complaining

---

### 6. Auxiliary Metrics (metric_group = 'aux')

Meta-information about LM calculations for transparency and debugging.

#### lm_version_tag
- **Type**: Label (string)
- **Calculation**: Version of LM used for calculation
- **Formula**: Directly from LM configuration (e.g., 'lm_v1')
- **Use Case**: Track which LM version calculated metrics
- **Example**: 'lm_v1'

#### calc_profile
- **Type**: Label (string)
- **Calculation**: Calculation scenario/profile used
- **Formula**: Based on call timing:
  - Hour 22-6: 'night_shift_v1'
  - Weekend: 'weekend_v1'
  - Default: 'default_v1'
- **Use Case**: Apply different rules for different contexts
- **Example**: 'night_shift_v1' for late-night calls

---

## Using LM Metrics

### In Reports

LM metrics are automatically integrated into weekly quality reports via `WeeklyQualityService`. Reports show:
- Average conversion scores
- Quality scores from LM
- Risk distribution (high/medium/low churn risk)

### In Dashboards

Use `MetricsService.get_lm_enhanced_metrics()` to retrieve traditional + LM metrics:

```python
metrics = await metrics_service.get_lm_enhanced_metrics(
    period="weekly",
    start_date="2025-12-01",
    end_date="2025-12-07"
)

# Access LM metrics
lm_data = metrics['lm_metrics']
conversion_avg = lm_data['aggregates']['conversion_score']['avg']
high_risk_count = lm_data['risk_distribution']['high']
```

### For Forecasting

Aggregate forecast metrics to predict future trends:

```python
# Get conversion probability forecast
forecast_values = await lm_repo.get_lm_values_by_metric(
    metric_code='conversion_prob_forecast',
    start_date=last_week,
    end_date=today
)

# Calculate expected conversions
expected_conversions = sum(v['value_numeric'] for v in forecast_values)
```

### Worker Automation

Use `LMCalculatorWorker` to automatically calculate metrics:

```python
# Process recent calls (last 24 hours)
worker = LMCalculatorWorker(db_manager)
await worker.process_recent_calls(hours_back=24)

# Backfill for historical data
await worker.backfill_all_calls(
    start_date=datetime(2025, 11, 1),
    end_date=datetime(2025, 12, 1)
)
```

---

## Database Schema

All metrics are stored in the `lm_value` table:

```sql
SELECT * FROM lm_value
WHERE history_id = 12345
ORDER BY metric_group, metric_code;
```

Key fields:
- `history_id`: Links to call_history
- `metric_code`: Unique metric identifier
- `metric_group`: Category (operational/conversion/quality/risk/forecast/aux)
- `value_numeric`: Numeric value
- `value_label`: Categorical value
- `value_json`: Complex data as JSON
- `lm_version`: LM version used
- `calc_method`: Calculation method (rule/tree/gbm)

---

## Metric Evolution

As the system learns and improves, metric calculations can evolve:

1. **Rule-based (current)**: Simple if-then logic
2. **Tree-based**: Decision trees for complex rules
3. **ML-based**: Gradient boosting or neural networks

The `lm_version` and `calc_method` fields track which version calculated each metric, enabling A/B testing and gradual rollout of new models.

---

## Best Practices

1. **Always specify date ranges** when aggregating metrics
2. **Use metric_group filters** for faster queries
3. **Monitor LM version distribution** to ensure consistency
4. **Backfill carefully** - use batching to avoid database overload
5. **Combine with traditional metrics** for comprehensive analysis

---

## Support and Questions

For questions about specific metrics or implementation details, contact the development team or refer to the source code in `app/services/lm_service.py`.
