from decimal import Decimal, ROUND_HALF_UP
import logging

# Mock constants for testing
LM_SCORE_METRICS = (
    "conversion_score",
    "normalized_call_score",
    "lost_opportunity_score",
    "cross_sell_potential",
    "complaint_risk_flag",
)

LM_PROBABILITY_METRICS = (
    "conversion_prob_forecast",
    "second_call_prob",
    "complaint_prob",
)

LM_SCRIPT_METRIC = "script_risk_index"

def _sanitize_value_numeric(value_numeric, metric_code):
    if value_numeric is None:
        return None
    try:
        value = Decimal(str(value_numeric))
    except (TypeError, ValueError):
        return None
    
    max_limit = Decimal("999999.9999")
    
    # Specific clamping based on metric type
    if metric_code in LM_SCORE_METRICS or metric_code == LM_SCRIPT_METRIC:
        value = max(Decimal("0.0000"), min(value, Decimal("100.0000")))
    elif metric_code in LM_PROBABILITY_METRICS:
        value = max(Decimal("0.0000"), min(value, Decimal("1.0000")))
    else:
        value = max(min(value, max_limit), -max_limit)

    value = value.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    return float(value)

def test_sanitization():
    # Test scores (0-100)
    assert _sanitize_value_numeric(150, "conversion_score") == 100.0
    assert _sanitize_value_numeric(-10, "conversion_score") == 0.0
    assert _sanitize_value_numeric(75.123456, "conversion_score") == 75.1235
    
    # Test probabilities (0-1)
    assert _sanitize_value_numeric(1.5, "complaint_prob") == 1.0
    assert _sanitize_value_numeric(-0.5, "complaint_prob") == 0.0
    assert _sanitize_value_numeric(0.123456, "complaint_prob") == 0.1235
    
    # Test other metrics
    assert _sanitize_value_numeric(1000000, "other") == 999999.9999
    assert _sanitize_value_numeric(-1000000, "other") == -999999.9999
    
    print("All sanitization tests passed!")

if __name__ == "__main__":
    test_sanitization()
