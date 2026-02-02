import datetime

from app.services.reports import ReportService


def _service():
    return ReportService.__new__(ReportService)


def test_resolve_dates_invalid_date_range_falls_back():
    svc = _service()
    start, end = svc._resolve_dates("daily", "bad-date")
    assert isinstance(start, datetime.datetime)
    assert isinstance(end, datetime.datetime)
    assert start.hour == 0 and start.minute == 0
    assert end.hour == 23 and end.minute == 59


def test_calculate_metrics_handles_missing_fields():
    svc = _service()
    scores = [
        {"call_score": 8, "talk_duration": "10", "outcome": "record"},
        {"talk_duration": "abc", "outcome": "lead_no_record"},
        {"call_score": None, "talk_duration": None, "outcome": "info_only"},
    ]
    metrics = svc._calculate_metrics_from_scores(scores)
    assert metrics["total_calls"] == 3
    assert metrics["booked_services"] == 1
    assert metrics["lead_no_record"] == 1


def test_calculate_metrics_null_coverage_rates_are_none():
    svc = _service()
    scores = [
        {"outcome": "record", "call_score": 5, "talk_duration": 10},
        {"outcome": "lead_no_record", "call_score": 4, "talk_duration": 5},
    ]
    metrics = svc._calculate_metrics_from_scores(scores)
    assert metrics["objection_present_coverage"] == 0
    assert metrics["objection_present_rate"] is None


def test_calculate_metrics_counts_unknowns():
    svc = _service()
    scores = [
        {"objection_present": 1, "objection_handled": None},
        {"booking_attempted": 1, "next_step_clear": None},
        {"outcome": "lead_no_record", "followup_captured": None},
    ]
    metrics = svc._calculate_metrics_from_scores(scores)
    assert metrics["count_objection_handled_unknown"] == 1
    assert metrics["count_booking_next_step_unknown"] == 1
    assert metrics["count_lead_followup_unknown"] == 1


def test_calculate_metrics_info_calls_outcome_priority():
    svc = _service()
    scores = [
        {"outcome": "info_only", "call_category": "Инфо", "talk_duration": 5},
        {"outcome": "record", "call_category": "Инфо", "talk_duration": 5},
        {"outcome": "", "call_category": "подтверждение", "talk_duration": 5},
        {"outcome": None, "call_category": "пропущенный", "talk_duration": 5},
    ]
    metrics = svc._calculate_metrics_from_scores(scores)
    # info_only counted by outcome, category only when outcome missing
    assert metrics["info_calls"] == 3


def test_calculate_metrics_cancellations_strict():
    svc = _service()
    scores = [
        {"outcome": "cancel", "call_category": "Отмена записи"},
        {"outcome": "lead_no_record", "call_category": "Отмена записи"},
        {"outcome": "record", "call_category": "Без отказа"},
        {"outcome": "record", "refusal_reason": "не интересно"},
    ]
    metrics = svc._calculate_metrics_from_scores(scores)
    # cancel by outcome or category containing "отмен"
    assert metrics["total_cancellations"] == 2


def test_build_call_examples_handles_missing_id_and_zero_score():
    svc = _service()
    scores = [
        {"call_score": 0, "outcome": "record", "transcript": "ок", "requested_service_name": "УЗИ"},
        {"call_score": 5, "outcome": "lead_no_record", "transcript": "нет", "requested_service_name": None},
    ]
    text = svc._build_call_examples(scores, limit=2)
    assert "Оценка: 0" in text
    assert "### Звонок" in text


def test_calculate_metrics_skips_invalid_rows():
    svc = _service()
    scores = [
        {"call_score": 7, "talk_duration": 10, "outcome": "record"},
        "bad-row",
        None,
    ]
    metrics = svc._calculate_metrics_from_scores(scores)
    assert metrics["total_calls"] == 1
    assert metrics["booked_services"] == 1


def test_build_call_examples_skips_invalid_rows():
    svc = _service()
    scores = [
        {"call_score": 2, "outcome": "record", "transcript": "тест", "requested_service_name": "УЗИ"},
        123,
    ]
    text = svc._build_call_examples(scores, limit=1)
    assert "### Звонок" in text
