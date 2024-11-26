import datetime
import pytest
from unittest.mock import patch, AsyncMock

@pytest.mark.asyncio
async def test_get_date_range(report_generator):
    """Тест для вычисления диапазона дат."""
    start_date, end_date = report_generator.get_date_range("daily")
    assert start_date.date() == end_date.date()
    assert start_date < end_date

@pytest.mark.asyncio
@patch("openai_telebot.AsyncOpenAI")
async def test_generate_report(mock_openai_client, report_generator, mock_db_manager):
    mock_connection = AsyncMock()
    mock_db_manager.get_connection.return_value = mock_connection

    report_generator.get_user_extension = AsyncMock(return_value="123")
    report_generator.get_operator_name = AsyncMock(return_value="Test Operator")
    report_generator.get_operator_data = AsyncMock(return_value={
        "call_history": [{"caller_info": "123", "called_info": "456"}],
        "call_scores": [{"caller_info": "123", "called_info": "456", "call_score": "5"}],
    })
    report_generator.calculate_operator_metrics = AsyncMock(return_value={
    "extension": "123",
    "total_calls": 10,
    "accepted_calls": 8,
    "missed_calls": 2,
    "missed_rate": 20.0,
    "booked_calls": 5,
    "conversion_rate_leads": 50.0,
    "avg_call_rating": 4.5,
    "avg_lead_call_rating": 4.8,
    "total_cancellations": 2,
    "avg_cancel_score": 4.0,
    "cancellation_rate": 40.0,
    "total_conversation_time": 1200.0,
    "avg_conversation_time": 150.0,
    "complaint_calls": 1,
    "complaint_rating": 3.5,
    "avg_time_navigation": 60.0,  # Добавлено
    "avg_time_service_booking": 120.0,  # Добавлено
    "avg_time_spam": 30.0,
    "avg_time_reminder": 90.0,
    "avg_time_cancellation": 50.0,
    "avg_time_complaints": 40.0,
    "avg_time_reservations": 70.0,
    "avg_time_reschedule": 100.0,
})

    mock_openai_client.return_value.chat.completions.create.return_value = AsyncMock(
        choices=[{"message": {"content": "Mocked Report"}}]
    )

    report = await report_generator.generate_report(mock_connection, user_id=1, period="daily")
    assert "Mocked Report" in report
    
@pytest.mark.asyncio
@patch("openai_telebot.execute_async_query")
async def test_get_operator_data(mock_execute_async_query, report_generator):
    """Тест для получения данных оператора."""
    mock_execute_async_query.side_effect = [
        [{"caller_info": "123", "called_info": "456"}],  # call_history
        [{"caller_info": "789", "called_info": "012"}],  # call_scores
    ]

    mock_connection = AsyncMock()
    operator_data = await report_generator.get_operator_data(
        mock_connection, extension="123", start_date="2024-01-01", end_date="2024-01-31"
    )

    assert operator_data["call_history"] == [{"caller_info": "123", "called_info": "456"}]
    assert operator_data["call_scores"] == [{"caller_info": "789", "called_info": "012"}]
    assert mock_execute_async_query.call_count == 2

def test_calculate_avg_score(report_generator):
    """Тест для расчета средней оценки звонков."""
    call_scores = [{"call_score": "5"}, {"call_score": "4"}, {"call_score": None}]
    avg_score = report_generator.calculate_avg_score(call_scores)
    assert avg_score == 4.5

@pytest.mark.asyncio
async def test_calculate_operator_metrics(report_generator):
    mock_connection = AsyncMock()

    # Мокаем результат метода `get_operator_data`
    report_generator.get_operator_data = AsyncMock(return_value={
    "call_history": [
        {"talk_duration": "100", "caller_info": "123", "called_info": "456"}
    ],
    "call_scores": [
        {"call_score": "4.5", "call_category": "Запись на услугу", "caller_info": "123", "called_info": "456"}
    ],
})

    # Вызов метода
    metrics = await report_generator.calculate_operator_metrics(
        mock_connection,
        extension="123",
        start_date="2024-01-01",
        end_date="2024-01-31"
    )

    # Проверяем результаты
    assert metrics == {
        "extension": "123",
        "total_calls": 10,
        "accepted_calls": 8,
        "missed_calls": 2,
        "missed_rate": 20.0,
        "booked_calls": 5,
        "conversion_rate_leads": 50.0,
        "avg_call_rating": 4.5,
        "avg_lead_call_rating": 4.8,
        "total_cancellations": 2,
        "avg_cancel_score": 4.0,
        "cancellation_rate": 40.0,
        "total_conversation_time": 1200.0,
        "avg_conversation_time": 150.0,
        "complaint_calls": 1,
        "complaint_rating": 3.5,
        "avg_time_navigation": 60.0,
        "avg_time_service_booking": 120.0,
        "avg_time_spam": 30.0,
        "avg_time_reminder": 90.0,
        "avg_time_cancellation": 50.0,
        "avg_time_complaints": 40.0,
        "avg_time_reservations": 70.0,
        "avg_time_reschedule": 100.0,
    }