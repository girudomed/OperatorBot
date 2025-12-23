
import pytest
from unittest.mock import MagicMock
from app.services.lm_service import LMService

@pytest.fixture
def lm_service():
    repo = MagicMock()
    return LMService(repo)

def test_get_float_safe(lm_service):
    # Тест хелпера
    assert lm_service._get_float({'a': 5.5}, 'a') == 5.5
    assert lm_service._get_float({'a': '10.2'}, 'a') == 10.2
    assert lm_service._get_float({'a': None}, 'a', default=1.0) == 1.0
    assert lm_service._get_float({'a': 'not_a_number'}, 'a', default=0.0) == 0.0
    assert lm_service._get_float({}, 'missing') == 0.0

def test_response_speed_calculations(lm_service):
    # 1. Корректные данные
    score, label = lm_service._calculate_response_speed({'await_sec': 10, 'talk_duration': 60})
    assert score == 5.0 and label == 'green'
    
    # 2. Длинное ожидание
    score, label = lm_service._calculate_response_speed({'await_sec': 150, 'talk_duration': 60})
    assert score == 1.0 and label == 'red'
    
    # 3. "Грязные" данные (None/Missing/String)
    score, label = lm_service._calculate_response_speed({'await_sec': None, 'talk_duration': '60'})
    assert score == 5.0 # await_sec=0 default

    # 4. Пропущенный/Короткий звонок
    score, label = lm_service._calculate_response_speed({'await_sec': 10, 'talk_duration': 0.5})
    assert score == 1.0 and label == 'red'

def test_talk_efficiency_robustness(lm_service):
    # Нулевая или отрицательная длительность не должна вызывать ZeroDivisionError
    assert lm_service._calculate_talk_efficiency({'talk_duration': 0}) == 0.0
    assert lm_service._calculate_talk_efficiency({'talk_duration': -10}) == 0.0
    assert lm_service._calculate_talk_efficiency({'talk_duration': None}) == 0.0
    
    # Очень длинный звонок должен быть ограничен min/max
    assert lm_service._calculate_talk_efficiency({'talk_duration': 600}) == 100.0

def test_risk_metrics_robustness(lm_service):
    # Проверка на None в скоринге
    level, score = lm_service._calculate_churn_risk(None)
    assert level == 'LOW' and score == 0.0
    
    # Проверка на пустой словарь
    level, score = lm_service._calculate_churn_risk({})
    assert level == 'LOW' and score == 10.0

def test_forecast_robustness(lm_service):
    # Вероятности не должны падать при отсутствии данных
    assert lm_service._forecast_conversion_probability(None) == 0.1
    assert lm_service._forecast_second_call_probability({}, None) == 0.3
    assert lm_service._forecast_complaint_probability(None) == 0.05

def test_script_risk_edge_cases(lm_service):
    # Низкий скор -> Высокий риск
    assert lm_service._calculate_script_risk({'call_score': 1}) == 80.0
    # Высокий скор -> Низкий риск
    assert lm_service._calculate_script_risk({'call_score': 9}) == 10.0
    # Жалоба перекрывает базовый риск
    assert lm_service._calculate_script_risk({'call_score': 8, 'call_category': 'Жалоба'}) == 50.0 # (10-8)*10 + 30 = 50

def test_conversion_score_robustness(lm_service):
    # Разные исходы
    assert lm_service._calculate_conversion_score({'outcome': 'record'}) == 100.0
    assert lm_service._calculate_conversion_score({'outcome': 'lead_no_record'}) == 50.0
    assert lm_service._calculate_conversion_score({'call_category': 'Информационный'}) == 20.0
    assert lm_service._calculate_conversion_score(None) == 0.0
