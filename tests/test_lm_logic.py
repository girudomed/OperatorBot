
import sys
import os

# Add project root to path
sys.path.append('/Users/vitalyefimov/Projects/operabot')

import re
from app.services.lm_service import LMService
from app.services.lm_rules import METRIC_CONFIG, EVIDENCE_RULES, get_badge

def test_parsing():
    print("Testing Variant B parsing...")
    service = LMService(None, None)
    result_text = """
    Инициативность: 8/10
    Вежливость: 9.5/10
    Информативность: 7/10
    Соблюдение скрипта: 10/10
    Удовлетворенность: 8/10
    """
    scores = service._parse_result_subscores(result_text)
    print(f"Parsed scores: {scores}")
    assert scores['initiative_score'] == 80.0
    assert scores['politeness_score'] == 95.0
    assert scores['script_score'] == 100.0
    print("Parsing test passed!")

def test_evidence_rules():
    print("\nTesting evidence rules...")
    
    # Test followup rules
    item_followup = {
        'refusal_category_code': 'SICKNESS',
        'refusal_group': 'клиент',
        'talk_duration': 15,
        'outcome': 'lead_no_record',
        'is_target': 1,
        'call_category': 'Лид (без записи)'
    }
    
    triggered = []
    for r in EVIDENCE_RULES['followup']:
        if r['condition'](item_followup):
            triggered.append(r['text'].format(**item_followup))
            
    print(f"Followup triggered: {triggered}")
    assert len(triggered) >= 2
    
    # Test complaints rules
    item_complaint = {
        'call_category': 'Жалоба',
        'has_trigger_word': True,
        'trigger_word': 'хам'
    }
    
    triggered_c = []
    for r in EVIDENCE_RULES['complaints']:
        if r['condition'](item_complaint):
            triggered_c.append(r['text'].format(**item_complaint))
            
    print(f"Complaints triggered: {triggered_c}")
    assert "Категория: Жалоба" in triggered_c[0]

    # SERVICE_NOT_PROVIDED не должен формировать жалобу
    service_drop = {
        'refusal_group': 'сервис',
        'refusal_category_code': 'SERVICE_NOT_PROVIDED'
    }
    for r in EVIDENCE_RULES['complaints']:
        assert not r['condition'](service_drop), "SERVICE_NOT_PROVIDED не должен попадать в жалобы"
    
    print("Evidence rules test passed!")

def test_badges():
    print("\nTesting badges...")
    conf = METRIC_CONFIG['complaint_risk_flag']
    assert get_badge(75, conf) == "🔴"
    assert get_badge(45, conf) == "🟡"
    assert get_badge(10, conf) == "🟢"
    
    q_conf = METRIC_CONFIG['normalized_call_score']
    assert get_badge(50, q_conf) == "🔴"
    assert get_badge(70, q_conf) == "🟡"
    assert get_badge(90, q_conf) == "🟢"
    print("Badges test passed!")

if __name__ == "__main__":
    try:
        test_parsing()
        test_evidence_rules()
        test_badges()
        print("\nAll tests passed successfully!")
    except Exception as e:
        print(f"\nTest failed: {e}")
        sys.exit(1)
