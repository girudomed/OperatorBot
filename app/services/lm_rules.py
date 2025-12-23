
"""
Конфигурация детерминированных правил для LM.
Определяет пороги, статусы, тексты оснований и действия.
"""

from typing import Dict, Any, List, Optional

FOLLOWUP_REFUSAL_CODES = {
    "PATIENT_WILL_CLARIFY",
    "CALL_BACK_LATER",
    "THINKING",
    "NO_TIME",
    "NEEDS_DECISION",
}

COMPLAINT_EXCLUDED_REFUSAL_CODES = {
    "SERVICE_NOT_PROVIDED",
    "AGE_RESTRICTION",
    "DOCUMENTS_REQUIRED",
}

# Пороги для метрик
METRIC_CONFIG = {
    "complaint_risk_flag": {
        "name": "Риск жалобы",
        "red": 60,
        "yellow": 30,
        "min_n": 30,
        "action_code": "open_list_complaint",
        "action_text": "Проверить запись/условия, связаться в течение 24 часов, зафиксировать исход в карточке.",
        "unit": "баллов",
    },
    "followup_needed_flag": {
        "name": "Контроль дозвона",
        "red_share": 0.40,
        "yellow_share": 0.20,
        "min_n": 30,
        "action_code": "open_list_followup",
        "action_text": "Перезвонить в течение 24 часов и зафиксировать исход (дозвон/перенос/запись).",
        "unit": "%",
    },
    "lost_opportunity_score": {
        "name": "Упущенная выручка",
        "red": 60,
        "yellow": 40,
        "min_n": 30,
        "action_code": "open_list_lost",
        "action_text": "Разобрать причину отказа и внести в классификатор + перезвон (если можно).",
        "unit": "баллов",
    },
    "normalized_call_score": {
        "name": "Скор качества",
        "red": 60,
        "yellow": 75, # Для скора инвертировано (ниже - хуже)
        "min_n": 30,
        "action_code": "open_list_quality",
        "action_text": "Провести работу с оператором по чек-листу.",
        "unit": "",
    }
}

# Правила "Почему" (Evidence Rules)
EVIDENCE_RULES = {
    "followup": [
        {
            "id": "refusal_callback",
            "condition": lambda item: (item.get('refusal_category_code') or '').upper() in FOLLOWUP_REFUSAL_CODES,
            "text": "Пациент ждёт ответ (refusal_category_code={refusal_category_code})."
        },
        {
            "id": "target_no_record",
            "condition": lambda item: item.get('is_target') == 1 and item.get('outcome') != 'record',
            "text": "Целевой звонок без записи (outcome={outcome})."
        },
        {
            "id": "lead_category_no_booking",
            "condition": lambda item: item.get('call_category') == 'Лид (без записи)' and item.get('outcome') != 'record',
            "text": "Лид без записи — нужен дозвон."
        },
        {
            "id": "tech_fail",
            "condition": lambda item: str(item.get('call_category') or '').lower() == 'сбой',
            "text": "Разговор оборвался из-за сбоя."
        }
    ],
    "complaints": [
        {
            "id": "category_complaint",
            "condition": lambda item: item.get('call_category') == 'Жалоба' or item.get('number_category') == 7,
            "text": "Категория: Жалоба"
        },
        {
            "id": "trigger_word",
            "condition": lambda item: item.get('has_trigger_word', False),
            "text": "Триггер в речи: найдено слово «{trigger_word}»"
        },
        {
            "id": "refusal_group_risk",
            "condition": lambda item: (
                item.get('refusal_group') in ('сервис', 'время', 'врач', 'качество')
                and (item.get('refusal_category_code') or '').upper() not in COMPLAINT_EXCLUDED_REFUSAL_CODES
            ),
            "text": "Отмена/отказ по группе: {refusal_group}"
        }
    ],
    "lost": [
        {
            "id": "target_no_record",
            "condition": lambda item: (item.get('is_target') == 1 and item.get('outcome') != 'record') or item.get('call_category') == 'Лид (без записи)',
            "text": "Потеря: не записали целевого клиента (outcome={outcome})."
        },
        {
            "id": "low_score",
            "condition": lambda item: float(item.get('call_score', 10) or 10) <= 4,
            "text": "Низкий call_score ({call_score}) — клиент остался недоволен."
        },
        {
            "id": "no_refusal_reason",
            "condition": lambda item: not item.get('refusal_category_code') or item.get('refusal_category_code') == 'OTHER_REASON',
            "text": "Причина отказа не зафиксирована (OTHER/NULL)"
        }
    ]
}

def get_badge(value: Optional[float], config: Optional[Dict[str, Any]]) -> str:
    """Возвращает статус-бейдж по значению и конфигу."""
    if value is None or config is None:
        return "⚪"

    red = config.get("red")
    yellow = config.get("yellow")
    if red is None or yellow is None:
        return "⚪"
    
    # Для метрик типа скора, где меньше - хуже
    if config.get("name") == "Скор качества":
        if value < red: return "🔴"
        if value < yellow: return "🟡"
        return "🟢"
        
    if value >= red: return "🔴"
    if value >= yellow: return "🟡"
    return "🟢"

def decline_word(n: int, forms: List[str]) -> str:
    """Склонение существительных (1 звонок, 2 звонка, 5 звонков)."""
    n = abs(n) % 100
    n1 = n % 10
    if 10 < n < 20: return forms[2]
    if 1 < n1 < 5: return forms[1]
    if n1 == 1: return forms[0]
    return forms[2]
