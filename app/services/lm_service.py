# -*- coding: utf-8 -*-
# Файл: app/services/lm_service.py

"""
Сервис расчета LM метрик.

LM (Learning/Logic Model) - аналитический слой для расчета метрик по звонкам.
Рассчитывает 6 категорий метрик: операционные, конверсионные, качество, риски, прогнозы, вспомогательные.
"""

from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING
from datetime import datetime
import re

from app.db.repositories.lm_repository import LMRepository
from app.db.models import CallRecord, CallHistoryRecord
from app.logging_config import get_watchdog_logger
from app.services.lm_weights import ComplaintWeightMatrix

if TYPE_CHECKING:
    from app.db.repositories.lm_dictionary_repository import LMDictionaryRepository

logger = get_watchdog_logger(__name__)

# LM Configuration
LM_VERSION = "v1912"
DEFAULT_CALC_METHOD = "rule"

COMPLAINT_LEGAL_KEYWORDS = [
    "жалоб",
    "напис",
    "прокуратур",
    "роспотреб",
    "суд",
    "полици",
    "обман",
    "мошен",
    "хам",
    "груб",
    "неадекват",
    "обязаны",
    "верните деньги",
    "буду разбираться",
    "записывалась",
    "записывался",
    "претензи",
    "заявлен",
    "адвокат",
    "юрист",
    "право",
    "закон",
    "нарушен",
    "официальн",
    "досудебн",
    "защит",
    "потребител",
    "инстанци",
    "накажу",
    "жаловаться",
    "жалобу",
]

COMPLAINT_BEHAVIOR_KEYWORDS = [
    "хам",
    "груб",
    "оскорб",
    "обман",
    "нагруб",
    "угрожа",
    "мошен",
    "орать",
    "кричат",
    "некомпетент",
    "безобрази",
    "кошмар",
    "ужас",
    "плохо",
    "отвратительн",
    "дерзко",
    "невоспитан",
    "издевательств",
    "нахамил",
    "оскорбил",
    "повысил голос",
    "бросил трубку",
]

COMPLAINT_PROCESS_KEYWORDS = [
    "не предупред",
    "не сказали",
    "дороже",
    "перепутали",
    "не тот врач",
    "не туда записали",
    "не то время",
    "ошибка",
    "ошиблись",
    "потеряли",
    "договаривались",
    "ждал",
    "ожидани",
    "не дозвонился",
    "дорого",
    "цена",
    "стоимость",
    "прайс",
    "не соответсвует",
    "не пришел",
    "опоздал",
    "задержали",
    "не принял",
    "отменили",
    "без согласия",
    "возврат",
    "чек",
    "квитанци",
    "записали не к тому",
    "врач не приехал",
    "клиника закрыта",
]

COMPLAINT_REFUND_KEYWORDS = [
    "верните деньги",
    "вернуть деньги",
    "возврат",
    "компенсац",
    "деньги назад",
    "верните",
]

COMPLAINT_STOP_PHRASES = [
    "претензий нет",
    "жалоб нет",
    "не жалуюсь",
    "без жалоб",
    "жалоба на здоровье",
    "жалоба на боль",
    "болит",
    "не к вам претензия",
    "жаловаться не буду",
]

COMPLAINT_THREAT_KEYWORDS = [
    "буду жаловаться",
    "подам жалобу",
    "напишу жалобу",
    "обращусь в министерство",
    "обращусь в росздравнадзор",
    "напишу в прокуратуру",
    "обращусь к руководству",
    "позову старшего",
    "соедините с руководителем",
    "дайте главного врача",
]

COMPLAINT_BLAME_KEYWORDS = [
    "вы обязаны",
    "вы должны были",
    "почему вы не",
    "как так получилось",
    "это неправильно",
    "так не делают",
    "это недопустимо",
    "это нарушение",
    "вы меня обманули",
    "вы ввели в заблуждение",
]

COMPLAINT_IRRITATION_KEYWORDS = [
    "меня это не устраивает",
    "мне это не подходит",
    "я недоволен",
    "я возмущен",
    "я возмущена",
    "меня не устраивает",
    "что за отношение",
    "почему так",
    "что за сервис",
]

COMPLAINT_EMOTION_KEYWORDS = [
    "ужас",
    "кошмар",
    "беспредел",
    "бардак",
    "издеваетесь",
]

COMPLAINT_CONFLICT_ESCALATION = [
    "соедините с руководителем",
    "позовите старшего",
    "сколько можно",
    "я уже звонил",
    "я уже звонила",
    "почему никто",
    "вы не слышите",
]

COMPLAINT_EXPECTATION_KEYWORDS = [
    "обещали",
    "должны были",
    "не сделали",
    "не выполнили",
    "запись сорвалась",
    "запись не состоялась",
    "мы были записаны",
    "меня записали",
    "информация расходится",
]

COMPLAINT_PASSIVE_PHRASES = [
    "ладно, понятно",
    "ясно, спасибо",
    "ну конечно",
    "понятно все",
    "как всегда",
    "спасибо",
    "всего доброго",
    "хорошо, спасибо",
]

COMPLAINT_SENSITIVE_SOURCES = {
    "продокторов",
    "prodoctorov",
    "otzovik",
}

COMPLAINT_GATE_MIN_TALK_SEC = 15
COMPLAINT_GATE_MIN_REPLICAS = 2
COMPLAINT_LONG_CALL_BONUS_SEC = 120

FOLLOWUP_INTENT_KEYWORDS = [
    "перезвоню",
    "уточню",
    "подумаю",
    "посоветуюсь",
    "спрошу",
    "муж",
    "жена",
    "график",
    "посмотрим",
    "наберу вас",
    "я сам",
    "через неделю",
    "позже позвоню",
]

FOLLOWUP_REFUSAL_CODES = {
    "PATIENT_WILL_CLARIFY",
    "CALL_BACK_LATER",
    "THINKING",
    "NO_TIME",
    "NEEDS_DECISION",
}

FOLLOWUP_LEAD_OUTCOMES = {
    "lead_no_record",
    "lead",
    "warm_lead",
    "potential",
}

FOLLOWUP_SPAM_KEYWORDS = [
    "спам",
    "реклама",
    "автоинформатор",
    "робот",
]

FOLLOWUP_AUTO_RESPONSES = [
    "автоответ",
    "автоинформатор",
    "робот",
    "ivr",
]

LOST_SPAM_CATEGORIES = {
    "спам",
    "спам, реклама",
    "реклама",
    "автоинформатор",
    "робот",
}

LOSS_REASON_CODE_MAP = {
    "PRICE": "Цена / дорого",
    "EXPENSIVE": "Цена / дорого",
    "NO_MONEY": "Цена / дорого",
    "NO_TIME": "Неудобное время",
    "SCHEDULE": "Неудобное время",
    "NOT_ACTUAL": "Не актуально",
    "NOT_NEED": "Не актуально",
    "ANOTHER_CLINIC": "Ушёл к конкуренту",
    "NO_TRUST": "Нет доверия",
    "QUALITY": "Недоволен качеством",
}

LOSS_REASON_KEYWORDS = [
    ("Цена / дорого", ("цена", "дорог", "стоим", "не потян", "бюджет", "оплатить")),
    ("Неудобное время", ("время", "распис", "неудоб", "перенес", "перезвон", "позже")),
    ("Нет доверия", ("не довер", "сомне", "боюсь", "опасаюсь", "не увер")),
    ("Не актуально", ("не нуж", "не акту", "передум", "откаж", "запишусь позже")),
    ("Ушёл к конкуренту", ("нашел друг", "нашла друг", "конкур", "в другой", "выбрали друг")),
]

INFO_REQUEST_KEYWORDS = [
    "делаете",
    "делаете ли",
    "можно ли",
    "есть ли",
    "как записаться",
    "сколько стоит",
]

FOLLOWUP_MIN_TALK_SEC = 15
FOLLOWUP_ALLOWED_OUTCOMES = {
    "lead_no_record",
    "lead",
    "info_only",
    "transfer",
    "operator_will_clarify",
    "patient_will_clarify",
    "warm_lead",
    "potential",
    "non_target",
}
FOLLOWUP_BLOCKED_OUTCOMES = {
    "record",
    "cancelled",
    "cancelled_by_patient",
    "cancelled_by_operator",
    "spam",
}

FOLLOWUP_ALLOWED_CODES = {
    "TIME_GENERAL",
    "DOCTOR_UNAVAILABLE",
    "LOCATION_INCONVENIENT",
    "PRICE",
    "URGENCY_NO_SLOTS",
    "PATIENT_WILL_CLARIFY",
    "OPERATOR_WILL_CLARIFY",
}

FOLLOWUP_DENY_CODES = {
    "SERVICE_NOT_PROVIDED",
    "AGE_RESTRICTION",
    "DOCUMENTS_REQUIRED",
    "OTHER_REASON",
}

FOLLOWUP_KEYWORD_MAP = {
    "SERVICE_NOT_PROVIDED": (
        "не предостав",
        "не делаем",
        "нет услуги",
        "нет врача",
        "не сможем",
        "рентген",
        "аппарат отсутствует",
        "нет такого",
        "услуга отсутствует",
        "услуги нет",
        "не оказываем",
        "таких нет",
        "не ставим",
        "не проводим",
    ),
    "TIME_GENERAL": (
        "не могу сегодня",
        "неудобно",
        "позже",
        "время",
        "перезвоните",
        "запишусь позже",
    ),
    "DOCTOR_UNAVAILABLE": (
        "нет врача",
        "врач занят",
        "врач не ведёт",
        "в отпуске",
        "врач не принимает",
    ),
    "LOCATION_INCONVENIENT": (
        "далеко",
        "неудобное расположение",
        "другой район",
        "неподходящий адрес",
    ),
    "PRICE": (
        "дорог",
        "цена",
        "стоим",
        "не потян",
        "бюджет",
    ),
    "URGENCY_NO_SLOTS": (
        "нет записи",
        "нет мест",
        "нет слотов",
        "всё занято",
        "нет времени на сегодня",
    ),
    "PATIENT_WILL_CLARIFY": (
        "повторно свяжется",
        "перезвоню",
        "уточню",
        "подумать",
        "советоваться",
    ),
    "OPERATOR_WILL_CLARIFY": (
        "мы перезвоним",
        "оператор уточнит",
        "свяжемся",
    ),
}


class LMService:
    """Сервис расчета метрик LM."""
    
    def __init__(
        self,
        lm_repository: LMRepository,
        lm_version: str = LM_VERSION,
        dictionary_repository: Optional["LMDictionaryRepository"] = None,
        dictionary_version: str = "v1",
    ):
        self.repo = lm_repository
        self.lm_version = lm_version
        self.dictionary_repo = dictionary_repository
        self.dictionary_version = dictionary_version
        self._dictionary_cache: Dict[str, List[Dict[str, Any]]] = {}
        self.complaint_matrix = ComplaintWeightMatrix()

    # ============================================================================
    # HELPERS
    # ============================================================================

    def _get_float(self, data: Dict[str, Any], key: str, default: float = 0.0) -> float:
        """Безопасное извлечение float."""
        if not data:
            return default
        val = data.get(key)
        if val is None:
            return default
        try:
            return float(val)
        except (ValueError, TypeError):
            return default
    
    def _to_int_flag(self, value: Any) -> int:
        """Преобразует флаг в 0/1, учитывая строковые значения."""
        if isinstance(value, str):
            return 1 if value.strip().lower() in ('1', 'true', 'yes') else 0
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    def _find_keyword(self, text: str, keywords: List[str]) -> Optional[str]:
        if not text:
            return None
        for kw in keywords:
            if kw in text:
                return kw
        return None

    def _count_transcript_replicas(self, transcript: str) -> int:
        if not transcript:
            return 0
        line_based = [line.strip() for line in transcript.splitlines() if line.strip()]
        if len(line_based) >= COMPLAINT_GATE_MIN_REPLICAS:
            return len(line_based)
        # Падение на одной строке — пробуем разбить по фразам.
        sentences = [seg.strip() for seg in re.split(r"[.!?]", transcript) if seg.strip()]
        return len(sentences)

    def _complaint_gate_reason(
        self,
        call_history: Optional[CallHistoryRecord],
        call_score: Optional[CallRecord],
        transcript: str,
        transcript_lower: str,
        talk_duration: float,
        replica_count: int,
    ) -> Optional[Tuple[str, str]]:
        outcome = str(call_score.get("outcome") or "").lower() if call_score else ""
        call_category = str(call_score.get("call_category") or "").lower() if call_score else ""
        call_type = ""
        if call_history:
            call_type = str(call_history.get("call_type") or "").lower()
        if not call_type and call_score:
            call_type = str(call_score.get("call_type") or "").lower()
        refusal_code = str(call_score.get("refusal_category_code") or "").upper() if call_score else ""

        if "сбой" in call_type or call_category == "сбой":
            return "gate_call_fail", "Гейт жалоб: звонок классифицирован как «Сбой»."
        if not transcript_lower or replica_count < COMPLAINT_GATE_MIN_REPLICAS:
            return "gate_short_dialog", "Гейт жалоб: нет живого диалога (меньше двух реплик)."
        if talk_duration and talk_duration < COMPLAINT_GATE_MIN_TALK_SEC:
            return "gate_short_duration", f"Гейт жалоб: короткий разговор ({talk_duration:.0f} с)."
        if refusal_code == "SERVICE_NOT_PROVIDED":
            return "gate_service_missing", "Гейт жалоб: услуга отсутствует (SERVICE_NOT_PROVIDED)."
        if call_score:
            textual_reason = " ".join(
                part
                for part in (
                    str(call_score.get("refusal_reason") or "").strip(),
                    str(call_score.get("result") or "").strip(),
                )
                if part
            ).strip()
            classified_reason = self._classify_followup_reason(
                refusal_code,
                textual_reason or None,
                transcript=transcript,
            )
            if classified_reason == "SERVICE_NOT_PROVIDED":
                return "gate_service_missing", "Гейт жалоб: услуга отсутствует (SERVICE_NOT_PROVIDED)."
        if outcome == "info_only" or call_category == "информационный":
            return "gate_info_call", "Гейт жалоб: информационный звонок без конфликта."
        if any(stop in transcript_lower for stop in FOLLOWUP_AUTO_RESPONSES):
            return "gate_auto", "Гейт жалоб: автоответчик или робот."
        spam_markers = ("спам", "auto", "робот")
        if any(marker in call_category for marker in spam_markers):
            return "gate_spam", "Гейт жалоб: категория «Спам/автоответчик»."
        return None

    def _detect_complaint_core_signals(
        self,
        transcript: str,
        transcript_lower: str,
        formatted_hits: List[Dict[str, Any]],
        call_score: Optional[CallRecord],
    ) -> Dict[str, Dict[str, Any]]:
        signals: Dict[str, Dict[str, Any]] = {
            "negative_emotion": {"hit": False},
            "complaint_phrase": {"hit": False},
            "dialog_conflict": {"hit": False},
            "expectation_violation": {"hit": False},
        }

        def _mark(key: str, reason: str, snippet: Optional[str] = None) -> None:
            slot = signals.setdefault(key, {"hit": False})
            if slot.get("hit"):
                return
            slot["hit"] = True
            if reason:
                slot["reason"] = reason
            if snippet:
                slot["snippet"] = snippet

        def _check_keywords(key: str, keywords: List[str], label: str) -> None:
            kw = self._find_keyword(transcript_lower, keywords)
            if kw:
                idx = transcript_lower.find(kw)
                snippet = self._extract_snippet(transcript, idx, idx + len(kw))
                _mark(key, f"{label}: «{kw}»", snippet)

        _check_keywords("complaint_phrase", COMPLAINT_THREAT_KEYWORDS, "Прямая угроза/жалоба")
        _check_keywords("complaint_phrase", COMPLAINT_BLAME_KEYWORDS, "Претензия клиента")
        _check_keywords("negative_emotion", COMPLAINT_IRRITATION_KEYWORDS, "Эмоциональный негатив")
        _check_keywords("negative_emotion", COMPLAINT_EMOTION_KEYWORDS, "Эмоциональный маркер")
        _check_keywords("dialog_conflict", COMPLAINT_CONFLICT_ESCALATION, "Эскалация диалога")
        _check_keywords("expectation_violation", COMPLAINT_EXPECTATION_KEYWORDS, "Нарушение ожиданий")

        outcome = str(call_score.get("outcome") or "").lower() if call_score else ""
        category = str(call_score.get("call_category") or "").lower() if call_score else ""
        if category.startswith("жалоба"):
            _mark("complaint_phrase", "Категория звонка = «Жалоба».")
        if outcome in ("cancelled", "cancelled_by_patient", "cancelled_by_operator"):
            _mark("expectation_violation", f"Исход звонка: {outcome}.")
        if "отмена записи" in category:
            _mark("expectation_violation", "Категория: отмена записи.")

        for hit in formatted_hits:
            category_key = (hit.get("category") or "").lower()
            snippet = hit.get("snippet")
            term = hit.get("term")
            if category_key == "behavior":
                _mark("negative_emotion", f"Фиксируется негатив: «{term}».", snippet)
            elif category_key in ("legal", "refund"):
                _mark("complaint_phrase", f"Фиксируется претензия «{term}».", snippet)
            elif category_key == "process":
                _mark("expectation_violation", f"Процессная проблема: «{term}».", snippet)

        return signals

    def _parse_result_subscores(self, result_text: Optional[str]) -> Dict[str, float]:
        """
        Парсит текст результата для извлечения суб-скоров (Вариант Б).
        Ожидаемый формат: 'Название: X/10'
        """
        if not result_text:
            return {}
            
        patterns = {
            'initiative_score': r'(?:Инициативность|ведение диалога)[^:]*:\s*(\d+(?:\.\d+)?)\s*/\s*10',
            'politeness_score': r'(?:Вежливость|эмпатия)[^:]*:\s*(\d+(?:\.\d+)?)\s*/\s*10',
            'info_score': r'(?:Информативность)[^:]*:\s*(\d+(?:\.\d+)?)\s*/\s*10',
            'script_score': r'(?:Соблюдение скрипта)[^:]*:\s*(\d+(?:\.\d+)?)\s*/\s*10',
            'satisfaction_score': r'(?:Удовлетворенность)[^:]*:\s*(\d+(?:\.\d+)?)\s*/\s*10',
        }
        
        scores = {}
        for key, pattern in patterns.items():
            match = re.search(pattern, result_text, re.IGNORECASE)
            if match:
                try:
                    scores[key] = float(match.group(1)) * 10.0 # Приводим к 0-100
                except (ValueError, TypeError):
                    pass
        return scores

    async def _get_dictionary_terms(self, dict_code: str) -> Optional[List[Dict[str, Any]]]:
        """Ленивая загрузка словаря с кешированием по версии."""
        if not self.dictionary_repo:
            return None
        cache_key = f"{dict_code}:{self.dictionary_version}"
        if cache_key in self._dictionary_cache:
            return self._dictionary_cache[cache_key]
        terms = await self.dictionary_repo.get_terms(dict_code, version=self.dictionary_version)
        self._dictionary_cache[cache_key] = terms
        return terms

    def _extract_snippet(self, text: str, start: int, end: int, window: int = 30) -> str:
        """Возвращает фрагмент текста вокруг совпадения."""
        if not text:
            return ""
        lo = max(0, start - window)
        hi = min(len(text), end + window)
        snippet = text[lo:hi].strip()
        return snippet

    def _scan_dictionary_terms(
        self,
        transcript: str,
        terms: Optional[List[Dict[str, Any]]],
    ) -> List[Dict[str, Any]]:
        """Находит совпадения словаря в транскрипте."""
        if not transcript or not terms:
            return []
        transcript_lower = transcript.lower()
        matches: List[Dict[str, Any]] = []
        detected_at = datetime.utcnow().isoformat()
        for term in terms:
            raw = term.get("term")
            if not raw:
                continue
            match_type = term.get("match_type", "phrase")
            weight = int(term.get("weight") or 0)
            if not weight:
                continue
            is_negative = bool(term.get("is_negative"))
            occurrences = 0
            sample_start = -1
            sample_end = -1

            if match_type == "regex":
                pattern = raw
                try:
                    compiled = re.compile(pattern, re.IGNORECASE)
                except re.error:
                    logger.warning("Неверное регулярное выражение в словаре: %s", pattern)
                    continue
                all_matches = list(compiled.finditer(transcript))
                occurrences = len(all_matches)
                if occurrences:
                    sample_start = all_matches[0].start()
                    sample_end = all_matches[0].end()
            else:
                # Для stem/phrase ищем по подстроке
                needle = raw.lower()
                if not needle:
                    continue
                index = transcript_lower.find(needle)
                if index >= 0:
                    sample_start = index
                    sample_end = index + len(needle)
                    occurrences = transcript_lower.count(needle)

            if occurrences <= 0:
                continue

            snippet = self._extract_snippet(transcript, sample_start, sample_end)
            matches.append(
                {
                    "term": raw,
                    "match_type": match_type,
                    "weight": weight,
                    "hit_count": occurrences,
                    "snippet": snippet,
                    "is_negative": is_negative,
                    "detected_at": detected_at,
                }
            )
        return matches

    def _classify_complaint_category(
        self,
        term: Optional[str],
        snippet: Optional[str],
        transcript_lower: str,
    ) -> Optional[str]:
        """Определяет категорию триггера жалобы."""
        haystacks = [
            str(term or "").lower(),
            str(snippet or "").lower(),
            transcript_lower,
        ]

        def _contains(keywords: List[str]) -> bool:
            return any(any(kw in text for text in haystacks) for kw in keywords)

        if _contains(COMPLAINT_LEGAL_KEYWORDS):
            return "legal"
        if _contains(COMPLAINT_REFUND_KEYWORDS):
            return "refund"
        if _contains(COMPLAINT_BEHAVIOR_KEYWORDS):
            return "behavior"
        if _contains(COMPLAINT_PROCESS_KEYWORDS):
            return "process"
        if _contains(INFO_REQUEST_KEYWORDS):
            return "info_request"
        spam_keywords = FOLLOWUP_SPAM_KEYWORDS + FOLLOWUP_AUTO_RESPONSES
        if _contains(spam_keywords):
            return "spam"
        return None

    # ============================================================================
    # PRIVATE CALCULATION METHODS - OPERATIONAL
    # ============================================================================

    def _calculate_response_speed(
        self,
        call_history: CallHistoryRecord,
        call_score: Optional[CallRecord] = None
    ) -> Tuple[float, str]:
        """
        Рассчитывает скорость отклика оператора (SST v1912).
        """
        # В call_history время ожидания обычно в поле await_sec
        wait_time = self._get_float(call_history, 'await_sec')
        talk_duration = self._get_float(call_history, 'talk_duration')
        
        if talk_duration <= 1.0: # Звонок не состоялся или очень короткий
            return (1.0, 'red')
            
        if wait_time < 20:
            return (5.0, 'green')
        elif wait_time < 40:
            return (4.0, 'green')
        elif wait_time < 60:
            return (3.0, 'yellow')
        elif wait_time < 120:
            return (2.0, 'red')
        else:
            return (1.0, 'red')

    def _calculate_talk_efficiency(
        self,
        call_history: CallHistoryRecord,
        call_score: Optional[CallRecord] = None
    ) -> float:
        """
        Рассчитывает эффективность разговора по длительности.
        """
        talk_duration = self._get_float(call_history, 'talk_duration')
        
        if talk_duration <= 0:
            return 0.0
        
        if talk_duration >= 60:
            return min(100.0, talk_duration / 3)
        
        return talk_duration * 2

    def _calculate_queue_impact(
        self,
        call_history: CallHistoryRecord,
        call_score: Optional[CallRecord] = None
    ) -> float:
        """
        Рассчитывает влияние на очередь.
        """
        talk_duration = self._get_float(call_history, 'talk_duration')
        if talk_duration <= 0:
            return 0.0
        
        impact = (talk_duration / 300) * 100
        return min(100.0, round(impact, 1))

    # ============================================================================
    # PRIVATE CALCULATION METHODS - CONVERSION
    # ============================================================================

    def _calculate_conversion_score(
        self,
        call_score: Optional[CallRecord]
    ) -> float:
        """
        Рассчитывает скор конверсии.
        """
        if not call_score:
            return 0.0
        
        outcome = str(call_score.get('outcome') or '')
        category = str(call_score.get('call_category') or '')
        
        if outcome == 'record':
            return 100.0
        elif outcome == 'lead_no_record':
            return 50.0
        elif outcome == 'info_only' or category == 'Информационный':
            return 20.0
        else:
            return 0.0

    def _calculate_cross_sell_potential(
        self,
        call_history: CallHistoryRecord,
        call_score: Optional[CallRecord] = None
    ) -> float:
        """
        Оценивает потенциал кросс-продаж.
        """
        target = call_score if call_score else call_history
        if not target:
            return 0.0

        if target.get("is_target") == 1 and target.get("outcome") == "record":
            return 70.0

        if not call_score:
            return 0.0
        
        outcome = call_score.get('outcome')
        if outcome == 'record':
            return 70.0
        
        return 0.0

    def _classify_loss_category(self, call_score: Optional[CallRecord]) -> Optional[Dict[str, Any]]:
        """Пытается определить категорию потери по кодам/текстам."""
        if not call_score:
            return None

        group_raw = str(call_score.get('refusal_group') or '').strip()
        if group_raw:
            return {"label": group_raw, "source": "refusal_group"}

        code = str(call_score.get('refusal_category_code') or '').upper()
        if code and code in LOSS_REASON_CODE_MAP:
            return {"label": LOSS_REASON_CODE_MAP[code], "source": "refusal_category_code"}

        textual_sources = [
            str(call_score.get('refusal_reason') or ''),
            str(call_score.get('result') or ''),
        ]
        for source_text in textual_sources:
            if not source_text:
                continue
            lower_text = source_text.lower()
            for label, keywords in LOSS_REASON_KEYWORDS:
                if any(needle in lower_text for needle in keywords):
                    return {"label": label, "source": "text", "sample": source_text.strip()}
        return None

    def _classify_followup_reason(
        self,
        refusal_code: Optional[str],
        refusal_reason: Optional[str],
        transcript: Optional[str] = None,
    ) -> Optional[str]:
        """Возвращает нормализованный код причины отказа для follow-up."""
        code = (refusal_code or "").strip().upper()
        if code:
            return code
        reason_text_raw = " ".join(filter(None, [refusal_reason, transcript])).lower()
        reason_text = re.sub(r"\s+", " ", re.sub(r"[^\w\s]", " ", reason_text_raw))
        for category, keywords in FOLLOWUP_KEYWORD_MAP.items():
            for kw in keywords:
                if kw and kw in reason_text:
                    return category
        return None

    def _calculate_lost_opportunity(
        self,
        call_history: Optional[CallHistoryRecord],
        call_score: Optional[CallRecord]
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Оценивает упущенную возможность (0-100) и собирает доказательства.
        """
        if not call_score:
            return 0.0, {}

        category_raw = str(call_score.get('call_category') or '')
        category = category_raw.lower()
        if category in LOST_SPAM_CATEGORIES:
            return 0.0, {"reasons": ["Категория 'Спам/Автоответчик' - потери 0."]}

        outcome_raw = str(call_score.get('outcome') or '').strip()
        outcome = outcome_raw.lower()
        is_target = self._to_int_flag(call_score.get('is_target'))

        talk_duration = self._get_float(call_history, 'talk_duration', 0.0) if call_history else 0.0
        call_score_value = self._get_float(call_score, 'call_score', 10.0)
        refusal_reason = str(call_score.get('refusal_reason') or '').strip()
        refusal_code = str(call_score.get('refusal_category_code') or '').strip()
        ai_result = str(call_score.get('result') or '').strip()

        reasons = []
        score = 0.0

        if is_target == 1 and outcome != 'record':
            reasons.append(f"Целевой звонок без записи (исход: {outcome_raw or '—'})")
            score += 60.0
        elif category_raw == 'Лид (без записи)' and outcome != 'record':
            reasons.append("Лид без записи")
            score += 60.0

        if score == 0.0:
            return 0.0, {}

        snippets: List[str] = []
        if talk_duration >= 30:
            reasons.append(f"Предметный разговор ({talk_duration:.0f} сек)")
            score += 10.0
        if call_score_value <= 4.0:
            reasons.append(f"Низкое качество разговора ({call_score_value:.1f}/10)")
            score += 20.0
        
        if refusal_code:
            reasons.append(f"Код отказа: {refusal_code}")
        elif not refusal_reason and not ai_result:
            reasons.append("Причина отказа не заполнена оператором")
            score += 10.0

        if ai_result:
            snippet = ai_result[:160].strip()
            reasons.append(f"Анализ AI: {snippet}" + ("..." if len(ai_result) > 160 else ""))
            snippets.append(snippet)

        loss_meta: Dict[str, Any] = {"reasons": reasons}
        category_info = self._classify_loss_category(call_score)
        if category_info:
            loss_meta["loss_category"] = category_info["label"]
            loss_meta["loss_source"] = category_info.get("source")
            if category_info.get("sample"):
                snippets.append(category_info["sample"])
        else:
            loss_meta["requires_reason"] = True
        if snippets:
            loss_meta["snippets"] = snippets

        return min(100.0, score), loss_meta

    # ============================================================================
    # PRIVATE CALCULATION METHODS - QUALITY
    # ============================================================================

    def _calculate_checklist_coverage(
        self,
        call_score: Optional[CallRecord]
    ) -> float:
        """
        Рассчитывает покрытие чек-листа.
        """
        checklist = self._get_float(call_score, 'number_checklist', 0.0) if call_score else 0.0
        return min(100.0, checklist * 10.0)

    def _calculate_normalized_score(
        self,
        call_score: Optional[CallRecord]
    ) -> float:
        """
        Нормализует оценку звонка к шкале 0-100.
        """
        score = self._get_float(call_score, 'call_score', 0.0) if call_score else 0.0
        return min(100.0, score * 10.0)

    def _calculate_script_risk(
        self,
        call_score: Optional[CallRecord]
    ) -> float:
        """
        Рассчитывает риск отклонения от скрипта.
        """
        if call_score is None:
            return 50.0
        
        score = self._get_float(call_score, 'call_score', 5.0)
        category = str(call_score.get('call_category') or '').lower()
        
        base_risk = (10.0 - score) * 10.0
        if 'жалоба' in category:
            return min(100.0, base_risk + 30.0)
        
        if score < 5:
            return 80.0
        elif score > 7:
            return 10.0
        
        return base_risk

    # ============================================================================
    # PRIVATE CALCULATION METHODS - RISK
    # ============================================================================

    def _calculate_churn_risk(
        self,
        call_score: Optional[CallRecord]
    ) -> Tuple[str, float]:
        """
        Рассчитывает уровень риска оттока (SST v1912).
        """
        if call_score is None:
            return ('LOW', 0.0)
            
        outcome = str(call_score.get('outcome') or '')
        refusal = call_score.get('refusal_reason')
        is_complaint = 'жалоба' in str(call_score.get('call_category') or '').lower()
        
        if refusal and is_complaint:
            return ('CRITICAL', 100.0)
        if refusal or outcome == 'cancel':
            return ('HIGH', 70.0)
        if is_complaint:
            return ('HIGH', 90.0)
            
        return ('LOW', 10.0)

    def _calculate_followup_needed(
        self,
        call_history: Optional[CallHistoryRecord],
        call_score: Optional[CallRecord]
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        Определяет, нужен ли follow-up, и возвращает детализированный контекст.
        """
        if not call_score:
            return False, None

        category_raw = str(call_score.get('call_category') or '')
        category = category_raw.lower()
        if any(tag in category for tag in FOLLOWUP_SPAM_KEYWORDS):
            return False, None

        transcript = str(call_score.get('transcript') or '')
        transcript_lower = transcript.lower()
        has_live_voice = bool(transcript.strip()) and not any(
            stop in transcript_lower for stop in FOLLOWUP_AUTO_RESPONSES
        )

        talk_duration = self._get_float(call_history, 'talk_duration', 0.0) if call_history else self._get_float(call_score, 'talk_duration', 0.0)
        if talk_duration < FOLLOWUP_MIN_TALK_SEC:
            return False, None

        call_success = str(call_score.get('call_success') or '').lower()
        if call_success and call_success not in ('принятый', 'accepted', 'success', '1', 'true'):
            return False, None

        is_target = self._to_int_flag(call_score.get('is_target'))
        if is_target != 1 and 'лид' in category:
            is_target = 1
        if is_target != 1:
            return False, None

        refusal_code = str(call_score.get('refusal_category_code') or '').upper()
        refusal_reason = str(call_score.get('refusal_reason') or '').strip()
        ai_reason = str(call_score.get('result') or '').strip()
        combined_reason = " ".join(part for part in (refusal_reason, ai_reason) if part).strip()
        classified_reason = self._classify_followup_reason(
            refusal_code,
            combined_reason or None,
            transcript=transcript,
        )
        if classified_reason in FOLLOWUP_DENY_CODES:
            return False, None

        outcome_raw = str(call_score.get('outcome') or '').strip()
        outcome = outcome_raw.lower()
        allow_override = classified_reason in FOLLOWUP_ALLOWED_CODES if classified_reason else False
        if outcome in FOLLOWUP_BLOCKED_OUTCOMES and not allow_override:
            return False, None
        if outcome and FOLLOWUP_ALLOWED_OUTCOMES and outcome not in FOLLOWUP_ALLOWED_OUTCOMES and not allow_override:
            return False, None

        context: Dict[str, Any] = {
            "reasons": [],
            "hits": [],
            "sla_hours": 24,
        }

        def _add_reason(reason: str, code: str, *, snippet: Optional[str] = None) -> None:
            if reason:
                context["reasons"].append(reason)
                context.setdefault("reason_codes", []).append(code)
                context.setdefault("reason", context.get("reason") or reason)
            if snippet:
                context.setdefault("snippets", []).append(snippet.strip())

        def _add_hit(term: str, hit_type: str) -> None:
            if not term:
                return
            context.setdefault("hits", []).append({"term": term, "type": hit_type})

        if classified_reason in FOLLOWUP_ALLOWED_CODES:
            messages = {
                "TIME_GENERAL": "Клиенту неудобно текущее время — предложите альтернативный слот.",
                "DOCTOR_UNAVAILABLE": "Врач временно недоступен — предложите другого специалиста или дату.",
                "LOCATION_INCONVENIENT": "Неподходящее расположение — предложите другой филиал.",
                "PRICE": "Возражение по цене — вернитесь с акцией или аргументом ценности.",
                "URGENCY_NO_SLOTS": "Не было свободных слотов. Нужно предложить ближайшее доступное время.",
                "PATIENT_WILL_CLARIFY": "Пациент собирается уточнить детали — важно перезвонить и зафиксировать ответ.",
                "OPERATOR_WILL_CLARIFY": "Оператор пообещал вернуться с ответом — необходимо выполнить обещание.",
            }
            base_message = messages.get(classified_reason, "Нужен повторный контакт по итогам звонка.")
            message = f"{base_message} (код {classified_reason})"
            _add_reason(message, classified_reason)
            if classified_reason == "URGENCY_NO_SLOTS":
                context["priority"] = "HIGH"
                context["sla_hours"] = 24

        # Ищем намерения в транскрипте
        intent_keyword = self._find_keyword(transcript_lower, FOLLOWUP_INTENT_KEYWORDS)
        if intent_keyword and outcome != 'record':
            idx = transcript_lower.find(intent_keyword)
            snippet = self._extract_snippet(transcript, idx, idx + len(intent_keyword))
            reason = f"Клиент выразил намерение («{intent_keyword}»), но итог не зафиксирован."
            _add_reason(reason, "intent", snippet=snippet)
            _add_hit(intent_keyword, "intent")

        if category_raw == 'Лид (без записи)' and (outcome in FOLLOWUP_LEAD_OUTCOMES or not outcome):
            _add_reason("Категория «Лид (без записи)» — необходим дозвон.", "lead_category")

        if outcome == 'non_target' and has_live_voice:
            _add_reason(f"Был реальный клиент, но outcome=non_target (длительность {talk_duration:.0f} c).", "non_target_missed")

        if classified_reason is None and outcome and outcome != 'record':
            _add_reason(f"Целевой звонок без записи (outcome={outcome_raw or '—'}).", "target_no_record")

        if not context["reasons"]:
            return False, None

        if not context.get("hits"):
            context.pop("hits", None)
        if not context.get("snippets"):
            context.pop("snippets", None)
        if not context.get("reason_codes"):
            context.pop("reason_codes", None)

        return True, context

    def _calculate_complaint_risk(
        self,
        call_history: Optional[CallHistoryRecord],
        call_score: Optional[CallRecord],
        dictionary_terms: Optional[List[Dict[str, Any]]] = None,
    ) -> Tuple[float, bool, Dict[str, Any]]:
        """
        Определяет риск жалобы и возвращает (score 0..100, flag, контекст).
        """
        transcript = ""
        if call_score:
            transcript = str(call_score.get("transcript") or "")
        transcript_lower = transcript.lower()
        talk_duration = self._get_float(call_history, "talk_duration", 0.0) if call_history else self._get_float(call_score, "talk_duration", 0.0)
        replica_count = self._count_transcript_replicas(transcript)

        gate_result = self._complaint_gate_reason(
            call_history,
            call_score,
            transcript,
            transcript_lower,
            talk_duration,
            replica_count,
        )
        if gate_result:
            code, message = gate_result
            return 0.0, False, {"reasons": [message], "gate": True, "gate_code": code}

        reasons: List[str] = []
        rule_hits: List[str] = []
        dictionary_hits_summary: List[str] = []
        score = 0.0

        dictionary_hits = self._scan_dictionary_terms(transcript, dictionary_terms)
        formatted_hits: List[Dict[str, Any]] = []
        category_breakdown: Dict[str, float] = {}
        for hit in dictionary_hits:
            impact = hit["weight"] * hit["hit_count"]
            if impact <= 0:
                continue
            marker = f"«{hit['term']}»"
            category = self._classify_complaint_category(hit.get("term"), hit.get("snippet"), transcript_lower)
            adjusted = self.complaint_matrix.apply_multiplier(category, impact)
            if hit.get("is_negative"):
                adjusted = -abs(adjusted)
            formatted_hits.append(
                {
                    "term": hit["term"],
                    "weight": hit["weight"],
                    "count": hit["hit_count"],
                    "impact": adjusted,
                    "snippet": hit.get("snippet"),
                    "negative": bool(hit.get("is_negative")),
                    "category": category,
                }
            )
            category_key = category or "other"
            category_breakdown.setdefault(category_key, 0.0)
            category_breakdown[category_key] += adjusted
            if adjusted <= 0 and category in ("info_request", "spam"):
                msg = f"{marker} → {category}: исключаем из жалоб."
                dictionary_hits_summary.append(msg)
                continue
            msg = f"Триггер {marker}"
            if category:
                msg += f" [{category}]"
            dictionary_hits_summary.append(msg)
        signals = self._detect_complaint_core_signals(transcript, transcript_lower, formatted_hits, call_score)
        core_hits = [key for key, payload in signals.items() if payload.get("hit")]
        if not core_hits:
            return 0.0, False, {
                "reasons": ["Нет признаков претензии или конфликта — жалоба не зафиксирована."],
                "core_signals": signals,
                "dictionary_hits_summary": dictionary_hits_summary[:5],
            }

        anti_phrase = self._find_keyword(transcript_lower, COMPLAINT_PASSIVE_PHRASES)
        if anti_phrase and "complaint_phrase" not in core_hits and "expectation_violation" not in core_hits:
            return 0.0, False, {"reasons": [f"Диалог завершён нейтрально («{anti_phrase}») — жалобы нет."], "core_signals": signals}

        signal_weights = {
            "complaint_phrase": 70,
            "negative_emotion": 30,
            "dialog_conflict": 20,
            "expectation_violation": 40,
        }
        for key in core_hits:
            score += signal_weights.get(key, 0)
        score = min(100.0, score)

        bonuses: List[str] = []
        if score > 0:
            if talk_duration and talk_duration >= COMPLAINT_LONG_CALL_BONUS_SEC:
                score = min(100.0, score + 5)
                bonuses.append("Длительный конфликт (>2 мин).")
            source = str(call_score.get("utm_source_by_number") or "").lower() if call_score else ""
            if source and any(src in source for src in COMPLAINT_SENSITIVE_SOURCES):
                score = min(100.0, score + 5)
                bonuses.append(f"Чувствительный источник: {source}.")

        for key in core_hits:
            payload = signals.get(key) or {}
            reason = payload.get("reason")
            snippet = payload.get("snippet")
            if reason:
                reasons.append(reason)
                rule_hits.append(key)
            if snippet:
                reasons.append(f"⤷ {snippet.strip()}")
        reasons.extend(bonuses)

        result_excerpt = ""
        if call_score:
            result_excerpt = str(call_score.get("result") or "").strip()

        context = {
            "reasons": reasons,
            "combo_flag": True,
            "rule_hits": rule_hits,
            "dictionary_hits": dictionary_hits,
            "dictionary_hits_summary": dictionary_hits_summary[:5],
            "hits": formatted_hits[:5],
            "snippets": [hit.get("snippet") for hit in formatted_hits if hit.get("snippet")][:5],
            "categories": category_breakdown,
            "core_signals": signals,
        }
        if result_excerpt:
            context["result_excerpt"] = result_excerpt[:500]
        return score, True, context

    async def _persist_dictionary_hits(
        self,
        history_id: int,
        complaint_context: Tuple[float, bool, Dict[str, Any]],
        dict_code: str = "complaint_risk",
    ) -> None:
        """Сохраняет словарные хиты из контекста жалобы."""
        if not self.dictionary_repo:
            return
        context_meta = complaint_context[2] if len(complaint_context) > 2 else {}
        hits = (context_meta or {}).get("dictionary_hits")
        if not hits:
            return
        await self.dictionary_repo.save_hits(history_id, dict_code, hits, self.dictionary_version)

    # ============================================================================
    # PRIVATE CALCULATION METHODS - FORECAST
    # ============================================================================

    def _forecast_conversion_probability(
        self,
        call_score: Optional[CallRecord]
    ) -> float:
        """
        Forecasts conversion probability (0.0-1.0).
        """
        if not call_score:
            return 0.1
        
        outcome = str(call_score.get('outcome') or '')
        is_target = call_score.get('is_target', 0)
        
        if outcome == 'record': return 1.0
        if outcome == 'lead_no_record': return 0.35
        if is_target: return 0.25
        return 0.05

    def _forecast_second_call_probability(
        self,
        call_history: CallHistoryRecord,
        call_score: Optional[CallRecord] = None
    ) -> float:
        """
        Прогнозирует вероятность повторного звонка.
        """
        target = call_score if call_score else call_history
        if not target: return 0.3
        
        raw_cat = str(target.get('call_category') or '')
        if raw_cat == "Навигация" or 'навигация' in raw_cat.lower():
            return 0.60

        if not call_score: return 0.3
        
        outcome = str(call_score.get('outcome') or '')
        if outcome == 'lead_no_record': return 0.6
        if outcome == 'record': return 0.2
        if 'жалоба' in raw_cat.lower(): return 0.7
        
        return 0.3

    def _forecast_complaint_probability(
        self,
        call_score: Optional[CallRecord]
    ) -> float:
        """
        Прогнозирует вероятность жалобы.
        """
        if not call_score:
            return 0.05
        
        category = str(call_score.get('call_category') or '').lower()
        score = self._get_float(call_score, 'call_score', 5.0)
        
        if 'жалоба' in category: return 1.0
        if score < 3: return 0.4
        elif score < 5: return 0.2
        return 0.05

    # ============================================================================
    # ORCHESTRATION & SYNC
    # ============================================================================

    def _determine_calc_profile(self, call_history: CallHistoryRecord, call_score: Optional[CallRecord]) -> str:
        call_date = call_history.get('call_date')
        if call_date and isinstance(call_date, datetime):
            if call_date.hour >= 22 or call_date.hour < 6: return 'night_shift_v1'
            if call_date.weekday() >= 5: return 'weekend_v1'
        return 'default_v1'

    def calculate_operational_metrics(self, h_rec, s_rec):
        speed, label = self._calculate_response_speed(h_rec, s_rec)
        return [
            {'metric_code': 'response_speed_score', 'metric_group': 'operational', 'value_numeric': speed, 'value_label': label},
            {'metric_code': 'talk_time_efficiency', 'metric_group': 'operational', 'value_numeric': self._calculate_talk_efficiency(h_rec, s_rec)},
            {'metric_code': 'queue_impact_index', 'metric_group': 'operational', 'value_numeric': self._calculate_queue_impact(h_rec, s_rec)}
        ]

    def calculate_conversion_metrics(self, h_rec, s_rec):
        lost_score, lost_meta = self._calculate_lost_opportunity(h_rec, s_rec)
        return [
            {'metric_code': 'conversion_score', 'metric_group': 'conversion', 'value_numeric': self._calculate_conversion_score(s_rec)},
            {
                'metric_code': 'lost_opportunity_score', 
                'metric_group': 'conversion', 
                'value_numeric': lost_score,
                'value_json': lost_meta,
            },
            {'metric_code': 'cross_sell_potential', 'metric_group': 'conversion', 'value_numeric': self._calculate_cross_sell_potential(h_rec, s_rec)}
        ]

    def calculate_quality_metrics(self, h_rec, s_rec):
        return [
            {'metric_code': 'checklist_coverage_ratio', 'metric_group': 'quality', 'value_numeric': self._calculate_checklist_coverage(s_rec)},
            {'metric_code': 'normalized_call_score', 'metric_group': 'quality', 'value_numeric': self._calculate_normalized_score(s_rec)},
            {'metric_code': 'script_risk_index', 'metric_group': 'quality', 'value_numeric': self._calculate_script_risk(s_rec)}
        ]

    def calculate_risk_metrics(self, h_rec, s_rec, complaint_context: Optional[Tuple[float, bool, Dict[str, Any]]] = None):
        churn_lbl, churn_val = self._calculate_churn_risk(s_rec)
        if complaint_context is None:
            complaint_context = self._calculate_complaint_risk(h_rec, s_rec)
        compl_score, compl_flag, compl_meta = complaint_context
        compl_reasons = (compl_meta or {}).get("reasons", [])
        flw_flag, flw_context = self._calculate_followup_needed(h_rec, s_rec)
        followup_payload = None
        if flw_flag:
            followup_payload = dict(flw_context or {})
            followup_payload.setdefault('reason', (flw_context or {}).get('reason', "Требуется follow-up"))
            followup_payload.setdefault('sla_hours', 24)
        return [
            {'metric_code': 'churn_risk_level', 'metric_group': 'risk', 'value_label': churn_lbl.upper(), 'value_numeric': churn_val},
            {
                'metric_code': 'complaint_risk_flag',
                'metric_group': 'risk',
                'value_label': 'true' if compl_flag else 'false',
                'value_numeric': compl_score,
                'value_json': compl_meta or {'reasons': compl_reasons},
            },
            {
                'metric_code': 'followup_needed_flag',
                'metric_group': 'risk',
                'value_label': 'true' if flw_flag else 'false',
                'value_numeric': 1.0 if flw_flag else 0.0,
                'value_json': followup_payload,
            }
        ]

    def calculate_forecast_metrics(self, h_rec, s_rec, complaint_context: Optional[Tuple[float, bool, Dict[str, Any]]] = None):
        if complaint_context is None:
            complaint_context = self._calculate_complaint_risk(h_rec, s_rec)
        complaint_score = complaint_context[0]
        complaint_prob = min(max(complaint_score, 0.0), 100.0) / 100.0
        return [
            {'metric_code': 'conversion_prob_forecast', 'metric_group': 'forecast', 'value_numeric': self._forecast_conversion_probability(s_rec)},
            {'metric_code': 'second_call_prob', 'metric_group': 'forecast', 'value_numeric': self._forecast_second_call_probability(h_rec, s_rec)},
            {'metric_code': 'complaint_prob', 'metric_group': 'forecast', 'value_numeric': complaint_prob}
        ]

    def calculate_auxiliary_metrics(self, h_rec, s_rec, calc_source: str = "batch"):
        profile = self._determine_calc_profile(h_rec, s_rec)
        return [
            {'metric_code': 'lm_version_tag', 'metric_group': 'aux', 'value_label': self.lm_version},
            {'metric_code': 'calc_profile', 'metric_group': 'aux', 'value_label': profile}
        ]

    async def calculate_all_metrics(
        self,
        history_id: int,
        h_rec: Optional[Dict] = None,
        s_rec: Optional[Dict] = None,
        calc_source: str = "batch",
        call_history: Optional[Dict] = None,
        call_score: Optional[Dict] = None,
    ) -> int:
        try:
            history_record = h_rec or call_history
            score_record = s_rec or call_score
            if history_record is None:
                raise ValueError("call_history (h_rec) is required for LM calculation")
            metrics = []
            metrics.extend(self.calculate_operational_metrics(history_record, score_record))
            metrics.extend(self.calculate_conversion_metrics(history_record, score_record))
            metrics.extend(self.calculate_quality_metrics(history_record, score_record))
            dictionary_terms = await self._get_dictionary_terms("complaint_risk")
            complaint_context = self._calculate_complaint_risk(history_record, score_record, dictionary_terms)
            metrics.extend(self.calculate_risk_metrics(history_record, score_record, complaint_context))
            metrics.extend(self.calculate_forecast_metrics(history_record, score_record, complaint_context))
            if self.dictionary_repo:
                await self._persist_dictionary_hits(history_id, complaint_context)

            flw_flag_dbg, flw_context_dbg = self._calculate_followup_needed(history_record, score_record)
            logger.debug(
                "[LM][calc] history_id=%s "
                "conversion_score=%.2f quality_score=%.2f complaint_score=%.2f reasons=%s followup=%s",
                history_id,
                metrics[3].get('value_numeric', 0) if len(metrics) > 3 else 0,
                metrics[6].get('value_numeric', 0) if len(metrics) > 6 else 0,
                complaint_context[0],
                (complaint_context[2] or {}).get("reasons"),
                {'flag': flw_flag_dbg, 'context': flw_context_dbg},
            )
            metrics.extend(self.calculate_auxiliary_metrics(history_record, score_record, calc_source))
            
            # Вариант Б: Парсим суб-скоры из result
            if score_record and score_record.get('result'):
                subscores = self._parse_result_subscores(score_record['result'])
                for m_code, val in subscores.items():
                    metrics.append({
                        'metric_code': m_code,
                        'metric_group': 'subscore',
                        'value_numeric': val
                    })
            
            profile = self._determine_calc_profile(history_record, score_record)
            score_id = score_record.get('call_scores_id') or score_record.get('id') if score_record else None
            
            payload = []
            for m in metrics:
                payload.append({
                    'history_id': history_id,
                    'call_score_id': score_id,
                    'metric_code': m['metric_code'],
                    'metric_group': m['metric_group'],
                    'value_numeric': m.get('value_numeric'),
                    'value_label': m.get('value_label'),
                    'value_json': m.get('value_json'),
                    'lm_version': self.lm_version,
                    'calc_profile': profile,
                    'calc_method': DEFAULT_CALC_METHOD,
                    'calc_source': calc_source
                })
            return await self.repo.save_lm_values_batch(payload)
        except Exception as e:
            logger.error(f"Failed to calculate metrics for history_id={history_id}: {e}", exc_info=True)
            return 0

    async def sync_new_metrics(self, days: int = 1, limit: int = 100) -> Dict[str, Any]:
        profile = "default_v1"
        watermark = await self.repo.get_calc_watermark(self.lm_version, profile)
        last_id = watermark.get('last_id') or 0
        
        query = """
            SELECT ca.*, ch.talk_duration, ch.await_sec, ch.context_start_time_dt as call_date
            FROM call_analytics ca
            JOIN call_history ch ON ca.history_id = ch.history_id
            WHERE ca.id > %s ORDER BY ca.id ASC LIMIT %s
        """
        rows = await self.repo.db_manager.execute_with_retry(query, (last_id, limit), fetchall=True) or []
        if not rows: return {"processed": 0, "status": "idle"}
            
        processed = 0
        new_id, new_date = last_id, watermark.get('last_score_date')
        
        for row in rows:
            try:
                h_rec = {'history_id': row['history_id'], 'talk_duration': row.get('talk_duration'), 'await_sec': row.get('await_sec'), 'call_date': row.get('call_date')}
                await self.calculate_all_metrics(row['history_id'], h_rec, dict(row), calc_source="sync")
                processed += 1
                new_id = row['id']
                new_date = row.get('synced_at') or row.get('call_date') or new_date
            except Exception as e:
                logger.error(f"Sync error for row {row.get('id')}: {e}")

        if processed > 0:
            await self.repo.update_calc_watermark(self.lm_version, profile, new_date, new_id)
        return {"processed": processed, "last_id": new_id}
