"""
Типы данных для работы с БД.
"""

from typing import TypedDict, Optional
from datetime import datetime


class UserRecord(TypedDict, total=False):
    """Запись пользователя из таблицы UsersTelegaBot."""
    user_id: int
    username: str
    full_name: str
    operator_id: Optional[int]
    role_id: Optional[int]
    extension: Optional[str]
    # Admin panel fields
    status: str  # 'pending', 'approved', 'blocked'
    approved_by: Optional[int]
    blocked_at: Optional[datetime]


class OperatorRecord(TypedDict, total=False):
    """Запись оператора из таблицы users."""
    id: int
    name: str
    extension: str
    user_id: Optional[int]


class CallRecord(TypedDict, total=False):
    """Запись звонка из таблицы call_scores."""
    id: int
    history_id: int
    call_score: float
    score_date: datetime
    called_info: Optional[str]
    call_date: Optional[datetime]
    call_type: Optional[str]
    talk_duration: Optional[int]
    call_success: Optional[str]
    transcript: Optional[str]
    result: Optional[str]
    caller_info: str
    caller_number: Optional[str]
    called_number: Optional[str]
    utm_source_by_number: Optional[str]
    call_category: str
    number_category: int
    number_checklist: Optional[int]
    category_checklist: Optional[str]
    is_target: int
    outcome: Optional[str]
    requested_service_id: Optional[int]
    requested_service_name: Optional[str]
    requested_doctor_id: Optional[int]
    requested_doctor_name: Optional[str]
    requested_doctor_speciality: Optional[str]
    refusal_reason: Optional[str]


class CallHistoryRecord(TypedDict, total=False):
    """Запись из таблицы call_history."""
    id: int
    talk_duration: Optional[int]
    call_type: Optional[str]
    called_info: Optional[str]
    caller_info: Optional[str]
    caller_number: Optional[str]
    called_number: Optional[str]
    recording_id: Optional[str]


class ReportRecord(TypedDict, total=False):
    """
    Запись отчёта из таблицы reports.
    
    ВАЖНО:
    - PK: report_id (не id!)
    - period и report_date — VARCHAR (строки), не DATE!
    """
    report_id: int  # PK
    user_id: int
    name: Optional[str]  # Имя оператора
    period: str  # VARCHAR(20): 'day', 'week', 'month'
    report_date: str  # VARCHAR(50): 'YYYY-MM-DD'
    report_text: Optional[str]
    total_calls: int
    accepted_calls: int
    booked_services: int
    conversion_rate: float
    avg_call_rating: float
    total_cancellations: int
    cancellation_rate: float
    total_conversation_time: int
    avg_conversation_time: float
    avg_spam_time: float
    total_spam_time: int
    avg_navigation_time: float
    complaint_calls: int
    complaint_rating: float
    recommendations: str
    # Дополнительные поля
    missed_calls: Optional[int]
    missed_rate: Optional[float]
    total_leads: Optional[int]


class CallMetrics(TypedDict):
    """Агрегированные метрики звонков."""
    total_calls: int
    avg_talk_time: Optional[float]
    successful_calls: int


class LMValueRecord(TypedDict, total=False):
    """Запись метрики LM из таблицы lm_value."""
    id: int
    history_id: int
    call_score_id: Optional[int]
    metric_code: str
    metric_group: str
    value_numeric: Optional[float]
    value_label: Optional[str]
    value_json: Optional[dict]
    lm_version: str
    calc_method: str
    calc_source: Optional[str]
    created_at: datetime
    updated_at: Optional[datetime]


class DashboardMetrics(TypedDict, total=False):
    """Метрики дашборда для оператора."""
    operator_name: str
    period_type: str  # 'day', 'week', 'month'
    period_start: str  # Дата начала периода
    period_end: str  # Дата окончания периода
    
    # Общая статистика
    total_calls: int
    accepted_calls: int
    missed_calls: int
    records_count: int
    leads_no_record: int
    wish_to_record: int
    conversion_rate: float
    
    # Качество
    avg_score_all: float
    avg_score_leads: float
    avg_score_cancel: float
    
    # Отмены
    cancel_calls: int
    reschedule_calls: int
    cancel_share: float
    
    # Время
    avg_talk_all: int
    total_talk_time: int
    avg_talk_record: int
    avg_talk_navigation: int
    avg_talk_spam: int
    
    # Жалобы
    complaint_calls: int
    avg_score_complaint: float
    
    # ML метрики (опционально)
    expected_records: Optional[float]
    record_uplift: Optional[float]
    hot_missed_leads: Optional[int]
    difficulty_index: Optional[float]


class MLPredictionRecord(TypedDict, total=False):
    """Запись ML-прогноза."""
    history_id: int
    call_score_id: Optional[int]
    ml_p_record: Optional[float]  # Вероятность записи
    ml_score_pred: Optional[float]  # Прогноз оценки
    ml_p_complaint: Optional[float]  # Риск жалобы
    ml_updated_at: Optional[datetime]


class OperatorDashboardCache(TypedDict, total=False):
    """Кешированный дашборд оператора."""
    id: int
    operator_name: str
    period_type: str
    period_start: datetime
    period_end: datetime
    metrics: DashboardMetrics  # JSON с метриками
    cached_at: datetime


class OperatorRecommendation(TypedDict, total=False):
    """Рекомендации для оператора."""
    id: int
    operator_name: str
    report_date: datetime
    recommendations: str
    call_samples_analyzed: int
    generated_at: datetime


class RolePermissions(TypedDict):
    """Разрешения роли."""
    role_id: int
    role_name: str
    can_view_own_stats: bool
    can_view_all_stats: bool
    can_view_dashboard: bool
    can_generate_reports: bool
    can_view_transcripts: bool
    can_manage_users: bool
    can_debug: bool


class LMMetricDictionary(TypedDict, total=False):
    """Запись из таблицы lm_metric_dictionary (если используется)."""
    id: int
    metric_code: str
    metric_group: str
    metric_name: str
    description: Optional[str]
    data_type: str
    formula: Optional[str]
    value_range: Optional[str]
    use_case: Optional[str]
    created_at: datetime
    updated_at: Optional[datetime]


# ============================================================================
# Типы для LM метрик
# ============================================================================

class LMMetricBase(TypedDict, total=False):
    """Базовый тип для метрики LM."""
    metric_code: str
    metric_group: str
    value_numeric: Optional[float]
    value_label: Optional[str]
    value_json: Optional[dict]
    calc_method: str


class LMOperationalMetric(TypedDict):
    """Операционная метрика LM (скорость, эффективность, нагрузка)."""
    metric_code: str  # response_speed_score, talk_time_efficiency, queue_impact_index
    metric_group: str  # 'operational'
    value_numeric: float  # 0-100
    calc_method: str  # 'rule'


class LMConversionMetric(TypedDict):
    """Конверсионная метрика LM (конверсия, потери, cross-sell)."""
    metric_code: str  # conversion_score, lost_opportunity_score, cross_sell_potential
    metric_group: str  # 'conversion'
    value_numeric: float  # 0-100
    calc_method: str  # 'rule'


class LMQualityMetric(TypedDict):
    """Метрика качества LM (чек-лист, скор, риск скрипта)."""
    metric_code: str  # checklist_coverage_ratio, normalized_call_score, script_risk_index
    metric_group: str  # 'quality'
    value_numeric: float  # 0-100
    calc_method: str  # 'rule'


class LMRiskMetric(TypedDict, total=False):
    """Метрика риска LM (отток, жалобы, «Нужно перезвонить»)."""
    metric_code: str  # churn_risk_level, complaint_risk_flag, followup_needed_flag
    metric_group: str  # 'risk'
    value_numeric: float  # 0-100 для churn_risk, 0-100 для complaint, 0/1 для followup
    value_label: str  # 'low'/'medium'/'high' или 'true'/'false'
    calc_method: str  # 'rule'


class LMForecastMetric(TypedDict):
    """Прогнозная метрика LM (вероятности)."""
    metric_code: str  # conversion_prob_forecast, second_call_prob, complaint_prob
    metric_group: str  # 'forecast'
    value_numeric: float  # 0-1 (вероятность)
    calc_method: str  # 'rule'


class LMAuxiliaryMetric(TypedDict, total=False):
    """Вспомогательная метрика LM (версия, профиль)."""
    metric_code: str  # lm_version_tag, calc_profile
    metric_group: str  # 'aux'
    value_label: str  # версия LM или профиль расчета
    calc_method: str  # 'meta'


# ============================================================================
# Admin Panel Models
# ============================================================================

class AdminActionLog(TypedDict, total=False):
    """Запись из таблицы admin_action_logs - аудит действий админов."""
    id: int
    actor_id: int  # Кто выполнил действие
    target_id: Optional[int]  # Над кем выполнено (null для системных)
    action: str  # approve, decline, promote, demote, block, unblock, lookup
    payload_json: Optional[str]  # JSON с дополнительными данными
    created_at: datetime  # Когда выполнено
