# Файл: app/db/repositories/lm_repository.py

"""
Репозиторий для работы с LM метриками.
"""

from typing import Any, Dict, List, Optional, Tuple
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
import json
import aiomysql

from app.db.manager import DatabaseManager
from app.db.models import LMValueRecord
from app.logging_config import get_watchdog_logger
from app.utils.periods import calculate_period_bounds

logger = get_watchdog_logger(__name__)
MOSCOW_TZ = ZoneInfo("Europe/Moscow")


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

LM_FLAG_METRICS = (
    "followup_needed_flag",
    "complaint_risk_flag",
)

LM_SCRIPT_METRIC = "script_risk_index"
LM_CHURN_METRIC = "churn_risk_level"

LM_METRIC_THRESHOLDS: Dict[str, float] = {
    "lost_opportunity_score": 60.0,
    "cross_sell_potential": 70.0,
    "complaint_prob": 0.3,
     "complaint_risk_flag": 60.0,
    "second_call_prob": 0.4,
    LM_SCRIPT_METRIC: 70.0,
}

LM_SCRIPT_MEDIUM_BAND: Tuple[float, float] = (40.0, 70.0)
LOSS_EXCLUDED_CATEGORIES = (
    'Спам',
    'Спам, реклама',
    'Реклама',
    'Автоинформатор',
    'Робот',
)


class LMRepository:
    """Репозиторий для работы с таблицей lm_value."""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    async def save_lm_value(
        self,
        history_id: int,
        metric_code: str,
        metric_group: str,
        lm_version: str,
        calc_method: str,
        value_numeric: Optional[float] = None,
        value_label: Optional[str] = None,
        value_json: Optional[Dict[str, Any]] = None,
        call_score_id: Optional[int] = None,
        calc_source: Optional[str] = None,
        calc_profile: str = "default_v1",
    ) -> int:
        """
        Сохраняет одно значение метрики LM.
        
        Args:
            history_id: ID звонка из call_history
            metric_code: Код метрики (например, 'conversion_score')
            metric_group: Группа метрики ('operational', 'conversion', 'quality', 'risk', 'forecast', 'aux')
            lm_version: Версия LM (например, 'lm_v1')
            calc_method: Метод расчета ('rule', 'tree', 'gbm', etc.)
            value_numeric: Числовое значение метрики
            value_label: Текстовое значение метрики
            value_json: JSON значение метрики
            call_score_id: ID из call_scores (опционально)
            calc_source: Источник расчета (опционально)
            
        Returns:
            ID созданной или обновленной записи
        """
        # Validate: at least one value must be provided
        if value_numeric is None and value_label is None and value_json is None:
            raise ValueError(f"At least one value must be provided for metric {metric_code}")
        
        value_numeric = self._sanitize_value_numeric(value_numeric, metric_code, history_id)
        # Convert value_json to JSON string if provided
        value_json_str = json.dumps(value_json) if value_json else None
        
        query = """
        INSERT INTO lm_value (
            history_id, call_score_id, metric_code, metric_group,
            value_numeric, value_label, value_json,
            lm_version, calc_profile, calc_method, calc_source,
            calculated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        ) AS new
        ON DUPLICATE KEY UPDATE
            call_score_id = new.call_score_id,
            value_numeric = new.value_numeric,
            value_label = new.value_label,
            value_json = new.value_json,
            lm_version = new.lm_version,
            calc_profile = new.calc_profile,
            calc_method = new.calc_method,
            calc_source = new.calc_source,
            calculated_at = new.calculated_at,
            updated_at = CURRENT_TIMESTAMP
        """
        
        calculated_at = datetime.utcnow()
        params = (
            history_id, call_score_id, metric_code, metric_group,
            value_numeric, value_label, value_json_str,
            lm_version, calc_profile, calc_method, calc_source,
            calculated_at
        )
        
        try:
            result = await self.db_manager.execute_with_retry(query, params)
        except Exception as exc:  # pragma: no cover - зависимость от БД
            # Ловим любые ошибки слоёв БД/менеджера, логируем с трассировкой и поднимаем контролируемую ошибку.
            logger.exception(
                "Failed to execute INSERT for LM value %s (history_id=%s): %s",
                metric_code,
                history_id,
                exc,
            )
            raise RuntimeError(
                f"Database error while saving LM value {metric_code}"
            ) from exc
        
        # Get the inserted/updated ID
        if result:
            # Fetch the record to get ID
            fetch_query = """
            SELECT id FROM lm_value 
            WHERE history_id = %s AND metric_code = %s
            """
            try:
                row = await self.db_manager.execute_with_retry(
                    fetch_query, 
                    (history_id, metric_code), 
                    fetchone=True
                )
            except Exception as exc:  # pragma: no cover - зависимость от БД
                logger.exception(
                    "Failed to fetch LM value ID for %s (history_id=%s): %s",
                    metric_code,
                    history_id,
                    exc,
                )
                raise RuntimeError(
                    f"Database error while confirming LM value {metric_code}"
                ) from exc
            if row:
                logger.debug(f"Saved LM value: {metric_code} for history_id={history_id}")
                return row['id']
        
        raise RuntimeError(f"Failed to save LM value for {metric_code}")

    @staticmethod
    def _sanitize_value_numeric(
        value_numeric: Optional[float],
        metric_code: str,
        history_id: int,
    ) -> Optional[float]:
        if value_numeric is None:
            return None
        try:
            value = Decimal(str(value_numeric))
        except (TypeError, ValueError, InvalidOperation):
            return None
        if value.is_nan() or value == Decimal("Infinity") or value == Decimal("-Infinity"):
            return None
        # DECIMAL(10,4): max 999999.9999
        max_value = Decimal("999999.9999")
        if abs(value) > max_value:
            logger.warning(
                "LM value_numeric out of range for %s (history_id=%s): %s",
                metric_code,
                history_id,
                value,
            )
            value = max(min(value, max_value), -max_value)
        value = value.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        return float(value)

    async def save_lm_values_batch(
        self,
        values: List[Dict[str, Any]]
    ) -> int:
        """
        Пакетное сохранение метрик LM.
        
        Args:
            values: Список словарей с полями для save_lm_value
            
        Returns:
            Количество сохраненных записей
        """
        if not values:
            return 0
        
        saved_count = 0
        for idx, value_data in enumerate(values):
            try:
                await self.save_lm_value(**value_data)
                saved_count += 1
            except (TypeError, ValueError, KeyError) as exc:
                logger.warning(
                    "Skipping LM payload #%s because of invalid input: %s",
                    idx,
                    exc,
                )
            except RuntimeError as exc:
                logger.error(
                    "Failed to persist LM payload #%s: %s",
                    idx,
                    exc,
                )
            except Exception as exc:
                logger.exception(
                    "Unexpected error while saving LM payload #%s",
                    idx,
                )
                raise
        
        logger.info(f"Saved {saved_count}/{len(values)} LM values")
        return saved_count
    
    async def get_group_metrics(
        self,
        metric_group: str,
        days: int = 7
    ) -> Dict[str, Dict[str, float]]:
        """
        Получает агрегированные метрики по группе за последние N дней.
        
        Returns:
            {metric_code: {avg: float, min: float, max: float, count: int}}
        """
        from datetime import datetime, timedelta
        
        start_date = datetime.now() - timedelta(days=days)
        
        query = """
            SELECT 
                metric_code,
                AVG(value_numeric) as avg_value,
                MIN(value_numeric) as min_value,
                MAX(value_numeric) as max_value,
                COUNT(*) as count_value
            FROM lm_value
            WHERE metric_group = %s
            AND created_at >= %s
            AND value_numeric IS NOT NULL
            GROUP BY metric_code
        """
        
        rows = await self.db_manager.execute_with_retry(
            query,
            (metric_group, start_date),
            fetchall=True
        ) or []
        
        result = {}
        for row in rows:
            result[row['metric_code']] = {
                'avg': float(row['avg_value'] or 0),
                'min': float(row['min_value'] or 0),
                'max': float(row['max_value'] or 0),
                'count': int(row['count_value'] or 0)
            }
        
        return result

    async def _fetch_weekly_numeric(
        self,
        metric_code: str,
        start_date: datetime,
        *,
        threshold: Optional[float] = None,
        medium_band: Optional[Tuple[float, float]] = None,
    ) -> List[Dict[str, Any]]:
        """Возвращает агрегаты по метрике с опциональными порогами."""
        extra_select_parts: List[str] = []
        params: List[Any] = []
        if threshold is not None:
            extra_select_parts.append(
                "SUM(CASE WHEN value_numeric >= %s THEN 1 ELSE 0 END) AS above_threshold"
            )
            params.append(threshold)
        if medium_band:
            extra_select_parts.append(
                "SUM(CASE WHEN value_numeric >= %s AND value_numeric < %s THEN 1 ELSE 0 END) "
                "AS medium_band_count"
            )
            params.extend([medium_band[0], medium_band[1]])
        extra_select = ""
        if extra_select_parts:
            extra_select = ",\n                " + ",\n                ".join(extra_select_parts)

        query = f"""
            SELECT
                DATE_FORMAT(created_at, '%%x-W%%v') AS period_key,
                MIN(DATE(created_at)) AS period_start,
                MAX(DATE(created_at)) AS period_end,
                AVG(value_numeric) AS avg_value,
                COUNT(DISTINCT history_id) AS sample_count
                {extra_select}
            FROM lm_value
            WHERE metric_code = %s
              AND created_at >= %s
            GROUP BY period_key
            ORDER BY period_start DESC
        """

        params.extend([metric_code, start_date])
        rows = await self.db_manager.execute_with_retry(
            query,
            tuple(params),
            fetchall=True,
        ) or []
        return rows

    async def _fetch_period_numeric(
        self,
        metric_code: str,
        start_date: datetime,
        end_date: datetime,
        *,
        threshold: Optional[float] = None,
        medium_band: Optional[Tuple[float, float]] = None,
    ) -> Dict[str, Any]:
        """Агрегаты для произвольного периода времени."""
        extra_select_parts: List[str] = []
        params: List[Any] = []
        if threshold is not None:
            extra_select_parts.append(
                "SUM(CASE WHEN value_numeric >= %s THEN 1 ELSE 0 END) AS above_threshold"
            )
            params.append(threshold)
        if medium_band:
            extra_select_parts.append(
                "SUM(CASE WHEN value_numeric >= %s AND value_numeric < %s THEN 1 ELSE 0 END) "
                "AS medium_band_count"
            )
            params.extend([medium_band[0], medium_band[1]])
        extra_select = ""
        if extra_select_parts:
            extra_select = ",\n                " + ",\n                ".join(extra_select_parts)

        query = f"""
            SELECT
                AVG(lv.value_numeric) AS avg_value,
                COUNT(DISTINCT lv.history_id) AS sample_count
                {extra_select}
            FROM lm_value lv
            LEFT JOIN call_history ch ON ch.history_id = lv.history_id
            LEFT JOIN call_scores cs ON cs.history_id = lv.history_id
            WHERE lv.metric_code = %s
              AND COALESCE(ch.context_start_time_dt, cs.call_date, lv.created_at) >= %s
              AND COALESCE(ch.context_start_time_dt, cs.call_date, lv.created_at) < %s
              AND COALESCE(ch.talk_duration, cs.talk_duration, 0) >= 10
        """
        params.extend([metric_code, start_date, end_date])
        row = await self.db_manager.execute_with_retry(
            query,
            tuple(params),
            fetchone=True,
        ) or {}
        return row

    async def _get_base_call_counts(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> Tuple[
        int,
        int,
        int,
        Dict[str, Dict[str, float]],
        List[Dict[str, Any]],
        List[Dict[str, Any]],
        List[Dict[str, Any]],
    ]:
        """Возвращает (total_calls, target_calls, lost_opportunity_count, coverage, booking_stats, loss_breakdown, utm_breakdown)."""
        total_calls = 0
        target_calls = 0
        lost_opportunity_count = 0

        total_query = """
            SELECT
                COUNT(*) AS total_cnt,
                SUM(CASE WHEN cs.is_target = 1 THEN 1 ELSE 0 END) AS target_cnt,
                SUM(
                    CASE
                        WHEN cs.is_target = 1
                             AND (cs.outcome IS NULL OR cs.outcome <> 'record')
                             AND (cs.call_category IS NULL OR cs.call_category NOT IN ('Спам', 'Спам, реклама', 'Реклама', 'Автоинформатор'))
                        THEN 1 ELSE 0
                    END
                ) AS lost_cnt,
                SUM(CASE WHEN cs.transcript IS NOT NULL AND cs.transcript <> '' THEN 1 ELSE 0 END) AS transcript_cnt,
                SUM(CASE WHEN cs.outcome IS NOT NULL AND cs.outcome <> '' THEN 1 ELSE 0 END) AS outcome_cnt,
                SUM(CASE WHEN cs.refusal_reason IS NOT NULL AND cs.refusal_reason <> '' THEN 1 ELSE 0 END) AS refusal_cnt,
                SUM(CASE WHEN cs.called_info IS NOT NULL AND cs.called_info <> '' THEN 1 ELSE 0 END) AS operator_cnt,
                SUM(CASE WHEN cs.utm_source_by_number IS NOT NULL AND cs.utm_source_by_number <> '' THEN 1 ELSE 0 END) AS utm_cnt
            FROM call_scores cs
            LEFT JOIN call_history ch ON ch.history_id = cs.history_id
            WHERE cs.call_date >= %s
              AND cs.call_date < %s
              AND COALESCE(ch.talk_duration, cs.talk_duration, 0) >= 10
        """

        coverage = {
            "transcript": {"count": 0, "percent": 0.0},
            "outcome": {"count": 0, "percent": 0.0},
            "refusal": {"count": 0, "percent": 0.0},
            "operator": {"count": 0, "percent": 0.0},
            "utm": {"count": 0, "percent": 0.0},
        }
        loss_breakdown: List[Dict[str, Any]] = []
        utm_breakdown: List[Dict[str, Any]] = []

        try:
            row = await self.db_manager.execute_with_retry(
                total_query, (start_date, end_date), fetchone=True
            )
            if row:
                total_calls = int(row.get("total_cnt") or 0)
                target_calls = int(row.get("target_cnt") or 0)
                lost_opportunity_count = int(row.get("lost_cnt") or 0)
                if total_calls > 0:
                    coverage["transcript"]["count"] = int(row.get("transcript_cnt") or 0)
                    coverage["transcript"]["percent"] = round(
                        coverage["transcript"]["count"] / total_calls * 100, 1
                    )
                    coverage["outcome"]["count"] = int(row.get("outcome_cnt") or 0)
                    coverage["outcome"]["percent"] = round(
                        coverage["outcome"]["count"] / total_calls * 100, 1
                    )
                    coverage["refusal"]["count"] = int(row.get("refusal_cnt") or 0)
                    coverage["refusal"]["percent"] = round(
                        coverage["refusal"]["count"] / total_calls * 100, 1
                    )
                    coverage["operator"]["count"] = int(row.get("operator_cnt") or 0)
                    coverage["operator"]["percent"] = round(
                        coverage["operator"]["count"] / total_calls * 100, 1
                    )
                    coverage["utm"]["count"] = int(row.get("utm_cnt") or 0)
                    coverage["utm"]["percent"] = round(
                        coverage["utm"]["count"] / total_calls * 100, 1
                    )
        except Exception:
            logger.exception("Не удалось получить статистику звонков из call_scores")

        utm_breakdown_query = """
            SELECT
                COALESCE(NULLIF(TRIM(utm_source_by_number), ''), 'Не указан') AS source_label,
                COUNT(*) AS cnt
            FROM call_scores
            WHERE call_date >= %s
              AND call_date < %s
              AND utm_source_by_number IS NOT NULL
              AND COALESCE(talk_duration, 0) >= 10
            GROUP BY source_label
            ORDER BY cnt DESC
            LIMIT 10
        """
        try:
            rows = await self.db_manager.execute_with_retry(
                utm_breakdown_query,
                (start_date, end_date),
                fetchall=True,
            ) or []
            for record in rows:
                count = int(record.get("cnt") or 0)
                if count <= 0:
                    continue
                label = record.get("source_label") or "Не указан"
                share = (count / total_calls * 100) if total_calls else 0.0
                utm_breakdown.append(
                    {
                        "label": label,
                        "count": count,
                        "share": round(share, 1) if share else 0.0,
                    }
                )
        except Exception:
            logger.exception("Не удалось получить разбивку по источникам")

        loss_placeholder = ", ".join(["%s"] * len(LOSS_EXCLUDED_CATEGORIES))
        loss_group_query = f"""
            SELECT loss_group, COUNT(*) AS cnt
            FROM (
                SELECT
                    CASE
                        WHEN COALESCE(refusal_group, '') <> '' THEN refusal_group
                        WHEN LOWER(COALESCE(refusal_reason, '')) LIKE '%%дорог%%'
                             OR LOWER(COALESCE(result, '')) LIKE '%%дорог%%'
                             OR LOWER(COALESCE(result, '')) LIKE '%%цена%%'
                        THEN 'Цена / дорого'
                        WHEN LOWER(COALESCE(refusal_reason, '')) LIKE '%%врем%%'
                             OR LOWER(COALESCE(result, '')) LIKE '%%врем%%'
                        THEN 'Неудобное время'
                        WHEN LOWER(COALESCE(refusal_reason, '')) LIKE '%%не довер%%'
                             OR LOWER(COALESCE(result, '')) LIKE '%%не довер%%'
                             OR LOWER(COALESCE(result, '')) LIKE '%%сомне%%'
                        THEN 'Нет доверия'
                        WHEN LOWER(COALESCE(refusal_reason, '')) LIKE '%%не нуж%%'
                             OR LOWER(COALESCE(refusal_reason, '')) LIKE '%%не акту%%'
                             OR LOWER(COALESCE(result, '')) LIKE '%%не акту%%'
                             OR LOWER(COALESCE(result, '')) LIKE '%%передум%%'
                        THEN 'Не актуально'
                        WHEN LOWER(COALESCE(refusal_reason, '')) LIKE '%%друг%% клин%%'
                             OR LOWER(COALESCE(result, '')) LIKE '%%конкур%%'
                             OR LOWER(COALESCE(result, '')) LIKE '%%другой%%'
                        THEN 'Ушёл к конкуренту'
                        ELSE 'Не указано'
                    END AS loss_group
                FROM call_scores
                WHERE call_date >= %s
                  AND call_date < %s
                  AND is_target = 1
                  AND (outcome IS NULL OR outcome <> 'record')
                  AND (call_category IS NULL OR call_category NOT IN ({loss_placeholder}))
                  AND COALESCE(talk_duration, 0) >= 10
            ) t
            GROUP BY loss_group
            ORDER BY cnt DESC
            LIMIT 5
        """
        loss_params: Tuple[Any, ...] = (start_date, end_date, *LOSS_EXCLUDED_CATEGORIES)
        try:
            rows = await self.db_manager.execute_with_retry(
                loss_group_query,
                loss_params,
                fetchall=True,
            ) or []
            for record in rows:
                cnt = int(record.get("cnt") or 0)
                if cnt <= 0:
                    continue
                label = record.get("loss_group") or "Не указано"
                share = (cnt / lost_opportunity_count * 100) if lost_opportunity_count else 0.0
                loss_breakdown.append(
                    {
                        "label": label,
                        "count": cnt,
                        "share": round(share, 1) if share else 0.0,
                    }
                )
        except Exception:
            logger.exception("Не удалось получить разбивку причин потерь")

        booking_query = """
            SELECT call_category, COUNT(*) AS cnt
            FROM call_scores
            WHERE outcome = 'record'
              AND call_date >= %s
              AND call_date < %s
              AND COALESCE(talk_duration, 0) >= 10
            GROUP BY call_category
            ORDER BY cnt DESC
            LIMIT 5
        """
        booking_stats = await self.db_manager.execute_with_retry(
            booking_query, (start_date, end_date), fetchall=True
        ) or []

        return (
            total_calls,
            target_calls,
            lost_opportunity_count,
            coverage,
            booking_stats,
            loss_breakdown,
            utm_breakdown,
        )

    async def get_weekly_summary(self, periods: int = 4) -> List[Dict[str, Any]]:
        """
        Возвращает агрегированные LM-метрики по неделям с типизацией и дельтами.
        """
        if periods <= 0:
            return []

        lookback = periods + 1  # +1 для расчета дельты с предыдущей неделей
        start_date = datetime.now() - timedelta(days=lookback * 7)
        period_buckets: Dict[str, Dict[str, Any]] = {}

        def ensure_period(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            key = row.get("period_key")
            if not key:
                return None
            bucket = period_buckets.get(key)
            if not bucket:
                bucket = {
                    "period_key": key,
                    "start_date": row.get("period_start"),
                    "end_date": row.get("period_end"),
                    "metrics": {},
                    "flags": {},
                    "churn": {},
                    "call_count": 0,
                }
                period_buckets[key] = bucket
            else:
                if not bucket["start_date"] and row.get("period_start"):
                    bucket["start_date"] = row.get("period_start")
                if not bucket["end_date"] and row.get("period_end"):
                    bucket["end_date"] = row.get("period_end")
            return bucket

        metric_specs: Dict[str, Dict[str, Any]] = {
            code: {"threshold": LM_METRIC_THRESHOLDS.get(code)}
            for code in (LM_SCORE_METRICS + LM_PROBABILITY_METRICS + (LM_SCRIPT_METRIC,))
        }
        metric_specs[LM_SCRIPT_METRIC]["medium_band"] = LM_SCRIPT_MEDIUM_BAND

        for metric_code, spec in metric_specs.items():
            rows = await self._fetch_weekly_numeric(
                metric_code,
                start_date,
                threshold=spec.get("threshold"),
                medium_band=spec.get("medium_band"),
            )
            for row in rows:
                bucket = ensure_period(row)
                if not bucket:
                    continue
                metric_entry = {
                    "avg": float(row.get("avg_value") or 0.0),
                    "count": int(row.get("sample_count") or 0),
                }
                if "above_threshold" in row:
                    metric_entry["alert_count"] = int(row.get("above_threshold") or 0)
                if "medium_band_count" in row:
                    metric_entry["medium_count"] = int(row.get("medium_band_count") or 0)
                bucket["metrics"][metric_code] = metric_entry
                if metric_code == "conversion_score" and not bucket["call_count"]:
                    bucket["call_count"] = metric_entry["count"]

        if LM_FLAG_METRICS:
            placeholders = ", ".join(["%s"] * len(LM_FLAG_METRICS))
            flag_query = f"""
                SELECT
                    DATE_FORMAT(created_at, '%%x-W%%v') AS period_key,
                    MIN(DATE(created_at)) AS period_start,
                    MAX(DATE(created_at)) AS period_end,
                    metric_code,
                    SUM(CASE WHEN value_label = 'true' THEN 1 ELSE 0 END) AS true_count,
                    COUNT(DISTINCT history_id) AS sample_count
                FROM lm_value
                WHERE metric_code IN ({placeholders})
                  AND created_at >= %s
                GROUP BY period_key, metric_code
                ORDER BY period_start DESC
            """
            flag_rows = await self.db_manager.execute_with_retry(
                flag_query,
                tuple(LM_FLAG_METRICS) + (start_date,),
                fetchall=True,
            ) or []
            for row in flag_rows:
                bucket = ensure_period(row)
                if not bucket:
                    continue
                metric_name = row.get("metric_code")
                if not metric_name:
                    continue
                bucket["flags"][metric_name] = {
                    "true_count": int(row.get("true_count") or 0),
                    "total": int(row.get("sample_count") or 0),
                }
                if not bucket["call_count"]:
                    bucket["call_count"] = int(row.get("sample_count") or 0)

        if LM_CHURN_METRIC:
            churn_query = """
                SELECT
                    DATE_FORMAT(created_at, '%%x-W%%v') AS period_key,
                    MIN(DATE(created_at)) AS period_start,
                    MAX(DATE(created_at)) AS period_end,
                    value_label,
                    COUNT(DISTINCT history_id) AS count_value
                FROM lm_value
                WHERE metric_code = %s
                  AND created_at >= %s
                GROUP BY period_key, value_label
                ORDER BY period_start DESC
            """
            churn_rows = await self.db_manager.execute_with_retry(
                churn_query,
                (LM_CHURN_METRIC, start_date),
                fetchall=True,
            ) or []
            for row in churn_rows:
                bucket = ensure_period(row)
                if not bucket:
                    continue
                label = (row.get("value_label") or "").lower()
                bucket["churn"][label] = int(row.get("count_value") or 0)
                if not bucket["call_count"]:
                    bucket["call_count"] = int(row.get("count_value") or 0)

        period_list = sorted(
            period_buckets.values(),
            key=lambda item: item.get("start_date") or date.min,
            reverse=True,
        )

        for idx, bucket in enumerate(period_list):
            prev = period_list[idx + 1] if idx + 1 < len(period_list) else None
            if not prev:
                continue
            for code, data in bucket["metrics"].items():
                prev_data = prev["metrics"].get(code)
                if not prev_data:
                    continue
                data["delta"] = data["avg"] - float(prev_data.get("avg") or 0.0)

        display = period_list[:periods]
        return display

    async def get_lm_period_summary(self, days: int = 7, *, reference: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Возвращает агрегаты LM-метрик за произвольный период (дней) + дельты к предыдущему периоду.
        """
        if days <= 0:
            raise ValueError("days must be positive")

        current_start, current_end = calculate_period_bounds(days, reference=reference)
        previous_start = current_start - timedelta(days=days)
        previous_end = current_start

        summary: Dict[str, Any] = {
            "period_days": days,
            "start_date": current_start.date(),
            "end_date": (current_end - timedelta(days=1)).date(),
            "metrics": {},
            "flags": {},
            "churn": {},
            "call_count": 0,
        }
        logger.info(
            "[LM] Period resolved: start=%s end=%s (MSK)",
            current_start.astimezone(MOSCOW_TZ),
            (current_end - timedelta(seconds=1)).astimezone(MOSCOW_TZ),
        )

        metric_specs: Dict[str, Dict[str, Any]] = {
            code: {"threshold": LM_METRIC_THRESHOLDS.get(code)}
            for code in (LM_SCORE_METRICS + LM_PROBABILITY_METRICS + (LM_SCRIPT_METRIC,))
        }
        metric_specs[LM_SCRIPT_METRIC]["medium_band"] = LM_SCRIPT_MEDIUM_BAND

        for metric_code, spec in metric_specs.items():
            current = await self._fetch_period_numeric(
                metric_code,
                current_start,
                current_end,
                threshold=spec.get("threshold"),
                medium_band=spec.get("medium_band"),
            )
            previous = await self._fetch_period_numeric(
                metric_code,
                previous_start,
                previous_end,
                threshold=spec.get("threshold"),
                medium_band=spec.get("medium_band"),
            )

            metric_entry = {
                "avg": float(current.get("avg_value") or 0.0),
                "count": int(current.get("sample_count") or 0),
            }
            if "above_threshold" in current:
                metric_entry["alert_count"] = int(current.get("above_threshold") or 0)
            if "medium_band_count" in current:
                metric_entry["medium_count"] = int(current.get("medium_band_count") or 0)
            prev_avg = previous.get("avg_value")
            if prev_avg is not None:
                metric_entry["delta"] = metric_entry["avg"] - float(prev_avg or 0.0)
            summary["metrics"][metric_code] = metric_entry
            if metric_code == "conversion_score" and not summary["call_count"]:
                summary["call_count"] = metric_entry["count"]

        if LM_FLAG_METRICS:
            placeholders = ", ".join(["%s"] * len(LM_FLAG_METRICS))
            flag_query = f"""
                SELECT
                    lv.metric_code,
                    SUM(CASE WHEN lv.value_label = 'true' THEN 1 ELSE 0 END) AS true_count,
                    COUNT(DISTINCT lv.history_id) AS sample_count
                FROM lm_value lv
                LEFT JOIN call_history ch ON ch.history_id = lv.history_id
                LEFT JOIN call_scores cs ON cs.history_id = lv.history_id
                WHERE lv.metric_code IN ({placeholders})
                  AND COALESCE(ch.context_start_time_dt, cs.call_date, lv.created_at) >= %s
                  AND COALESCE(ch.context_start_time_dt, cs.call_date, lv.created_at) < %s
                  AND COALESCE(ch.talk_duration, cs.talk_duration, 0) >= 10
                GROUP BY lv.metric_code
            """
            flag_rows = await self.db_manager.execute_with_retry(
                flag_query,
                tuple(LM_FLAG_METRICS) + (current_start, current_end),
                fetchall=True,
            ) or []
            for row in flag_rows:
                metric_name = row.get("metric_code")
                if not metric_name:
                    continue
                summary["flags"][metric_name] = {
                    "true_count": int(row.get("true_count") or 0),
                    "total": int(row.get("sample_count") or 0),
                }
                if not summary["call_count"]:
                    summary["call_count"] = int(row.get("sample_count") or 0)

        if LM_CHURN_METRIC:
            churn_query = """
                SELECT
                    lv.value_label,
                    COUNT(DISTINCT lv.history_id) AS count_value
                FROM lm_value lv
                LEFT JOIN call_history ch ON ch.history_id = lv.history_id
                LEFT JOIN call_scores cs ON cs.history_id = lv.history_id
                WHERE lv.metric_code = %s
                  AND COALESCE(ch.context_start_time_dt, cs.call_date, lv.created_at) >= %s
                  AND COALESCE(ch.context_start_time_dt, cs.call_date, lv.created_at) < %s
                  AND COALESCE(ch.talk_duration, cs.talk_duration, 0) >= 10
                GROUP BY lv.value_label
            """
            churn_rows = await self.db_manager.execute_with_retry(
                churn_query,
                (LM_CHURN_METRIC, current_start, current_end),
                fetchall=True,
            ) or []
            for row in churn_rows:
                label = (row.get("value_label") or "").lower()
                summary["churn"][label] = int(row.get("count_value") or 0)
                if not summary["call_count"]:
                    summary["call_count"] = int(row.get("count_value") or 0)

        (
            total_calls,
            target_calls,
            lost_opportunity_count,
            coverage,
            booking_stats,
            loss_breakdown,
            utm_breakdown,
        ) = await self._get_base_call_counts(current_start, current_end)
        summary["call_count"] = total_calls or summary["call_count"]
        summary["base"] = {
            "total_calls": total_calls,
            "target_calls": target_calls,
            "non_target_calls": max(total_calls - target_calls, 0),
            "lost_opportunity_count": lost_opportunity_count,
        }
        summary["coverage"] = coverage
        summary["bookings"] = booking_stats
        summary["loss_breakdown"] = loss_breakdown
        summary["utm_breakdown"] = utm_breakdown
        summary["updated_at"] = datetime.now()

        return summary

    async def get_weekly_metrics(
        self,
        metric_groups: Optional[List[str]] = None,
        *,
        weeks: int = 4,
    ) -> List[Dict[str, Any]]:
        """
        Возвращает агрегированные метрики по неделям для указанных групп.

        Returns:
            Список периодов в порядке убывания (последние недели первыми) с агрегатами.
        """
        if weeks <= 0:
            return []

        groups = metric_groups or ["conversion", "quality", "risk", "forecast"]
        if not groups:
            return []

        start_date = datetime.now() - timedelta(days=weeks * 7)
        placeholders = ", ".join(["%s"] * len(groups))
        query = f"""
            SELECT
                DATE_FORMAT(created_at, '%%x-W%%v') AS period_key,
                MIN(DATE(created_at)) AS period_start,
                MAX(DATE(created_at)) AS period_end,
                metric_group,
                metric_code,
                AVG(value_numeric) AS avg_value,
                MIN(value_numeric) AS min_value,
                MAX(value_numeric) AS max_value,
                COUNT(*) AS count_value
            FROM lm_value
            WHERE value_numeric IS NOT NULL
              AND metric_group IN ({placeholders})
              AND created_at >= %s
            GROUP BY
                DATE_FORMAT(created_at, '%%x-W%%v'),
                metric_group,
                metric_code
            ORDER BY period_start DESC
        """

        params = tuple([*groups, start_date])
        rows = await self.db_manager.execute_with_retry(
            query,
            params,
            fetchall=True,
        ) or []

        periods: Dict[str, Dict[str, Any]] = {}
        ordered_keys: List[str] = []

        for row in rows:
            key = row.get("period_key")
            if not key:
                continue

            if key not in periods:
                if len(ordered_keys) >= weeks:
                    continue
                periods[key] = {
                    "period_key": key,
                    "start_date": row.get("period_start"),
                    "end_date": row.get("period_end"),
                    "metrics": {},
                    "total_count": 0,
                }
                ordered_keys.append(key)

            bucket = periods[key]
            group = row.get("metric_group") or "unknown"
            metric_code = row.get("metric_code") or "unknown"
            metric_group_bucket = bucket["metrics"].setdefault(group, {})
            metric_group_bucket[metric_code] = {
                "avg": float(row.get("avg_value") or 0),
                "min": float(row.get("min_value") or 0),
                "max": float(row.get("max_value") or 0),
                "count": int(row.get("count_value") or 0),
            }
            bucket["total_count"] += int(row.get("count_value") or 0)

        return [periods[key] for key in ordered_keys]
    
    async def get_risk_summary(self, days: int = 7) -> Dict[str, int]:
        """
        Получает сводку по метрикам рисков.
        
        Returns:
            {
                'churn_risk_high': int,
                'churn_risk_medium': int,
                'complaint_risk_count': int,
                'followup_needed_count': int
            }
        """
        from datetime import datetime, timedelta
        
        start_date = datetime.now() - timedelta(days=days)
        
        # Churn risk levels
        churn_query = """
            SELECT value_label, COUNT(*) as count
            FROM lm_value
            WHERE metric_code = 'churn_risk_level'
            AND created_at >= %s
            GROUP BY value_label
        """
        
        churn_rows = await self.db_manager.execute_with_retry(
            churn_query,
            (start_date,),
            fetchall=True
        ) or []
        
        churn_counts = {row['value_label']: int(row['count']) for row in churn_rows}
        
        # Complaint risk (flag true)
        complaint_query = """
            SELECT COUNT(*) as count
            FROM lm_value
            WHERE metric_code = 'complaint_risk_flag'
            AND value_label = 'true'
            AND created_at >= %s
        """
        
        complaint_row = await self.db_manager.execute_with_retry(
            complaint_query,
            (start_date,),
            fetchone=True
        )
        
        # Followup needed
        followup_query = """
            SELECT COUNT(*) as count
            FROM lm_value
            WHERE metric_code = 'followup_needed_flag'
            AND value_label = 'true'
            AND created_at >= %s
        """
        
        followup_row = await self.db_manager.execute_with_retry(
            followup_query,
            (start_date,),
            fetchone=True
        )
        
        return {
            'churn_risk_high': churn_counts.get('high', 0),
            'churn_risk_medium': churn_counts.get('medium', 0),
            'churn_risk_low': churn_counts.get('low', 0),
            'complaint_risk_count': int(complaint_row['count'] if complaint_row else 0),
            'followup_needed_count': int(followup_row['count'] if followup_row else 0)
        }

    async def get_lm_values_by_call(
        self,
        history_id: int,
        metric_group: Optional[str] = None
    ) -> List[LMValueRecord]:
        """
        Получает все метрики LM для конкретного звонка.
        
        Args:
            history_id: ID звонка из call_history
            metric_group: Фильтр по группе метрик (опционально)
            
        Returns:
            Список записей LMValueRecord
        """
        query = "SELECT * FROM lm_value WHERE history_id = %s"
        params = [history_id]
        
        if metric_group:
            query += " AND metric_group = %s"
            params.append(metric_group)
        
        query += " ORDER BY metric_group, metric_code"
        
        rows = await self.db_manager.execute_with_retry(
            query, 
            tuple(params), 
            fetchall=True
        )
        
        # Convert JSON strings back to dicts
        result = []
        for row in (rows or []):
            if row.get('value_json'):
                try:
                    row['value_json'] = json.loads(row['value_json'])
                except (json.JSONDecodeError, TypeError):
                    logger.warning(f"Failed to parse value_json for lm_value.id={row.get('id')}")
                    row['value_json'] = None
            result.append(row)
        
        return result

    async def get_lm_values_by_metric(
        self,
        metric_code: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 1000
    ) -> List[LMValueRecord]:
        """
        Получает все значения конкретной метрики за период.
        
        Args:
            metric_code: Код метрики
            start_date: Начало периода (опционально)
            end_date: Конец периода (опционально)
            limit: Максимальное количество записей
            
        Returns:
            Список записей LMValueRecord
        """
        query = "SELECT * FROM lm_value WHERE metric_code = %s"
        params = [metric_code]
        
        if start_date:
            query += " AND created_at >= %s"
            params.append(start_date)
        
        if end_date:
            query += " AND created_at <= %s"
            params.append(end_date)
        
        query += " ORDER BY created_at DESC LIMIT %s"
        params.append(limit)
        
        rows = await self.db_manager.execute_with_retry(
            query, 
            tuple(params), 
            fetchall=True
        )
        
        # Convert JSON strings back to dicts
        result = []
        for row in (rows or []):
            if row.get('value_json'):
                try:
                    row['value_json'] = json.loads(row['value_json'])
                except (json.JSONDecodeError, TypeError):
                    logger.warning(f"Failed to parse value_json for lm_value.id={row.get('id')}")
                    row['value_json'] = None
            result.append(row)
        
        return result

    async def get_aggregated_metrics(
        self,
        metric_codes: List[str],
        start_date: datetime,
        end_date: datetime,
        group_by: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Получает агрегированные метрики за период.
        
        Args:
            metric_codes: Список кодов метрик для агрегации
            start_date: Начало периода
            end_date: Конец периода
            group_by: Поле для группировки (опционально, например 'metric_code')
            
        Returns:
            Список агрегированных значений
        """
        if not metric_codes:
            return []
        
        # Build placeholders for metric_codes
        placeholders = ', '.join(['%s'] * len(metric_codes))
        
        select_fields = []
        if group_by:
            select_fields.append(f"{group_by}")
        
        select_fields.extend([
            "COUNT(*) as count",
            "AVG(value_numeric) as avg_value",
            "MIN(value_numeric) as min_value",
            "MAX(value_numeric) as max_value",
            "STDDEV(value_numeric) as stddev_value"
        ])
        
        query = f"""
        SELECT {', '.join(select_fields)}
        FROM lm_value
        WHERE metric_code IN ({placeholders})
        AND created_at BETWEEN %s AND %s
        """
        
        if group_by:
            query += f" GROUP BY {group_by}"
        
        params = tuple(metric_codes) + (start_date, end_date)
        
        rows = await self.db_manager.execute_with_retry(
            query, 
            params, 
            fetchall=True
        )
        
        return rows or []

    async def delete_lm_values_by_call(self, history_id: int) -> int:
        """
        Удаляет все метрики LM для конкретного звонка.
        
        Args:
            history_id: ID звонка из call_history
            
        Returns:
            Количество удаленных записей
        """
        query = "DELETE FROM lm_value WHERE history_id = %s"
        
        result = await self.db_manager.execute_with_retry(query, (history_id,))
        
        logger.info(f"Deleted LM values for history_id={history_id}")
        return result if result else 0

    async def get_metric_statistics(
        self,
        metric_code: str,
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, Any]:
        """
        Получает статистику по конкретной метрике за период.
        
        Args:
            metric_code: Код метрики
            start_date: Начало периода
            end_date: Конец периода
            
        Returns:
            Словарь со статистикой (count, avg, min, max, stddev, percentiles)
        """
        query = """
        SELECT 
            COUNT(*) as count,
            AVG(value_numeric) as avg_value,
            MIN(value_numeric) as min_value,
            MAX(value_numeric) as max_value,
            STDDEV(value_numeric) as stddev_value
        FROM lm_value
        WHERE metric_code = %s
        AND created_at BETWEEN %s AND %s
        AND value_numeric IS NOT NULL
        """
        
        stats = await self.db_manager.execute_with_retry(
            query,
            (metric_code, start_date, end_date),
            fetchone=True
        )
        
        return stats or {}

    async def get_followup_calls(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Получает звонки, помеченные как «Нужно перезвонить».
        
        Returns:
            Список звонков с флагом followup_needed=true
        """
        from datetime import datetime, timedelta
        
        start_date = datetime.now() - timedelta(days=7)
        
        query = """
            SELECT DISTINCT
                lv.history_id,
                lv.created_at,
                (SELECT value_label FROM lm_value 
                 WHERE history_id = lv.history_id 
                 AND metric_code = 'churn_risk_level' 
                 LIMIT 1) as churn_risk_level
            FROM lm_value lv
            WHERE lv.metric_code = 'followup_needed_flag'
            AND lv.value_label = 'true'
            AND lv.created_at >= %s
            ORDER BY lv.created_at DESC
            LIMIT %s
        """
        
        rows = await self.db_manager.execute_with_retry(
            query,
            (start_date, limit),
            fetchall=True
        ) or []
        
        return [dict(row) for row in rows]

    async def get_calc_watermark(self, lm_version: str, calc_profile: str) -> Dict[str, Any]:
        """Получает watermark для инкрементального расчета."""
        query = """
            SELECT last_score_date, last_id 
            FROM lm_calc_state 
            WHERE lm_version = %s AND calc_profile = %s
        """
        row = await self.db_manager.execute_with_retry(
            query, (lm_version, calc_profile), fetchone=True
        )
        if row:
            return row
        return {"last_score_date": None, "last_id": 0}

    async def update_calc_watermark(
        self, lm_version: str, calc_profile: str, last_date: datetime, last_id: int
    ) -> None:
        """Обновляет watermark для инкрементального расчета."""
        query = """
            INSERT INTO lm_calc_state (lm_version, calc_profile, last_score_date, last_id)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                last_score_date = VALUES(last_score_date),
                last_id = VALUES(last_id),
                updated_at = CURRENT_TIMESTAMP
        """
        await self.db_manager.execute_with_retry(
            query, (lm_version, calc_profile, last_date, last_id), commit=True
        )
    async def get_call_info(self, history_id: int) -> Optional[Dict[str, Any]]:
        """Получает базовую информацию о звонке (номер, дата)."""
        query = "SELECT history_id, caller_number, context_start_time_dt FROM call_history WHERE history_id = %s"
        return await self.db_manager.execute_with_retry(query, (history_id,), fetchone=True)

    async def get_call_records_for_lm(self, history_id: int) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """Возвращает записи call_history и call_scores для расчета LM."""
        history_query = """
            SELECT history_id, talk_duration, await_sec, context_start_time_dt AS call_date
            FROM call_history
            WHERE history_id = %s
            LIMIT 1
        """
        score_query = """
            SELECT *
            FROM call_scores
            WHERE history_id = %s
            LIMIT 1
        """
        history = await self.db_manager.execute_with_retry(history_query, (history_id,), fetchone=True)
        score = await self.db_manager.execute_with_retry(score_query, (history_id,), fetchone=True)
        return history, score

    async def get_action_list(
        self,
        action_type: str,
        limit: int = 10,
        offset: int = 0,
        *,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """Получает список звонков для действий (followup, complaints, churn, lost)."""
        base_query = """
            SELECT DISTINCT
                lv.history_id,
                lv.created_at,
                lv.value_numeric,
                lv.value_label,
                lv.value_json,
                cs.call_category,
                cs.call_score,
                cs.caller_number,
                cs.called_info,
                cs.utm_source_by_number,
                cs.call_date,
                cs.result AS operator_result,
                cs.refusal_reason,
                cs.outcome
            FROM lm_value lv
            LEFT JOIN call_scores cs ON cs.history_id = lv.history_id
            LEFT JOIN call_history ch ON ch.history_id = lv.history_id
        """
        where_clause = ""
        params: List[Any] = []

        if action_type == "followup":
            where_clause = "lv.metric_code = 'followup_needed_flag' AND lv.value_label = 'true'"
        elif action_type == "complaints":
            threshold = LM_METRIC_THRESHOLDS.get("complaint_risk_flag", 60.0)
            where_clause = (
                "lv.metric_code = 'complaint_risk_flag' AND "
                "(lv.value_numeric >= %s OR JSON_EXTRACT(lv.value_json, '$.combo_flag') = true)"
            )
            params.append(threshold)
        elif action_type == "churn":
            where_clause = "lv.metric_code = 'churn_risk_level' AND lv.value_label IN ('CRITICAL', 'HIGH')"
        elif action_type == "lost":
            threshold = LM_METRIC_THRESHOLDS.get("lost_opportunity_score", 60.0)
            where_clause = "lv.metric_code = 'lost_opportunity_score' AND lv.value_numeric >= %s"
            params.append(threshold)
        else:
            return []

        date_filter = ""
        if start_date and end_date:
            date_filter = (
                " AND COALESCE(ch.context_start_time_dt, cs.call_date, lv.created_at) >= %s"
                " AND COALESCE(ch.context_start_time_dt, cs.call_date, lv.created_at) < %s"
            )
            params.extend([start_date, end_date])

        query = f"""
            {base_query}
            WHERE {where_clause}{date_filter}
              AND COALESCE(ch.talk_duration, cs.talk_duration, 0) >= 10
            ORDER BY COALESCE(ch.context_start_time_dt, cs.call_date, lv.created_at) DESC
            LIMIT %s OFFSET %s
        """
        params.extend([limit, offset])
        rows = await self.db_manager.execute_with_retry(query, tuple(params), fetchall=True) or []
        result: List[Dict[str, Any]] = []
        for row in rows:
            row_dict = dict(row)
            value_json = row_dict.get('value_json')
            if value_json:
                try:
                    row_dict['value_json'] = json.loads(value_json)
                except (TypeError, json.JSONDecodeError):
                    row_dict['value_json'] = value_json
            result.append(row_dict)
        return result

    async def get_action_count(
        self,
        action_type: str,
        *,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> int:
        """Получает общее количество элементов в списке действий."""
        params: List[Any] = []
        if action_type == "followup":
            conditions = "lv.metric_code = 'followup_needed_flag' AND lv.value_label = 'true'"
        elif action_type == "complaints":
            threshold = LM_METRIC_THRESHOLDS.get("complaint_risk_flag", 60.0)
            conditions = (
                "lv.metric_code = 'complaint_risk_flag' "
                "AND (lv.value_numeric >= %s OR JSON_EXTRACT(lv.value_json, '$.combo_flag') = true)"
            )
            params.append(threshold)
        elif action_type == "churn":
            conditions = "lv.metric_code = 'churn_risk_level' AND lv.value_label IN ('CRITICAL', 'HIGH')"
        elif action_type == "lost":
            threshold = LM_METRIC_THRESHOLDS.get("lost_opportunity_score", 60.0)
            conditions = "lv.metric_code = 'lost_opportunity_score' AND lv.value_numeric >= %s"
            params.append(threshold)
        else:
            return 0

        if start_date and end_date:
            conditions += (
                " AND COALESCE(ch.context_start_time_dt, cs.call_date, lv.created_at) >= %s"
                " AND COALESCE(ch.context_start_time_dt, cs.call_date, lv.created_at) < %s"
            )
            params.extend([start_date, end_date])

        query = f"""
            SELECT COUNT(DISTINCT lv.history_id) AS total
            FROM lm_value lv
            LEFT JOIN call_scores cs ON cs.history_id = lv.history_id
            LEFT JOIN call_history ch ON ch.history_id = lv.history_id
            WHERE {conditions}
              AND COALESCE(ch.talk_duration, cs.talk_duration, 0) >= 10
        """

        row = await self.db_manager.execute_with_retry(query, tuple(params) if params else None, fetchone=True)
        return int((row or {}).get('total') or 0)
