# Файл: app/db/repositories/operators.py

"""
Репозиторий для работы с операторами.
"""

from typing import Any, Dict, List, Optional, Set
from datetime import datetime
import json

from app.db.manager import DatabaseManager
from app.db.models import OperatorRecord, CallMetrics
from app.logging_config import get_watchdog_logger
from app.core.roles import ROLE_NAME_TO_ID
from app.config import DEV_ADMIN_ID, DEV_ADMIN_USERNAME
from app.core.roles import ROLE_NAME_TO_ID

logger = get_watchdog_logger(__name__)


class OperatorRepository:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    # === Методы из bot/repositories/operators.py ===
    
    async def get_extension_by_user_id(self, user_id: int) -> Optional[str]:
        """
        Получает extension оператора по user_id.
        
        Сначала ищем в таблице users (основной справочник операторов),
        затем, для обратной совместимости, проверяем UsersTelegaBot.
        """
        user_query = """
            SELECT extension
            FROM users
            WHERE user_id = %s
            LIMIT 1
        """
        row = await self.db_manager.execute_with_retry(
            user_query,
            (user_id,),
            fetchone=True,
        )
        extension = row.get("extension") if row else None
        if extension:
            return extension

        legacy_query = """
            SELECT extension
            FROM UsersTelegaBot
            WHERE user_id = %s
            LIMIT 1
        """
        legacy_row = await self.db_manager.execute_with_retry(
            legacy_query,
            (user_id,),
            fetchone=True,
        )
        return legacy_row.get("extension") if legacy_row else None

    async def get_name_by_extension(self, extension: str) -> str:
        query = "SELECT full_name FROM users WHERE extension = %s"
        rows = await self.db_manager.execute_with_retry(query, (extension,), fetchall=True)
        if rows and rows[0].get('full_name'):
            return rows[0]['full_name']

        legacy_query = """
            SELECT
                COALESCE(operator_name, full_name) AS name
            FROM UsersTelegaBot
            WHERE extension = %s
            LIMIT 1
        """
        legacy = await self.db_manager.execute_with_retry(
            legacy_query,
            (extension,),
            fetchone=True,
        )
        if legacy and legacy.get("name"):
            return legacy["name"]
        logger.warning(
            "[OPERATORS] Не удалось найти имя оператора по extension=%s",
            extension,
        )
        return 'Неизвестно'

    async def get_call_data(
        self,
        extension: str,
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Получает данные звонков (history и scores) для оператора.
        """
        start_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
        end_str = end_date.strftime('%Y-%m-%d %H:%M:%S')

        # Call History
        history_query = """
        SELECT 
            ch.history_id, 
            ch.called_info, 
            ch.context_start_time,
            COALESCE(ch.context_start_time_dt, FROM_UNIXTIME(ch.context_start_time)) AS context_start_time_dt, 
            ch.talk_duration,
            ch.answered_extension
        FROM mangoapi_db.call_history ch
        WHERE 
            ch.answered_extension = %s
            AND COALESCE(ch.context_start_time_dt, FROM_UNIXTIME(ch.context_start_time)) BETWEEN %s AND %s
        """
        history_rows = await self.db_manager.execute_with_retry(
            history_query, 
            (extension, start_str, end_str), 
            fetchall=True
        ) or []

        # Call Scores
        scores_query = """
        SELECT 
            cs.history_id, 
            cs.called_info, 
            cs.call_date, 
            cs.talk_duration, 
            cs.call_category, 
            cs.call_score, 
            cs.result
        FROM mangoapi_db.call_scores cs
        INNER JOIN mangoapi_db.call_history ch ON ch.history_id = cs.history_id
        WHERE 
            ch.answered_extension = %s
            AND cs.call_date BETWEEN %s AND %s
        """
        scores_rows = await self.db_manager.execute_with_retry(
            scores_query, 
            (extension, start_str, end_str), 
            fetchall=True
        ) or []

        # Processing
        history_ids_scores = {row['history_id'] for row in scores_rows}
        accepted = [row for row in history_rows if row['history_id'] in history_ids_scores]
        missed = [row for row in history_rows if row['history_id'] not in history_ids_scores]

        return {
            'call_history': history_rows,
            'call_scores': scores_rows,
            'accepted_calls': accepted,
            'missed_calls': missed
        }

    async def get_call_scores(
        self,
        extension: str,
        start_date: datetime,
        end_date: datetime,
    ) -> List[Dict[str, Any]]:
        """
        Получает данные звонков только из call_scores по extension.
        Используем поля call_scores без call_history.
        """
        start_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
        end_str = end_date.strftime('%Y-%m-%d %H:%M:%S')

        base_select = [
            "cs.id",
            "cs.history_id",
            "cs.called_info",
            "cs.caller_info",
            "cs.score_date",
            "cs.call_date",
            "cs.talk_duration",
            "cs.call_category",
            "cs.call_score",
            "cs.outcome",
            "cs.refusal_reason",
            "cs.refusal_group",
            "cs.result",
            "cs.transcript",
            "cs.requested_service_name",
            "cs.requested_doctor_name",
            "cs.requested_doctor_speciality",
            "cs.is_target",
        ]
        optional_columns = [
            "objection_present",
            "objection_handled",
            "booking_attempted",
            "next_step_clear",
            "followup_captured",
        ]

        columns = await self._get_call_scores_columns()
        if columns is None:
            scores_query = self._build_call_scores_query(
                base_select,
                optional_columns,
                available_columns=None,
            )
            try:
                rows = await self.db_manager.execute_with_retry(
                    scores_query,
                    (extension, extension, start_str, end_str),
                    fetchall=True,
                )
                return rows or []
            except Exception as exc:
                if "Unknown column" not in str(exc):
                    raise
                logger.warning(
                    "[OPERATORS] Нет части колонок call_scores, выгружаем без них: %s",
                    exc,
                    exc_info=True,
                )
                logger.warning(
                    "[OPERATORS] В call_scores отсутствуют колонки: %s",
                    ", ".join(optional_columns),
                )
                columns = set()
        else:
            missing = [name for name in optional_columns if name not in columns]
            if missing:
                logger.warning(
                    "[OPERATORS] В call_scores отсутствуют колонки: %s",
                    ", ".join(missing),
                )

        scores_query = self._build_call_scores_query(
            base_select,
            optional_columns,
            available_columns=columns,
        )
        logger.debug("[OPERATORS] Built scores_query (len=%d): %s", len(scores_query), scores_query)

        rows = await self.db_manager.execute_with_retry(
            scores_query,
            (extension, extension, start_str, end_str),
            fetchall=True,
        )
        return rows or []

    async def _get_call_scores_columns(self) -> Optional[Set[str]]:
        query = """
            SELECT COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = 'call_scores'
        """
        try:
            rows = await self.db_manager.execute_with_retry(
                query,
                fetchall=True,
            )
        except Exception as exc:
            logger.warning(
                "[OPERATORS] Не удалось получить список колонок call_scores: %s",
                exc,
                exc_info=True,
            )
            return None
        columns: Set[str] = set()
        for row in rows or []:
            name = row.get("COLUMN_NAME") if isinstance(row, dict) else (row[0] if row else None)
            if name:
                columns.add(str(name))
        return columns

    @staticmethod
    def _build_call_scores_query(
        base_select: List[str],
        optional_columns: List[str],
        *,
        available_columns: Optional[Set[str]],
    ) -> str:
        select_parts = list(base_select)
        if available_columns is None:
            select_parts.extend([f"cs.{name}" for name in optional_columns])
        else:
            for name in optional_columns:
                if name in available_columns:
                    select_parts.append(f"cs.{name}")
                else:
                    select_parts.append(f"NULL AS {name}")
        select_clause = ",\n            ".join(select_parts)
        return f"""
        SELECT
            {select_clause}
        FROM mangoapi_db.call_scores cs
        WHERE
            cs.is_target = 1
            AND (cs.called_info = %s OR cs.caller_info = %s)
            AND cs.score_date BETWEEN %s AND %s
        """

    async def get_quality_summary(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, Any]:
        """
        Получает агрегированные метрики качества за период.
        """
        start_ts = int(start_date.timestamp())
        end_ts = int(end_date.timestamp())
        start_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
        end_str = end_date.strftime('%Y-%m-%d %H:%M:%S')

        history_query = """
            SELECT
                COUNT(*) AS total_calls,
                SUM(
                    CASE
                        WHEN COALESCE(ch.talk_duration, 0) = 0 THEN 1
                        ELSE 0
                    END
                ) AS missed_calls
            FROM call_history ch
            WHERE ch.context_start_time BETWEEN %s AND %s
        """
        log_extra = {
            "feature": "weekly_quality_report",
            "date_range_start": start_str,
            "date_range_end": end_str,
        }
        logger.info(
            f"weekly_quality_report: подсчёт звонков (history) за период {start_str} — {end_str}",
            extra=log_extra,
        )

        try:
            history_stats = await self.db_manager.execute_with_retry(
                history_query,
                (start_ts, end_ts),
                fetchone=True,
            ) or {}
        except Exception:
            logger.exception(
                "weekly_quality_report: ошибка выборки call_history",
                extra=log_extra,
            )
            raise

        # ИСПРАВЛЕНИЕ: Используем поля outcome и refusal_reason вместо текстовых категорий
        # Метрики из call_scores
        scores_query = """
            SELECT
                COUNT(*) AS total_scored_calls,
                AVG(cs.call_score) AS avg_score,
                SUM(CASE WHEN cs.outcome = 'lead_no_record' THEN 1 ELSE 0 END) AS leads_no_record,
                SUM(CASE WHEN cs.outcome = 'record' THEN 1 ELSE 0 END) AS booked_leads,
                SUM(CASE WHEN cs.outcome IN ('record', 'lead_no_record') THEN 1 ELSE 0 END) AS total_leads,
                SUM(
                    CASE 
                        WHEN cs.outcome = 'cancel' OR cs.refusal_reason IS NOT NULL 
                        THEN 1 ELSE 0 
                    END
                ) AS cancellations
            FROM call_scores cs
            WHERE cs.call_date BETWEEN %s AND %s
              AND cs.is_target = 1
        """
        logger.info(
            f"weekly_quality_report: подсчёт скорингов (scores) за период {start_str} — {end_str}",
            extra=log_extra,
        )

        try:
            scores_stats = await self.db_manager.execute_with_retry(
                scores_query,
                (start_str, end_str),
                fetchone=True,
            ) or {}
        except Exception:
            logger.exception(
                "weekly_quality_report: ошибка выборки call_scores",
                extra=log_extra,
            )
            raise

        return {
            "total_calls": history_stats.get("total_calls") or 0,
            "missed_calls": history_stats.get("missed_calls") or 0,
            "avg_score": scores_stats.get("avg_score") or 0.0,
            "total_leads": scores_stats.get("total_leads") or 0,
            "leads_no_record": scores_stats.get("leads_no_record") or 0,
            "booked_leads": scores_stats.get("booked_leads") or 0,
            "cancellations": scores_stats.get("cancellations") or 0,
        }

    async def save_report(
        self,
        user_id: int,
        name: str,
        report_text: str,
        period: str,
        start_date: datetime,
        end_date: datetime,
        metrics: Dict[str, Any],
        recommendations: str
    ):
        query = """
        INSERT INTO reports (
            user_id, operator_name, report_text, period, 
            start_date, end_date, metrics_json, recommendations, created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """
        metrics_str = json.dumps(metrics, default=str)

        await self.db_manager.execute_with_retry(
            query,
            (user_id, name, report_text, period, start_date, end_date, metrics_str, recommendations)
        )

    def _is_dev_account(self, record: Dict[str, Any]) -> bool:
        """Проверяет, относится ли запись к dev/admin аккаунту, которого нужно скрыть."""
        telegram_id = record.get("telegram_id")
        if DEV_ADMIN_ID and telegram_id and str(telegram_id) == str(DEV_ADMIN_ID):
            return True
        return False

    async def get_approved_operators(self, include_pending: bool = True) -> List[Dict[str, Any]]:
        """
        Возвращает список операторов для генерации отчётов.

        Args:
            include_pending: включать ли пользователей со статусом pending.
        """
        # The 'statuses' and 'placeholders' logic is no longer needed as 'status' and 'role_id' are removed from WHERE clause.
        # The instruction implies removing these fields and filtering by extension IS NOT NULL instead.
        query = f"""
            SELECT
                user_id,
                NULLIF(full_name, '') AS full_name,
                NULLIF(name, '') AS name,
                extension
            FROM users
            WHERE extension IS NOT NULL
            ORDER BY COALESCE(NULLIF(full_name, ''), NULLIF(name, ''), CAST(user_id AS CHAR))
        """
        # The 'params' should be empty as there are no dynamic parameters in the new WHERE clause.
        params = () 
        rows = await self.db_manager.execute_with_retry(
            query,
            params=params,
            fetchall=True,
        ) or []

        result: List[Dict[str, Any]] = []
        for record in rows:
            if not record.get("user_id"):
                continue
            if self._is_dev_account(record):
                continue
            result.append(record)
        return result

    async def get_operator_info_by_user_id(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Получает информацию об операторе по user_id."""
        query = """
            SELECT
                user_id,
                NULLIF(full_name, '') AS full_name,
                NULLIF(name, '') AS name,
                extension
            FROM users
            WHERE user_id = %s
            LIMIT 1
        """
        return await self.db_manager.execute_with_retry(
            query,
            params=(user_id,),
            fetchone=True,
        )

    # === Методы из db_module.py ===

    async def find_operator_by_id(self, user_id: int) -> Optional[OperatorRecord]:
        """
        Поиск оператора по его ID.
        """
        query = "SELECT * FROM users WHERE user_id = %s"
        result = await self.db_manager.execute_query(query, (user_id,), fetchone=True)
        if not result:
            logger.warning(f"Оператор с ID {user_id} не найден.")
        return result

    async def find_operator_by_extension(self, extension: str) -> Optional[OperatorRecord]:
        """Поиск оператора по его extension в таблице users."""
        query = "SELECT * FROM users WHERE extension = %s"
        result = await self.db_manager.execute_query(query, (extension,), fetchone=True)
        if not result:
            logger.warning(f"Оператор с extension {extension} не найден.")
        return result

    async def find_operator_by_name(self, operator_name: str) -> Optional[OperatorRecord]:
        """Поиск оператора по имени в таблице users."""
        query = "SELECT * FROM users WHERE full_name = %s"
        result = await self.db_manager.execute_query(query, (operator_name,), fetchone=True)
        if not result:
            logger.warning(f"Оператор с именем {operator_name} не найден.")
        return result

    async def get_operator_extension(self, user_id: int) -> Optional[str]:
        """
        Получение extension по user_id из таблицы users.
        """
        query = "SELECT extension FROM users WHERE user_id = %s"
        result = await self.db_manager.execute_query(query, (user_id,), fetchone=True)
        if result and 'extension' in result:
            extension = result['extension']
            logger.info(f"Найден extension {extension} для user_id {user_id}")
            return extension
        else:
            logger.warning(f"Extension не найден для user_id {user_id}")
            return None

    async def operator_exists(self, extension: str) -> bool:
        """Проверка существования оператора по extension."""
        query = "SELECT 1 FROM users WHERE extension = %s"
        result = await self.db_manager.execute_query(query, (extension,), fetchone=True)
        return bool(result)

    async def get_operator_calls(self, extension: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Получение звонков оператора за указанный период."""
        if not await self.operator_exists(extension):
            logger.warning(f"Оператор с extension {extension} не найден.")
            return []

        query = """
        SELECT u.*, cs.call_date, cs.call_score, cs.result, cs.talk_duration
        FROM users u
        JOIN call_scores cs 
            ON SUBSTRING_INDEX(cs.called_info, ' ', 1) = u.extension
        WHERE u.extension = %s
        """
        params = [extension]
        if start_date and end_date:
            query += " AND cs.call_date BETWEEN %s AND %s"
            params.extend([start_date, end_date])
            
        result = await self.db_manager.execute_query(query, params, fetchall=True)
        if not result or not isinstance(result, list):
            logger.warning(f"Звонки оператора с extension {extension} за период не найдены.")
            return []
        
        return result

    async def get_operator_call_metrics(self, extension: str, start_date: Optional[str] = None, 
                                       end_date: Optional[str] = None) -> Optional[CallMetrics]:
        """Получение метрик звонков оператора за определенный период."""
        # Нормализация extension
        extension = ''.join(c for c in str(extension) if c.isalnum())
        
        query = """
        SELECT COUNT(*) as total_calls, 
               AVG(talk_duration) as avg_talk_time,
               SUM(CASE WHEN result = 'success' THEN 1 ELSE 0 END) as successful_calls
        FROM call_scores
        WHERE called_info LIKE %s
        """
        params = [f"%{extension}%"]

        if start_date:
            query += " AND call_date >= %s"
            params.append(start_date)
        if end_date:
            query += " AND call_date <= %s"
            params.append(end_date)

        result = await self.db_manager.execute_query(query, params, fetchone=True)
        if not result:
            logger.warning(f"Метрики звонков для оператора с extension {extension} за период не найдены.")
        return result

    # ========================================================================
    # Новые методы для ML Analytics
    # ========================================================================

    async def find_operator_by_name_fuzzy(self, operator_name: str) -> List[Dict[str, Any]]:
        """
        Нечеткий поиск оператора по имени (LIKE поиск).
        Используется при регистрации для сопоставления Telegram пользователя с оператором.
        
        Returns:
            Список операторов, имена которых частично совпадают
        """
        query = """
        SELECT id, name, extension, user_id
        FROM users
        WHERE name LIKE %s OR extension LIKE %s
        ORDER BY name
        """
        search_pattern = f"%{operator_name}%"
        
        result = await self.db_manager.execute_query(
            query,
            (search_pattern, search_pattern),
            fetchall=True
        )
        
        return result or []

    async def get_operator_by_extension_from_history(self, extension: str) -> Optional[Dict[str, Any]]:
        """
        Поиск оператора по extension в таблице users.
        Возвращает данные для связывания с Telegram пользователем.
        """
        query = """
        SELECT id, name as full_name, extension, user_id
        FROM users
        WHERE extension = %s
        LIMIT 1
        """
        
        result = await self.db_manager.execute_query(
            query,
            (extension,),
            fetchone=True
        )
        
        return result

    def parse_operator_from_call_info(self, call_info: str) -> Optional[str]:
        """
        Парсинг имени/extension оператора из поля called_info/caller_info.
        
        Формат called_info обычно: "extension_number Name" или "Name <extension>"
        
        Returns:
            extension или имя оператора
        """
        if not call_info:
            return None
        
        # Попробуем извлечь extension (обычно это числа в начале или в скобках)
        import re
        
        # Паттерн 1: "1234 Иванова И.И."
        match = re.match(r'^(\d+)\s+', call_info)
        if match:
            return match.group(1)
        
        # Паттерн 2: "Иванова И.И. <1234>"
        match = re.search(r'<(\d+)>', call_info)
        if match:
            return match.group(1)
        
        # Если extension не найден, возвращаем само имя
        # Может быть полезно для дальнейшего fuzzy поиска
        return call_info.strip()

    async def get_all_operator_names(self) -> List[str]:
        """Получить список всех имен операторов для отображения при регистрации."""
        query = """
        SELECT DISTINCT name
        FROM users
        WHERE name IS NOT NULL AND name != ''
        ORDER BY name
        """
        
        result = await self.db_manager.execute_query(query, fetchall=True)
        
        if not result:
            return []
        
        return [row.get('name') for row in result if row.get('name')]
