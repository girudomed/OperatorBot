"""
Репозиторий для работы с операторами.
"""

from typing import Any, Dict, List, Optional
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
        query = "SELECT extension FROM users WHERE user_id = %s"
        rows = await self.db_manager.execute_with_retry(query, (user_id,), fetchall=True)
        if not rows:
            return None
        return rows[0].get('extension')

    async def get_name_by_extension(self, extension: str) -> str:
        query = "SELECT full_name FROM users WHERE extension = %s"
        rows = await self.db_manager.execute_with_retry(query, (extension,), fetchall=True)
        if rows and rows[0].get('full_name'):
            return rows[0]['full_name']
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
        start_ts = int(start_date.timestamp())
        end_ts = int(end_date.timestamp())
        start_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
        end_str = end_date.strftime('%Y-%m-%d %H:%M:%S')

        # Call History
        history_query = """
        SELECT history_id, called_info, context_start_time, talk_duration
        FROM call_history
        WHERE 
            called_info LIKE CONCAT(%s, '%%')
            AND context_start_time BETWEEN %s AND %s
        """
        history_rows = await self.db_manager.execute_with_retry(
            history_query, 
            (extension, start_ts, end_ts), 
            fetchall=True
        ) or []

        # Call Scores
        scores_query = """
        SELECT history_id, called_info, call_date, talk_duration, call_category, call_score, result
        FROM call_scores
        WHERE 
            called_info LIKE CONCAT(%s, '%%')
            AND call_date BETWEEN %s AND %s
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
                SUM(CASE WHEN cs.history_id IS NULL THEN 1 ELSE 0 END) AS missed_calls
            FROM call_history ch
            LEFT JOIN call_scores cs ON cs.history_id = ch.history_id
            WHERE ch.context_start_time BETWEEN %s AND %s
        """
        log_extra = {
            "feature": "weekly_quality_report",
            "date_range_start": start_str,
            "date_range_end": end_str,
        }
        logger.info(
            "weekly_quality_report: подсчёт звонков (history) за период %s — %s",
            start_str,
            end_str,
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
        lead_pattern = '%Лид%'  # Паттерн выносим в переменную
        scores_query = """
            SELECT
                COUNT(*) AS total_scored_calls,
                AVG(cs.call_score) AS avg_score,
                SUM(CASE 
                    WHEN cs.outcome = 'lead_no_record' OR cs.call_category LIKE %s 
                    THEN 1 ELSE 0 
                END) AS total_leads,
                SUM(CASE WHEN cs.outcome = 'record' THEN 1 ELSE 0 END) AS booked_leads,
                SUM(CASE 
                    WHEN cs.outcome = 'cancel' OR cs.refusal_reason IS NOT NULL 
                    THEN 1 ELSE 0 
                END) AS cancellations
            FROM call_scores cs
            WHERE cs.call_date BETWEEN %s AND %s
        """
        logger.info(
            "weekly_quality_report: подсчёт скорингов (scores) за период %s — %s",
            start_str,
            end_str,
            extra=log_extra,
        )

        try:
            scores_stats = await self.db_manager.execute_with_retry(
                scores_query,
                (lead_pattern, start_str, end_str),
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
            SELECT user_id,
                   full_name,
                   extension
            FROM users
            WHERE extension IS NOT NULL
            ORDER BY COALESCE(full_name, 'Без имени')
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
            SELECT user_id, full_name, extension
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
