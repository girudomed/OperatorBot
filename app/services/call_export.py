# Файл: app/services/call_export.py

"""Сервис формирования XLSX с расшифровками звонков."""

from __future__ import annotations

from datetime import datetime, timedelta
from io import BytesIO
from typing import Any, Dict, Iterable, List, Tuple
from zoneinfo import ZoneInfo

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font
from openpyxl.utils import get_column_letter

from app.db.manager import DatabaseManager
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)

MOSCOW_TZ = ZoneInfo("Europe/Moscow")

EXPORT_PERIOD_OPTIONS = (14, 30, 60, 180)

HEADERS = (
    "Дата",
    "Время",
    "Оператор",
    "Врач",
    "Услуга",
    "Номер, с которого звонили",
    "Номер, на который звонили",
    "Тип звонка",
    "Вход/исход",
    "Длительность разговора (в секундах)",
    "Целевой/нецелевой (0/1)",
    "Расшифровка",
    "Анализ Звонка",
    "Цель звонка",
    "Специальность врача",
    "Исход звонка",
    "Причина отказа",
    "Категория причины",
    "Причины отказа",
    "Группа отказа",
    "Источник",
    "Оценка качества (0–10)",
    "Дата/время оценки",
)

CATEGORY_LABELS = {
    "запись на услугу": "запись",
    "лид (без записи)": "лид (без записи)",
    "отмена записи": "отмена",
    "перенос записи": "перенос",
    "напоминание о приеме": "подтверждение",
    "жалоба": "жалоба",
    "навигация": "вход/адрес",
    "информация о состоянии пациента": "результаты/анализы",
    "спам": "спам",
    "спам, реклама": "спам, реклама",
    "технический звонок": "технический",
}

OUTCOME_LABELS = {
    "record": "запись",
    "lead_no_record": "лид (без записи)",
    "info_only": "справка",
    "non_target": "нецелевой",
}

KEYWORD_LABELS: Tuple[Tuple[str, str], ...] = (
    ("времен", "уточнение времени"),
    ("анализ", "результаты/анализы"),
    ("результат", "результаты/анализы"),
    ("перенос", "перенос"),
    ("отмен", "отмена"),
    ("подтвержд", "подтверждение"),
    ("жалоб", "жалоба"),
    ("адрес", "вход/адрес"),
)


class CallExportService:
    """Генератор выгрузок звонков в формате XLSX."""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    async def build_export(self, days: int) -> Tuple[BytesIO, str, int, Tuple[datetime, datetime]]:
        """
        Строит XLSX-файл с расшифровками за указанное количество дней.

        Возвращает bytes-поток, имя файла, количество строк и границы периода.
        """

        if days not in EXPORT_PERIOD_OPTIONS:
            raise ValueError(f"Unsupported export period: {days}")

        period_start, period_end = self._resolve_period(days)
        rows = await self._fetch_calls(period_start, period_end)
        logger.info(
            "[CALL_EXPORT] Building workbook: %s rows for %s-%s",
            len(rows),
            period_start,
            period_end,
        )
        workbook = self._build_workbook(rows)
        filename = f"Выгрузка_звонков_{period_start:%Y%m%d}_{period_end:%Y%m%d}.xlsx"
        return workbook, filename, len(rows), (period_start, period_end)

    async def _fetch_calls(
        self,
        start: datetime,
        end: datetime,
    ) -> List[Dict[str, Any]]:
        query = """
            SELECT
                cs.history_id,
                cs.call_date,
                cs.called_info,
                cs.requested_doctor_name,
                cs.requested_service_name,
                cs.caller_number,
                cs.called_number,
                cs.call_type,
                cs.context_type,
                cs.talk_duration,
                cs.is_target,
                cs.transcript,
                cs.result,
                cs.call_category,
                cs.requested_doctor_speciality,
                cs.outcome,
                cs.refusal_reason,
                rc.label AS refusal_category_label,
                cs.refusal_category_code,
                cs.refusal_group,
                cs.utm_source_by_number,
                cs.call_score,
                cs.score_date
            FROM call_scores cs
            LEFT JOIN refusal_categories rc ON rc.id = cs.refusal_category_id
            WHERE cs.call_date BETWEEN %s AND %s
            ORDER BY cs.call_date ASC
        """
        result = await self.db_manager.execute_with_retry(
            query,
            params=(start, end),
            fetchall=True,
            query_name="call_export.fetch_calls",
        )
        return [dict(row) for row in (result or [])]

    def _resolve_period(self, days: int) -> Tuple[datetime, datetime]:
        now = datetime.now(MOSCOW_TZ)
        start_date = (now - timedelta(days=max(days - 1, 0))).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
        end_date = now.replace(hour=23, minute=59, second=59, microsecond=999999)
        return start_date.replace(tzinfo=None), end_date.replace(tzinfo=None)

    def _build_workbook(self, rows: Iterable[Dict[str, Any]]) -> BytesIO:
        wb = Workbook()
        ws = wb.active
        ws.title = "Звонки"

        ws.append(HEADERS)
        header_font = Font(bold=True)
        for col_idx in range(1, len(HEADERS) + 1):
            ws.cell(row=1, column=col_idx).font = header_font

        wrap_columns = {12, 13, 14}
        row_index = 2
        for row in rows:
            call_date = row.get("call_date")
            if not isinstance(call_date, datetime):
                continue
            score_date = row.get("score_date")
            goal = self._resolve_goal(row)
            values = [
                call_date.date(),
                call_date.time(),
                self._normalize_label(row.get("called_info")),
                self._normalize_label(row.get("requested_doctor_name")),
                self._normalize_label(row.get("requested_service_name")),
                self._normalize_label(row.get("caller_number")),
                self._normalize_label(row.get("called_number")),
                self._normalize_label(row.get("call_type")),
                self._normalize_label(row.get("context_type")),
                row.get("talk_duration"),
                row.get("is_target"),
                (row.get("transcript") or "").strip(),
                (row.get("result") or "").strip(),
                goal,
                self._normalize_label(row.get("requested_doctor_speciality")),
                self._normalize_label(row.get("outcome")),
                self._normalize_label(row.get("refusal_reason")),
                self._normalize_label(row.get("refusal_category_label")),
                self._normalize_label(row.get("refusal_category_code")),
                self._normalize_label(row.get("refusal_group")),
                self._normalize_label(row.get("utm_source_by_number")) or "не указано",
                row.get("call_score"),
                score_date,
            ]
            for idx, value in enumerate(values, start=1):
                cell = ws.cell(row=row_index, column=idx, value=value)
                if idx == 1 and isinstance(value, datetime):
                    cell.number_format = "DD.MM.YYYY"
                elif idx == 1:
                    cell.number_format = "DD.MM.YYYY"
                if idx == 2 and isinstance(value, datetime):
                    cell.number_format = "HH:MM"
                elif idx == 2:
                    cell.number_format = "HH:MM"
                if idx == len(values):
                    cell.number_format = "DD.MM.YYYY HH:MM"
                if idx in wrap_columns:
                    cell.alignment = Alignment(wrap_text=True, vertical="top")
            row_index += 1

        column_widths = {
            1: 12,
            2: 10,
            3: 24,
            4: 24,
            5: 26,
            6: 24,
            7: 24,
            8: 18,
            9: 14,
            10: 18,
            11: 20,
            12: 80,
            13: 60,
            14: 26,
            15: 26,
            16: 18,
            17: 24,
            18: 24,
            19: 18,
            20: 20,
            21: 24,
            22: 20,
            23: 24,
        }
        for column, width in column_widths.items():
            ws.column_dimensions[get_column_letter(column)].width = width

        buffer = BytesIO()
        wb.save(buffer)
        buffer.seek(0)
        return buffer

    def _resolve_goal(self, row: Dict[str, Any]) -> str:
        category = self._normalize_label(row.get("call_category"))
        if category:
            normalized = self._map_category(category)
            if normalized:
                return normalized

        refusal = self._normalize_label(row.get("refusal_reason"))
        if refusal:
            keyword = self._match_keyword(refusal)
            if keyword:
                return keyword

        service = self._normalize_label(row.get("requested_service_name"))
        if service:
            keyword = self._match_keyword(service)
            if keyword:
                return keyword

        outcome = (row.get("outcome") or "").strip().lower()
        if outcome in OUTCOME_LABELS:
            return OUTCOME_LABELS[outcome]

        if row.get("is_target"):
            return "лид (без записи)"

        return "не указано"

    @staticmethod
    def _normalize_label(value: Any) -> str:
        if not value:
            return ""
        text = str(value).strip()
        return text

    def _map_category(self, category: str) -> str:
        normalized = category.lower()
        for key, label in CATEGORY_LABELS.items():
            if key in normalized:
                return label
        keyword = self._match_keyword(category)
        if keyword:
            return keyword
        return category

    @staticmethod
    def _match_keyword(text: str) -> str:
        normalized = text.lower()
        for fragment, label in KEYWORD_LABELS:
            if fragment in normalized:
                return label
        return ""
