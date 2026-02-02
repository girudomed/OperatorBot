# Файл: app/services/reports.py

"""
Сервис генерации отчетов для операторов.
"""

import datetime
import hashlib
import json
from typing import Optional, Tuple, Dict, Any, List

from app.services.openai_service import OpenAIService
from app.db.repositories.operators import OperatorRepository
from app.db.repositories.reports_v2 import ReportsV2Repository
from app.db.manager import DatabaseManager
from app.logging_config import get_watchdog_logger
from app.utils.error_handlers import log_async_exceptions

logger = get_watchdog_logger(__name__)


class ReportService:
    SCORING_VERSION = "v2026-01-29-v4"
    MIN_COVERAGE_FOR_STRONG = 10

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.repo = OperatorRepository(db_manager)
        self.report_repo_v2 = ReportsV2Repository(db_manager)
        self.openai = OpenAIService()

    @log_async_exceptions
    async def generate_report(
        self,
        user_id: int,
        period: str = 'daily',
        date_range: Optional[str] = None,
        extension: Optional[str] = None,
    ) -> str:
        try:
            if not isinstance(user_id, int):
                logger.warning("report: некорректный user_id=%r", user_id)
                raise ValueError("user_id must be int")
            if period is not None and not isinstance(period, str):
                logger.warning("report: некорректный period=%r, используем daily", period)
                period = "daily"
            if date_range is not None and not isinstance(date_range, str):
                logger.warning("report: некорректный date_range=%r, игнорируем", date_range)
                date_range = None

            # 1. Resolve Dates
            start_date, end_date = self._resolve_dates(period, date_range)
            logger.info(
                "Генерация отчета для user_id=%s period=%s date_range=%s start=%s end=%s",
                user_id,
                period,
                date_range,
                start_date,
                end_date,
            )
            normalized_period = self._normalize_period(period)
            date_from = start_date if isinstance(start_date, datetime.datetime) else datetime.datetime.combine(start_date, datetime.time.min)
            date_to = end_date if isinstance(end_date, datetime.datetime) else datetime.datetime.combine(end_date, datetime.time.max)

            # legacy reports cache intentionally removed

            # 2. Get Operator Info
            try:
                resolved_extension = extension or await self.repo.get_extension_by_user_id(user_id)
            except Exception:
                logger.exception("report: ошибка получения extension для user_id=%s", user_id)
                raise
            if not resolved_extension:
                logger.warning(
                    "report: не найден extension для пользователя %s",
                    user_id,
                )
                return "Ошибка: Не удалось найти extension оператора."
            
            try:
                name = await self.repo.get_name_by_extension(resolved_extension)
            except Exception:
                logger.exception("report: ошибка получения имени по extension=%s", resolved_extension)
                raise

            # 2.1 Try v2 cache after resolving operator_key
            v2_cache_key = self._build_report_cache_key(
                operator_key=resolved_extension,
                date_from=date_from,
                date_to=date_to,
                period_label=normalized_period,
                filters={"user_id": user_id, "period": normalized_period, "date_range": date_range, "extension": resolved_extension},
                scoring_version=self.SCORING_VERSION,
            )
            try:
                existing_v2 = await self.report_repo_v2.get_ready_report_by_cache_key(v2_cache_key)
            except Exception:
                logger.exception("report: ошибка чтения кеша v2 (cache_key=%s)", v2_cache_key)
                raise
            if existing_v2 and existing_v2.get("report_text"):
                logger.info(
                    "Отчёт v2 уже существует (cache_key=%s) — возвращаем сохранённый результат",
                    v2_cache_key,
                )
                return existing_v2["report_text"]

            # 3. Get Call Data (ТОЛЬКО call_scores)
            try:
                scores = await self.repo.get_call_scores(resolved_extension, start_date, end_date)
            except Exception:
                logger.exception(
                    "report: ошибка получения call_scores (extension=%s, start=%s, end=%s)",
                    resolved_extension,
                    start_date,
                    end_date,
                )
                raise
            if not scores:
                logger.warning(
                    "report: нет данных по call_scores для %s (extension=%s, period=%s-%s)",
                    user_id,
                    resolved_extension,
                    start_date,
                    end_date,
                )
                await self._safe_save_report_status(
                    user_id=user_id,
                    operator_key=resolved_extension,
                    operator_name=name,
                    date_from=date_from,
                    date_to=date_to,
                    period_label=normalized_period,
                    filters={"user_id": user_id, "period": normalized_period, "date_range": date_range, "extension": resolved_extension},
                    metrics={},
                    cache_key=v2_cache_key,
                    status="empty",
                    error_text="no_call_scores",
                )
                return f"Нет данных для оператора {name} за указанный период."

            # 4. Calculate Metrics (только из call_scores)
            metrics = self._calculate_metrics_from_scores(scores)

            # 5. Собираем примеры звонков для GPT
            examples = self._build_call_examples(scores, limit=5)

            # 6. Генерируем отчёт через GPT (всегда)
            report_text = await self._generate_report_with_gpt(
                name=name,
                start=start_date,
                end=end_date,
                metrics=metrics,
                call_examples=examples,
            )
            if not report_text or not report_text.strip():
                logger.error(
                    "report: пустой ответ GPT для user_id=%s (extension=%s, period=%s-%s)",
                    user_id,
                    resolved_extension,
                    start_date,
                    end_date,
                )
                await self._safe_save_report_status(
                    user_id=user_id,
                    operator_key=resolved_extension,
                    operator_name=name,
                    date_from=date_from,
                    date_to=date_to,
                    period_label=normalized_period,
                    filters={"user_id": user_id, "period": normalized_period, "date_range": date_range, "extension": resolved_extension},
                    metrics=metrics,
                    cache_key=v2_cache_key,
                    status="error",
                    error_text="empty_gpt_response",
                )
                return "Произошла ошибка при генерации отчета."

            # 7. Save to reports_v2
            operator_key = resolved_extension
            filters = {
                "user_id": user_id,
                "period": normalized_period,
                "date_range": date_range,
                "extension": resolved_extension,
            }
            metrics_json = metrics.copy()
            cache_key = self._build_report_cache_key(
                operator_key=operator_key,
                date_from=date_from,
                date_to=date_to,
                period_label=normalized_period,
                filters=filters,
                scoring_version=self.SCORING_VERSION,
            )
            await self._safe_save_report_status(
                user_id=user_id,
                operator_key=operator_key,
                operator_name=name,
                date_from=date_from,
                date_to=date_to,
                period_label=normalized_period,
                filters=filters,
                metrics=metrics_json,
                cache_key=cache_key,
                status="ready",
                error_text=None,
                report_text=report_text,
            )

            # 8. Возвращаем сохранённый текст из reports_v2
            try:
                saved_v2 = await self.report_repo_v2.get_ready_report_by_cache_key(cache_key)
            except Exception:
                logger.exception("report: ошибка чтения сохранённого отчёта (cache_key=%s)", cache_key)
                raise
            if saved_v2 and saved_v2.get("report_text"):
                return saved_v2["report_text"]
            return report_text

        except Exception:
            logger.exception("Ошибка генерации отчета")
            raise

    def _calculate_metrics_from_scores(self, scores: List[Dict[str, Any]]) -> Dict[str, Any]:
        total_calls = 0
        booked = 0
        lead_no_record = 0
        cancellations = 0
        complaints = 0
        info_calls = 0
        total_score = 0.0
        score_count = 0
        total_talk = 0

        # Новые метрики (counts & coverage)
        m = {
            "objection_present": {"true": 0, "cov": 0},
            "objection_handled": {"true": 0, "cov": 0},
            "booking_attempted": {"true": 0, "cov": 0},
            "next_step_clear": {"true": 0, "cov": 0},
            "followup_captured": {"true": 0, "cov": 0},
            "handled_given_objection": {"true": 0, "cov": 0},
        }
        unknown = {
            "objection_handled": 0,
            "next_step_clear": 0,
            "followup_captured": 0,
        }

        for row in scores:
            if not isinstance(row, dict):
                logger.warning(
                    "report: некорректная строка call_scores (ожидался dict), пропускаем: %r",
                    row,
                )
                continue
            total_calls += 1
            outcome = (row.get("outcome") or "").lower()
            category = (row.get("call_category") or "").lower()
            score = row.get("call_score")
            if score is not None:
                total_score += float(score)
                score_count += 1
            duration = row.get("talk_duration") or 0
            total_talk += int(duration) if str(duration).isdigit() else 0

            # Старая воронка
            if outcome == "record":
                booked += 1
            elif outcome in ["lead_no_record", "lead"]:
                lead_no_record += 1
            elif outcome in ["info_only", "non_target", "info"]:
                info_calls += 1

            # Отмены считаем строго как отмены
            if outcome == "cancel" or "отмен" in category:
                cancellations += 1
            if "жалоб" in category:
                complaints += 1
            if not outcome and any(x in category for x in ["инфо", "подтверж", "пропущ"]):
                info_calls += 1

            # Новые флаги
            for flag in ["objection_present", "objection_handled", "booking_attempted", "next_step_clear", "followup_captured"]:
                val = row.get(flag)
                if val is not None:
                    m[flag]["cov"] += 1
                    if val == 1:
                        m[flag]["true"] += 1

            # Специальная метрика: обработка ПРИ наличии возражения
            if row.get("objection_present") == 1:
                oh = row.get("objection_handled")
                if oh is not None:
                    m["handled_given_objection"]["cov"] += 1
                    if oh == 1:
                        m["handled_given_objection"]["true"] += 1
                else:
                    unknown["objection_handled"] += 1

            if row.get("booking_attempted") == 1:
                ns = row.get("next_step_clear")
                if ns is None:
                    unknown["next_step_clear"] += 1

            if row.get("outcome") == "lead_no_record":
                fu = row.get("followup_captured")
                if fu is None:
                    unknown["followup_captured"] += 1

        conversion = (booked / total_calls) if total_calls else 0.0
        avg_score = (total_score / score_count) if score_count else 0.0

        res = {
            "total_calls": total_calls,
            "booked_services": booked,
            "lead_no_record": lead_no_record,
            "info_calls": info_calls,
            "total_cancellations": cancellations,
            "complaint_calls": complaints,
            "conversion_rate": round(conversion * 100, 2),
            "avg_call_rating": round(avg_score, 2),
            "total_conversation_time": total_talk,
            "avg_conversation_time": round(total_talk / total_calls, 2) if total_calls else 0.0,
            "cancellation_rate": round((cancellations / total_calls) * 100, 2) if total_calls else 0.0,
        }

        # Добавляем новые метрики в результат
        for key, vals in m.items():
            true_count = vals["true"]
            cov_count = vals["cov"]
            res[f"{key}_count"] = true_count
            res[f"{key}_coverage"] = cov_count
            res[f"{key}_rate"] = round((true_count / cov_count * 100), 2) if cov_count > 0 else None

        # Провалы (counts) для управления
        res["count_objection_not_handled"] = sum(
            1 for r in scores if r.get("objection_present") == 1 and r.get("objection_handled") == 0
        )
        res["count_objection_handled_unknown"] = unknown["objection_handled"]
        res["count_booking_no_next_step"] = sum(
            1 for r in scores if r.get("booking_attempted") == 1 and r.get("next_step_clear") == 0
        )
        res["count_booking_next_step_unknown"] = unknown["next_step_clear"]
        # Для lead_no_record мы хотим знать сколько из них БЕЗ followup
        res["count_lead_no_followup"] = sum(
            1 for r in scores if r.get("outcome") == "lead_no_record" and r.get("followup_captured") == 0
        )
        res["count_lead_followup_unknown"] = unknown["followup_captured"]

        return res

    def _build_call_examples(self, scores: List[Dict[str, Any]], limit: int = 5) -> str:
        def _row_key(row: Dict[str, Any]) -> int:
            row_id = row.get("id")
            return row_id if row_id is not None else id(row)

        valid_scores = []
        for row in scores:
            if not isinstance(row, dict):
                logger.warning(
                    "report: некорректная строка call_scores в примерах (ожидался dict), пропускаем: %r",
                    row,
                )
                continue
            valid_scores.append(row)

        # 1. 2 худших по score
        worst = sorted(
            [s for s in valid_scores if s.get("call_score") is not None],
            key=lambda x: x["call_score"],
        )[:2]
        
        # 2. 1 лучший по score
        best = sorted(
            [s for s in valid_scores if s.get("call_score") is not None],
            key=lambda x: x["call_score"],
            reverse=True,
        )[:1]
        
        # 3. Проблемные кейсы (возражение было, но не обработано)
        no_handle = [
            s for s in valid_scores if s.get("objection_present") == 1 and s.get("objection_handled") == 0
        ][:1]
        
        # 4. Проблемные кейсы (lead_no_record без follow-up)
        no_followup = [
            s for s in valid_scores if s.get("outcome") == "lead_no_record" and s.get("followup_captured") == 0
        ][:1]
        
        # Собираем уникальный список
        seen_ids = set()
        selected = []
        for s in worst + best + no_handle + no_followup:
            key = _row_key(s)
            if key not in seen_ids:
                selected.append(s)
                seen_ids.add(key)
        
        # Если не набрали лимит - добираем просто по порядку (но не те что уже есть)
        if len(selected) < limit:
            others = [s for s in valid_scores if _row_key(s) not in seen_ids]
            selected.extend(others[:(limit - len(selected))])

        examples = []
        for idx, row in enumerate(selected, start=1):
            transcript = (row.get("transcript") or "").strip()
            if len(transcript) > 600:
                transcript = transcript[:600] + "…"
            
            # Формируем строку флагов
            flags = []
            if row.get("objection_present") is not None:
                flags.append(f"Возражение: {'Да' if row['objection_present'] else 'Нет'}")
            if row.get("objection_handled") is not None:
                flags.append(f"Обработано: {'Да' if row['objection_handled'] else 'Нет'}")
            if row.get("booking_attempted") is not None:
                flags.append(f"Попытка записи: {'Да' if row['booking_attempted'] else 'Нет'}")
            if row.get("next_step_clear") is not None:
                flags.append(f"След.шаг ясен: {'Да' if row['next_step_clear'] else 'Нет'}")
            if row.get("followup_captured") is not None:
                flags.append(f"Follow-up: {'Да' if row['followup_captured'] else 'Нет'}")
            
            flags_str = " | ".join(flags)
            score_value = row.get("call_score")
            score_text = score_value if score_value is not None else "Нет"

            examples.append(
                f"### Звонок {idx}\n"
                f"- Оценка: {score_text} | Результат: {row.get('outcome') or '?'}\n"
                f"- Метрики: {flags_str}\n"
                f"- Услуга: {row.get('requested_service_name') or '?'}\n"
                f"- Фрагмент:\n{transcript or 'Нет расшифровки'}\n"
            )
        return "\n".join(examples) if examples else "Нет подходящих примеров звонков."

    async def _generate_report_with_gpt(
        self,
        name: str,
        start: Any,
        end: Any,
        metrics: Dict[str, Any],
        call_examples: str,
    ) -> str:
        period_line = f"{start} - {end}"
        template = (
            "# 1. Объём и типы звонков {name} (по факту из массива)\n\n"
            "По присланному материалу зафиксированы **≈{total_calls} завершённых диалогов** (часть — короткие, часть — длинные).\n\n"
            "Я разделяю их **по результату**, а не по длительности.\n\n"
            "## Итоговая воронка\n\n"
            "| Тип звонка | Кол-во |\n"
            "| --- | --- |\n"
            "| ✅ Запись оформлена | **{booked}** |\n"
            "| ❌ Запись не состоялась (консультация / «подумаю») | **{lead_no_record}** |\n"
            "| ❌ Отмена без перезаписи | **{cancellations}** |\n"
            "| ℹ️ Инфо / подтверждение / пропущенный | **{info_calls}** |\n"
            "| **Всего** | **{total_calls}** |\n\n"
            "### Конверсия {name} в запись\n\n"
            "- **{booked} / {total_calls} = ~{conversion}%**\n\n"
            "⚠️ Это **пограничное значение**:\n\n"
            "- для регистратуры — нормально\n"
            "- для **продающего колл-центра клиники — ниже нормы (ожидание 55–65%)**\n\n"
            "---\n\n"
            "# 2. Как {name} продаёт услуги (реальная модель поведения)\n\n"
            "## Общий стиль\n\n"
            "...\n\n"
            "👉 {name} **отвечает на запрос**, но **редко управляет диалогом**.\n\n"
            "---\n\n"
            "# 3. Продажа УЗИ: ключевой фокус анализа\n\n"
            "## 3.1. Предлагает ли {name} комплекс УЗИ\n\n"
            "**Факт:**\n\n"
            "...\n\n"
            "---\n\n"
            "## 3.2. Предлагает ли УЗИ нескольких органов / зон\n\n"
            "**Факт:**\n\n"
            "...\n\n"
            "---\n\n"
            "# 4. Работа с врачом как инструментом продаж\n\n"
            "## Что есть:\n\n"
            "- ...\n\n"
            "## Чего нет:\n\n"
            "- ...\n\n"
            "---\n\n"
            "# 5. Время и адрес — как {name} предлагает выбор\n\n"
            "## Время приёма\n\n"
            "...\n\n"
            "## Адреса клиники\n\n"
            "...\n\n"
            "---\n\n"
            "# 6. Возражения, которые {name} НЕ отрабатывает\n\n"
            "## Топ-возражения из звонков\n\n"
            "...\n\n"
            "---\n\n"
            "# 7. Где {name} работает ХОРОШО\n\n"
            "...\n\n"
            "---\n\n"
            "# 8. Ключевой управленческий вывод\n\n"
            "...\n\n"
            "---\n\n"
            "# 9. Потенциал роста (без увеличения нагрузки)\n\n"
            "...\n\n"
            "➡️ **конверсия может вырасти с ~{conversion}% до 60–65%**\n"
        )

        # Собираем блок фактов для промпта
        facts = [
            f"Имя оператора: {name}",
            f"Период: {period_line}",
            f"ВСЕГО звонков: {metrics.get('total_calls')}",
            f"Записи: {metrics.get('booked_services')}",
            f"Lead No Record: {metrics.get('lead_no_record')}",
            f"Отмены: {metrics.get('total_cancellations')}",
            f"Инфо: {metrics.get('info_calls')}",
            f"Конверсия: {metrics.get('conversion_rate')}%",
            f"Средняя оценка: {metrics.get('avg_call_rating')}",
            "",
            "НОВЫЕ МЕТРИКИ (ДЛЯ ЖЕСТКИХ ВЫВОДОВ):",
        ]

        # Добавляем флаги с coverage
        for key in ["objection_present", "objection_handled", "booking_attempted", "next_step_clear", "followup_captured", "handled_given_objection"]:
            rate = metrics.get(f"{key}_rate")
            cov = metrics.get(f"{key}_coverage", 0)
            true_count = metrics.get(f"{key}_count", 0)
            rate_text = f"{rate}%" if rate is not None else "н/д"
            facts.append(f"- {key}: {rate_text} (true={true_count}, cov={cov})")

        facts.extend([
            "",
            "ПРОИГРЫШНЫЕ СВЯЗКИ (COUNTS):",
            f"- Возражение было, но НЕ отработано: {metrics.get('count_objection_not_handled')} раз",
            f"- Возражение было, но обработка НЕ оценена: {metrics.get('count_objection_handled_unknown')} раз",
            f"- Запись предлагалась, но след.шаг НЕ ясен: {metrics.get('count_booking_no_next_step')} раз",
            f"- Запись предлагалась, но след.шаг НЕ оценен: {metrics.get('count_booking_next_step_unknown')} раз",
            f"- Лид без записи и БЕЗ follow-up: {metrics.get('count_lead_no_followup')} раз",
            f"- Лид без записи и follow-up НЕ оценен: {metrics.get('count_lead_followup_unknown')} раз",
        ])

        prompt = (
            "Ты — аналитик колл-центра клиники (Кумихо 🦊). Твоя задача — написать честный, жесткий и фактологичный отчет по оператору.\n"
            "Используй ТОЛЬКО предоставленные данные и примеры звонков. Если данных для вывода не хватает (низкий coverage), не выдумывай показатели, а пиши мягче (например, 'в предоставленных примерах не встретилось').\n\n"
            "СТИЛЬ ОТЧЕТА:\n"
            "- Как в эталонном примере Наили.\n"
            "- Минимум 'воды', максимум управленческих выводов.\n"
            "- Если видишь проигрышную связку (например, objection_handled_rate низкий) — делай из этого 'точку потери' в Разделе 6 и 8.\n"
            "- Жесткие формулировки допускаются только если coverage >= {min_cov}.\n"
            "- Если метрики по апселлу/комплексам нет, не пиши '0 раз' по отсутствию в примерах; пиши мягко ('в примерах не встретилось').\n\n"
            "ДАННЫЕ:\n"
            "{facts}\n\n"
            "ПРИМЕРЫ ЗВОНКОВ:\n"
            "{examples}\n\n"
            "КАРКАС (СТРОГО СОБЛЮДАЙ ВСЕ ЗАГОЛОВКИ):\n"
            "{template}\n"
        ).format(
            facts="\n".join(facts),
            examples=call_examples,
            min_cov=self.MIN_COVERAGE_FOR_STRONG,
            template=template.format(
                name=name,
                total_calls=metrics.get("total_calls", 0),
                booked=metrics.get("booked_services", 0),
                lead_no_record=metrics.get("lead_no_record", 0),
                cancellations=metrics.get("total_cancellations", 0),
                info_calls=metrics.get("info_calls", 0),
                conversion=metrics.get("conversion_rate", 0),
            )
        )

        try:
            logger.info(
                "report: GPT запрос (name=%s, period=%s - %s, prompt_chars=%s)",
                name,
                start,
                end,
                len(prompt),
            )
            return await self.openai.generate_recommendations(prompt, max_tokens=2500)
        except (ValueError, RuntimeError) as exc:
            logger.warning("Ожидаемая ошибка при генерации отчета GPT: %s", exc)
            return ""
        except Exception:
            logger.exception("Непредвиденная ошибка при генерации отчета GPT")
            raise

    async def _safe_save_report_status(
        self,
        *,
        user_id: int,
        operator_key: str,
        operator_name: Optional[str],
        date_from: datetime.datetime,
        date_to: datetime.datetime,
        period_label: str,
        filters: Dict[str, Any],
        metrics: Dict[str, Any],
        cache_key: str,
        status: str,
        error_text: Optional[str],
        report_text: str = "",
    ) -> None:
        try:
            await self.report_repo_v2.save_report(
                user_id=user_id,
                operator_key=operator_key,
                operator_name=operator_name,
                date_from=date_from,
                date_to=date_to,
                period_label=period_label,
                scoring_version=self.SCORING_VERSION,
                filters_json=filters,
                metrics_json=metrics,
                report_text=report_text,
                cache_key=cache_key,
                status=status,
                generated_at=datetime.datetime.utcnow(),
                error_text=error_text,
            )
        except Exception:
            logger.exception(
                "report: не удалось сохранить статус отчета (cache_key=%s status=%s)",
                cache_key,
                status,
            )
            raise


    def _build_report_cache_key(
        self,
        operator_key: str,
        date_from: datetime.datetime,
        date_to: datetime.datetime,
        period_label: str,
        filters: Dict[str, Any],
        scoring_version: str,
    ) -> str:
        payload = {
            "operator_key": operator_key,
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "period_label": period_label,
            "scoring_version": scoring_version,
            "filters": filters,
        }
        raw = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _normalize_period(self, period: Optional[str]) -> str:
        value = (period or "daily").strip().lower()
        mapping = {
            "day": "daily",
            "daily": "daily",
            "week": "weekly",
            "weekly": "weekly",
            "month": "monthly",
            "monthly": "monthly",
        }
        return mapping.get(value, value)

    def _format_report_date(self, start_date: Any) -> str:
        base_date: Optional[datetime.date]
        if isinstance(start_date, datetime.datetime):
            base_date = start_date.date()
        elif isinstance(start_date, datetime.date):
            base_date = start_date
        else:
            base_date = None
        if base_date:
            return base_date.strftime("%Y-%m-%d")
        return str(start_date)

    def _resolve_dates(
        self, 
        period: str, 
        date_range: Optional[str]
    ) -> Tuple[datetime.datetime, datetime.datetime]:
        now = datetime.datetime.now()
        
        if period == 'daily':
            if date_range:
                try:
                    dt = datetime.datetime.strptime(date_range, '%Y-%m-%d')
                except ValueError as exc:
                    logger.debug("Дата '%s' не соответствует формату YYYY-MM-DD: %s", date_range, exc)
                    try:
                        dt = datetime.datetime.strptime(date_range, '%d/%m/%Y')
                    except ValueError:
                        logger.warning("Невалидная дата '%s', используем текущую", date_range)
                        dt = now
                return dt.replace(hour=0, minute=0, second=0), dt.replace(hour=23, minute=59, second=59)
            return now.replace(hour=0, minute=0, second=0), now.replace(hour=23, minute=59, second=59)
            
        elif period == 'weekly':
            start = now - datetime.timedelta(days=now.weekday())
            return start.replace(hour=0, minute=0, second=0), now
            
        elif period == 'monthly':
            start = now.replace(day=1, hour=0, minute=0, second=0)
            return start, now
            
        # Default fallback
        return now.replace(hour=0, minute=0, second=0), now
