
"""Экраны для работы с LM-метриками."""

import json
import re
import html
from typing import List, Dict, Any, Optional, Tuple
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo
from telegram import InlineKeyboardButton

from app.telegram.ui.admin.screens import Screen
from app.telegram.utils.callback_lm import LMCB
from app.telegram.utils.callback_data import AdminCB
from app.services.lm_rules import METRIC_CONFIG, get_badge, decline_word
from app.logging_config import get_watchdog_logger

MIN_SAMPLE_SIZE = 30
LM_TRANSCRIPT_SNIPPET_LIMIT = 500

WORD_FORMS = {
    "call": ("звонок", "звонка", "звонков"),
    "task": ("задача", "задачи", "задач"),
    "client": ("клиент", "клиента", "клиентов"),
}

STATUS_ICONS = {
    "green": "🟢",
    "yellow": "🟡",
    "red": "🔴",
    "gray": "⚪",
}

MOSCOW_TZ = ZoneInfo("Europe/Moscow")

logger = get_watchdog_logger(__name__)

METHODOLOGY_SECTIONS = [
    {
        "title": "Response speed score (1–5)",
        "lines": [
            "Что: время ожидания клиента до ответа оператора.",
            "Как: используем call_history.await_sec и ступени &lt;20 / 40 / 60 / 120 секунд.",
            "Порог: &lt;2 баллов (ожидание >60 c) = красный статус, требуется разбор очереди.",
        ],
    },
    {
        "title": "Talk time efficiency (0–100)",
        "lines": [
            "Что: эффективность использования линии и времени клиента.",
            "Как: берём talk_duration, нормируем (длинные разговоры ≥60 c режутся капом).",
            "Порог: &lt;40 баллов означает, что контакты слишком короткие и риск недосказанности высок.",
        ],
    },
    {
        "title": "Conversion score (0–100)",
        "lines": [
            "Что: вероятность записи после звонка.",
            "Как: outcome='record' → 100, 'lead_no_record' → 50, инфо-звонки → 20, остальное → 0.",
            "Порог: &lt;60 баллов попадает в светофор «Конверсия» и требует обратной связи оператору.",
        ],
    },
    {
        "title": "Complaint risk flag / complaint_prob",
        "lines": [
            "Что: вероятность эскалации жалобы.",
            "Как: словари (lm_dictionary_terms) групп A–E + стоп-слова, веса фиксируются в БД; дополнительные факторы — низкий call_score, длительный разговор и категория «Жалоба».",
            "Порог: complaint_score ≥ 60 или конфликтная комбинация (call_score ≤3 + talk ≥30 c) → список «⚠️ Жалобы». Хиты сохраняются в lm_dictionary_hits.",
        ],
    },
    {
        "title": "Флаг «Нужно перезвонить»",
        "lines": [
            "Что: незакрытый процесс (клиент ждёт действия после звонка).",
            "Как: outcome ∈ лидовых сценариев, call_category='Лид (без записи)', коды отказов PATIENT_WILL_CLARIFY/CALL_BACK_LATER/THINKING/NO_TIME или технический сбой/non_target при реальном клиенте.",
            "Порог: flag=true => обязательный перезвон в течение 24 часов и фиксация исхода.",
        ],
    },
    {
        "title": "Lost opportunity score / count",
        "lines": [
            "Что: насколько болезненно упустили целевой звонок, и сколько их в периоде.",
            "Как: is_target=1 и outcome!='record' (исключая спам) дают базу 60 баллов; +10 за talk_duration ≥30 c, +20 при call_score ≤4, +10 если refusal_reason пустой. Количество фиксируем в summary как lost_opportunity_count.",
            "Порог: score ≥ 60 заносит звонок в список «💸 Потери», KPI — доля таких звонков от целевых.",
        ],
    },
]

def render_lm_summary_screen(
    history_id: int,
    metrics: Dict[str, Any],
    call_info: Optional[Dict[str, Any]] = None,
    action_context: Optional[str] = None,
    period_days: Optional[int] = None
) -> Screen:
    """
    Экрана сводки LM (Level 3.3: Один звонок).
    action_context: из какого списка пришли (followup, complaints, lost, churn)
    """
    from app.services.lm_rules import METRIC_CONFIG, EVIDENCE_RULES, get_badge
    
    # 1. Базовая инфо
    caller = call_info.get('caller_number') or "Звонок" if call_info else "Звонок"
    date_dt = None
    if call_info:
        date_dt = call_info.get('context_start_time_dt') or call_info.get('call_date') or call_info.get('context_start_time')
    date_str = date_dt.strftime('%d.%m %H:%M') if date_dt else "—"
    operator = _extract_operator_name(call_info.get('called_info')) or "—"
    source = call_info.get('utm_source_by_number') or "—"
    outcome = call_info.get('outcome') or "—"
    call_score = call_info.get('call_score', "—")
    talk_duration = call_info.get('talk_duration', 0)
    
    # 2. Метрики для светофоров
    speed = metrics.get('response_speed_score', {})
    efficiency = metrics.get('talk_time_efficiency', {})
    conversion = metrics.get('conversion_score', {})
    churn_lbl = metrics.get('churn_risk_level', {}).get('value_label', 'LOW')
    complaint_val = metrics.get('complaint_risk_flag', {}).get('value_numeric', 0)
    followup_data = metrics.get('followup_needed_flag', {}) or {}
    followup_flag = followup_data.get('value_label') == 'true'
    followup_reason = ((followup_data.get('value_json') or {}) if followup_data else {}).get('reason')

    speed_icon = get_badge(speed.get('value_numeric', 0), METRIC_CONFIG.get('response_speed_score', {'red': 2, 'yellow': 3}))
    churn_icon = "🔴" if churn_lbl in ("CRITICAL", "HIGH") else "🟢"
    complaint_icon = "⚠️" if complaint_val >= 60 else "✅"
    followup_icon = "📞" if followup_flag else "✅"

    text = (
        f"🎯 <b>Звонок #{history_id}</b>\n"
        f"<b>Дата/время:</b> {date_str}\n"
        f"<b>Оператор:</b> {operator}\n"
        f"<b>Источник:</b> {source}\n"
        f"<b>Исход:</b> {outcome}\n"
        f"<b>Скор:</b> {call_score}\n"
        f"<b>Длительность:</b> {talk_duration}s\n\n"
    )
    
    metric_reasons: List[str] = []
    has_evidence = False
    
    # Резоны из value_json (приоритет)
    context_keys = ["complaint_risk_flag", "followup_needed_flag", "lost_opportunity_score"]
    for ck in context_keys:
        m_data = metrics.get(ck, {})
        m_json = m_data.get("value_json") or {}
        if not m_json:
            continue
        reasons = m_json.get("reasons") or m_json.get("dictionary_hits_summary") or []
        for reason in reasons:
            clean_reason = str(reason).strip()
            if clean_reason:
                metric_reasons.append(f"• {clean_reason}")
                has_evidence = True
        hits = m_json.get("hits") or []
        for hit in hits[:3]:
            term = hit.get("term")
            if not term:
                continue
            impact = hit.get("impact") or hit.get("weight")
            snippet = hit.get("snippet")
            hit_line = f"• Триггер «{term}»"
            if impact:
                try:
                    hit_line += f" (+{float(impact):.0f})"
                except (TypeError, ValueError):
                    pass
            if snippet:
                hit_line += f": {snippet}"
            metric_reasons.append(hit_line)
            has_evidence = True
        for snippet in (m_json.get("snippets") or [])[:2]:
            text_snippet = str(snippet).strip()
            if text_snippet:
                metric_reasons.append(f"⤷ {text_snippet}")
                has_evidence = True
        if ck == "lost_opportunity_score" and m_json.get("loss_category"):
            metric_reasons.append(f"Категория отказа: {m_json['loss_category']}")
            has_evidence = True
        if m_json.get("requires_reason"):
            metric_reasons.append("⚠️ Причина отказа не заполнена — требуйте заполнения перед закрытием кейса.")
            has_evidence = True
        if m_json.get("result_excerpt"):
            metric_reasons.append(f"📝 Анализ: {m_json['result_excerpt']}")
            has_evidence = True

    transcript_truncated = False

    # 3. Блок "Почему в списке" и "Что сделать"
    if action_context and action_context != "none":
        text += f"📂 <b>Раздел: {action_context.upper()}</b>\n"
        
        if not metric_reasons and action_context in EVIDENCE_RULES:
            item_for_rules = {**call_info} if call_info else {}
            item_for_rules.update({k: v.get('value_numeric') for k, v in metrics.items() if 'value_numeric' in v})
            item_for_rules.update({k: v.get('value_label') for k, v in metrics.items() if 'value_label' in v})
            
            rules = EVIDENCE_RULES[action_context]
            for r in rules:
                try:
                    if r['condition'](item_for_rules):
                        metric_reasons.append(f"• {r['text'].format(**item_for_rules)}")
                except Exception: continue

        if metric_reasons:
            unique_reasons = []
            seen = set()
            for r in metric_reasons:
                clean_r = str(r).strip()
                if clean_r and clean_r not in seen:
                    unique_reasons.append(clean_r)
                    seen.add(clean_r)
            text += "<b>Почему в списке:</b>\n" + "\n".join(unique_reasons[:8]) + "\n\n"
        
        analysis = call_info.get("result") or call_info.get("operator_result")
        if analysis:
            short_analysis = str(analysis)[:400] + ("..." if len(str(analysis)) > 400 else "")
            text += f"🔍 <b>Анализ звонка:</b>\n<i>{short_analysis}</i>\n\n"
        refusal_reason_text = (call_info.get("refusal_reason") or call_info.get("refusal_comment") or "").strip()
        if refusal_reason_text:
            text += f"🚫 <b>Причина отказа:</b> {html.escape(refusal_reason_text)}\n\n"
        transcript_text = call_info.get("transcript") or call_info.get("raw_transcript")
        if transcript_text:
            snippet_raw = _strip_html(str(transcript_text)).strip()
            if snippet_raw:
                snippet = snippet_raw[:LM_TRANSCRIPT_SNIPPET_LIMIT]
                if len(snippet_raw) > LM_TRANSCRIPT_SNIPPET_LIMIT:
                    snippet = snippet.rstrip() + "…"
                    transcript_truncated = True
                safe_snippet = html.escape(snippet)
                text += f"📝 <b>Расшифровка (фрагмент):</b>\n<code>{safe_snippet}</code>\n\n"
                if transcript_truncated:
                    text += "<i>Текст сокращён. Нажмите «Показать больше», чтобы увидеть полную расшифровку.</i>\n\n"

        mapping = {
            "followup": "followup_needed_flag",
            "complaints": "complaint_risk_flag",
            "lost": "lost_opportunity_score",
            "churn": "churn_risk_level"
        }
        conf = METRIC_CONFIG.get(mapping.get(action_context, ""))
        if conf:
            text += f"✅ <b>Что сделать:</b>\n{conf['action_text']}\n\n"
    elif has_evidence:
         text += "📌 <b>Особенности звонка:</b>\n" + "\n".join(metric_reasons[:5]) + "\n\n"

    text += (
        "<b>Индикаторы:</b>\n"
        f"{speed_icon} Ожидание: {speed.get('value_numeric', 0)}/5\n"
        f"⚡ Эффективность: {efficiency.get('value_numeric', 0):.1f}\n"
        f"💰 Конверсия: {conversion.get('value_numeric', 0):.1f}\n"
        f"{churn_icon} Риск оттока: {churn_lbl}\n"
        f"{complaint_icon} Риск жалобы: {'ДА' if complaint_val >= 60 else 'НЕТ'}\n"
        f"{followup_icon} Нужно перезвонить: {'НУЖЕН' if followup_flag else 'НЕТ'}\n"
    )
    if followup_flag and followup_reason:
        text += f"{followup_reason}\nSLA: перезвонить в течение 24 часов.\n"
    
    # 4. Клавиатура
    keyboard = []

    bundle_cb = AdminCB.create(
        AdminCB.CALL,
        "bundle",
        history_id,
        "lm",
        action_context or "none",
    )
    keyboard.append([
        InlineKeyboardButton(
            "🎧 Аудио и текст",
            callback_data=bundle_cb,
        )
    ])
    if transcript_truncated:
        full_cb = AdminCB.create(
            AdminCB.CALL,
            "full_transcript",
            history_id,
            "lm",
            action_context or "none",
        )
        keyboard.append([
            InlineKeyboardButton(
                "📄 Показать больше",
                callback_data=full_cb,
            )
        ])

    if action_context and action_context != "none":
        back_callback = LMCB.create(LMCB.ACTION_LIST, action_context, 0)
    else:
        back_callback = AdminCB.create(AdminCB.LM_MENU, AdminCB.lm_SUM, period_days or "")
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data=back_callback)])

    return Screen(text=text, keyboard=keyboard, parse_mode="HTML")

def render_lm_action_list_screen(
    action_type: str,
    items: List[Dict[str, Any]],
    page: int = 0,
    total: int = 0,
    period_days: Optional[int] = None
) -> Screen:
    """
    Экран списка действий («Нужно перезвонить», риски).
    """
    titles = {
        "followup": "📞 Нужно перезвонить",
        "complaints": "⚠️ Возможные жалобы",
        "churn": "📉 Риск ухода клиентов",
        "lost": "💸 Потерянные обращения",
    }
    title = titles.get(action_type, "Список действий")
    
    rules = {
        "followup": "Клиент или оператор ждёт возвращения к разговору. SLA: 24 часа.",
        "complaints": "Есть явные признаки недовольства в диалоге.",
        "churn": "Клиенты с высоким риском ухода — требуется удержание.",
        "lost": "Целевые обращения без записи — нужно вернуть в воронку.",
    }

    text_header = f"<b>{title}</b>\n"
    rule_text = rules.get(action_type)
    if rule_text:
        text_header += f"{rule_text}\n"

    if not items:
        text_header += "\n<i>Список пуст. Хорошая работа!</i>"
        text = text_header
    else:
        text_header += f"Всего элементов: {total}\n\n"
        entry_chunks: List[str] = []
        for i, item in enumerate(items, 1):
            h_id = item.get('history_id')
            created = item.get('call_date') or item.get('created_at')
            date_str = created.strftime('%d.%m %H:%M') if created else "—"
            operator = _extract_operator_name(item.get("called_info")) or "—"
            source = item.get("utm_source_by_number") or "—"
            outcome = item.get("outcome") or "—"
            call_score = item.get("call_score", "—")
            
            reasons, next_step = _describe_action_item(action_type, item)
            
            reasons = _shorten_text(reasons, 320)
            next_step = _shorten_text(next_step, 220)
            entry_chunks.append(
                f"#{h_id} | {date_str} | {operator} | {source}\n"
                f"Исход: {outcome} | Скор: {call_score}\n"
                f"Причина: {reasons}\n"
                f"Действие: {next_step}\n\n"
            )
        MAX_TEXT = 3500
        text = text_header
        pruned = False
        added = 0
        for chunk in entry_chunks:
            if len(text) + len(chunk) > MAX_TEXT:
                pruned = True
                break
            text += chunk
            added += 1
        if pruned:
            remaining = len(entry_chunks) - added
            text = text.rstrip() + f"\n…и ещё {remaining} записей, откройте следующую страницу."

    keyboard = []
    # Элементы списка как кнопки для перехода
    for item in items:
        h_id = item.get('history_id')
        keyboard.append([
            InlineKeyboardButton(
                f"🔎 Детали #{h_id}",
                callback_data=LMCB.create(LMCB.ACTION_SUMMARY, h_id, action_type),
            )
        ])
    
    # Навигация
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Назад", callback_data=LMCB.create(LMCB.ACTION_LIST, action_type, page - 1)))
    if total > (page + 1) * 10:
        nav_row.append(InlineKeyboardButton("Вперед ➡️", callback_data=LMCB.create(LMCB.ACTION_LIST, action_type, page + 1)))
    
    if nav_row:
        keyboard.append(nav_row)
        
    keyboard.append([
        InlineKeyboardButton("◀️ В сводку LM", callback_data=AdminCB.create(AdminCB.LM_MENU, AdminCB.lm_SUM, period_days or "")),
        InlineKeyboardButton("🏠 Админка", callback_data=AdminCB.create(AdminCB.BACK))
    ])
    
    return Screen(text=text, keyboard=keyboard, parse_mode="HTML")


def render_lm_periods_screen(
    summary: Dict[str, Any],
    selected_days: int,
    available_periods: tuple[int, ...],
) -> Screen:
    """
    Экран агрегированной LM-аналитики в формате сигнальной сводки.
    """
    header = "🧠 <b>LM-аналитика</b>\n"
    if not summary:
        text = header + "\n<i>Нет накопленных данных по LM метрикам за выбранный период.</i>"
        keyboard = [
            [InlineKeyboardButton("🏠 В админ-панель", callback_data=AdminCB.create(AdminCB.DASHBOARD))],
            [InlineKeyboardButton("◀️ Назад", callback_data=AdminCB.create(AdminCB.BACK))],
        ]
        return Screen(text=text, keyboard=keyboard)

    period_label = _format_period_label(summary.get("start_date"), summary.get("end_date"))
    calls_total = summary.get("call_count", 0)
    base = summary.get("base", {})
    lost_total = base.get("lost_opportunity_count")
    updated_at = summary.get("updated_at")
    coverage = summary.get("coverage")

    metrics = summary.get("metrics", {})
    flags = summary.get("flags", {})
    churn = summary.get("churn", {})
    action_counts = summary.get("action_counts") or {}

    complaint_metric_count = metrics.get("complaint_risk_flag", {}).get("alert_count", 0)
    followup_metrics = flags.get("followup_needed_flag", {}) or {}
    followup_metric_count = followup_metrics.get("true_count", 0)
    followup_total = followup_metrics.get("total") or 0
    lost_metrics = metrics.get("lost_opportunity_score", {}) or {}
    lost_metric_count = lost_metrics.get("alert_count", 0)
    lost_fact = base.get("lost_opportunity_count")
    if lost_fact is None:
        lost_fact = lost_metrics.get("count")
    lost_fact = int(lost_fact or 0)
    churn_metric_high = churn.get("high", 0)
    churn_total = sum(int(v or 0) for v in churn.values()) if churn else 0

    def _resolve_action_count(key: str, fallback: int) -> int:
        value = action_counts.get(key)
        if value is None:
            return int(fallback or 0)
        try:
            return int(value)
        except (TypeError, ValueError):
            return int(fallback or 0)

    complaint_count = _resolve_action_count("complaints", complaint_metric_count)
    followup_count = _resolve_action_count("followup", followup_metric_count)
    lost_count = _resolve_action_count("lost", lost_metric_count)
    churn_high = _resolve_action_count("churn", churn_metric_high)

    coverage_line = _build_coverage_text(coverage)

    text_parts = []
    text_parts.append("🧠 <b>LM-АНАЛИТИКА</b>")
    text_parts.append("ℹ️ Дашборд отвечает на вопрос «что происходит». LM — «почему это произошло и что делать».")
    text_parts.append(f"<b>Период:</b> {period_label} (последние {selected_days} дн.)")
    if lost_total is not None:
        text_parts.append(f"<b>Потери:</b> {lost_total} целевых без записи")
    text_parts.append(f"<b>Обновлено:</b> {_format_datetime(updated_at)}")
    text_parts.append("⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯")

    text_parts.append("\n<b>⚡ ГЛАВНОЕ</b>")
    text_parts.append(_build_headline(summary, calls_total).strip())

    text_parts.append("\n<b>✅ ЧТО СДЕЛАТЬ СЕГОДНЯ</b>")
    text_parts.append(
        _build_actions_today_section(
            complaint_count,
            followup_count,
            followup_total,
            lost_count,
            lost_fact,
            churn_high,
            churn_total,
            calls_total,
        ).strip()
    )

    text_parts.append("\n<b>📌 КАЧЕСТВО ДАННЫХ</b>")
    text_parts.append(_build_data_quality_section(summary, coverage_line).strip())

    text_parts.append("\n<b>🚦 ИНДИКАТОРЫ</b>")
    text_parts.append(_build_indicators_block(summary, calls_total).strip())

    loss_section = _build_loss_breakdown_section(summary)
    if loss_section:
        text_parts.append("\n<b>💸 ПОТЕРИ</b>")
        text_parts.append(loss_section.strip())
    text_parts.append(_build_week_actions_section(summary).strip())

    text_parts.append("\n<b>📂 СПИСКИ ДЛЯ ОБРАБОТКИ</b>")
    text_parts.append(
        _build_action_lists_description(
            complaint_count,
            followup_count,
            followup_total,
            lost_count,
            lost_fact,
            churn_high,
            churn_total,
        ).strip()
    )

    keyboard: List[List[InlineKeyboardButton]] = []
    action_buttons: List[InlineKeyboardButton] = []
    if complaint_count:
        action_buttons.append(
            InlineKeyboardButton(
                f"⚠️ Возможные жалобы ({complaint_count})",
                callback_data=LMCB.create(LMCB.ACTION_LIST, "complaints", 0),
            )
        )
    if followup_count:
        action_buttons.append(
            InlineKeyboardButton(
                f"📞 Нужно перезвонить ({followup_count})",
                callback_data=LMCB.create(LMCB.ACTION_LIST, "followup", 0),
            )
        )
    if lost_count:
        action_buttons.append(
            InlineKeyboardButton(
                f"💸 Потерянные обращения ({lost_count})",
                callback_data=LMCB.create(LMCB.ACTION_LIST, "lost", 0),
            )
        )
    if churn_high:
        action_buttons.append(
            InlineKeyboardButton(
                f"📉 Отток ({churn_high})",
                callback_data=LMCB.create(LMCB.ACTION_LIST, "churn", 0),
            )
        )
    while action_buttons:
        keyboard.append(action_buttons[:2])
        action_buttons = action_buttons[2:]

    period_row: List[InlineKeyboardButton] = []
    for days in available_periods:
        label = f"{days} дн."
        prefix = "✅" if days == selected_days else "📅"
        period_row.append(
            InlineKeyboardButton(
                f"{prefix} {label}",
                callback_data=AdminCB.create(AdminCB.LM_MENU, AdminCB.lm_SUM, days),
            )
        )
    if period_row:
        keyboard.append(period_row)

    keyboard.append(
        [
            InlineKeyboardButton(
                "📘 Методика расчёта",
                callback_data=LMCB.create(LMCB.ACTION_METHOD, "period", selected_days),
            )
        ]
    )

    keyboard.append(
        [
            InlineKeyboardButton(
                "🔄 Обновить",
                callback_data=AdminCB.create(AdminCB.LM_MENU, AdminCB.lm_SUM, selected_days),
            ),
            InlineKeyboardButton(
                "⬅️ В админ-панель",
                callback_data=AdminCB.create(AdminCB.DASHBOARD),
            ),
        ]
    )
    keyboard.append(
        [
            InlineKeyboardButton("◀️ Назад", callback_data=AdminCB.create(AdminCB.BACK)),
        ]
    )

    text = "\n".join(text_parts)
    return Screen(text=text, keyboard=keyboard, parse_mode="HTML")


def _build_headline(summary: Dict[str, Any], calls_total: int) -> str:
    metrics = summary.get("metrics", {})
    flags = summary.get("flags", {})
    summary_line = []

    if calls_total < MIN_SAMPLE_SIZE:
        return "⚪ Выборка мала — дождитесь большего периода, прежде чем принимать решения.\n"

    quality_value = metrics.get("normalized_call_score", {}).get("avg")
    followup_share = _safe_ratio(flags.get("followup_needed_flag", {}).get("true_count"), calls_total)
    complaint_count = metrics.get("complaint_risk_flag", {}).get("alert_count", 0)

    if quality_value is not None and quality_value < 65:
        summary_line.append("качество разговоров ниже нормы")
    if followup_share is not None and followup_share >= 0.10:
        summary_line.append("много незакрытых задач «Нужно перезвонить»")
    if complaint_count:
        summary_line.append("есть кейсы высокого риска жалобы")

    if not summary_line:
        return "🟢 Ключевые показатели в норме — держите текущий ритм контроля.\n"

    return "⚠️ " + "; ".join(summary_line) + ".\n"


def _build_actions_today_section(
    complaint_count: int,
    followup_count: int,
    followup_total: int,
    lost_count: int,
    lost_total: int,
    churn_high: int,
    churn_total: int,
    calls_total: int,
) -> str:
    if calls_total < MIN_SAMPLE_SIZE:
        return "⚪ Слишком мало звонков – дождитесь накопления данных, прежде чем проводить действия.\n"

    entries: List[str] = []
    if complaint_count:
        entries.append(
            f"1) ⚠️ Возможные жалобы: {_format_with_word(complaint_count, WORD_FORMS['call'])} — обработать в течение 24 часов и зафиксировать результат."
        )
    if followup_count:
        entries.append(
            f"{len(entries)+1}) 📞 Нужно перезвонить: факт {followup_total}, к обработке {followup_count} — перезвонить ≤24 ч и закрыть вопрос."
        )
    if lost_count:
        entries.append(
            f"{len(entries)+1}) 💸 Потерянные обращения: факт {lost_total}, к обработке {lost_count} — довнести причину отказа и вернуть клиента в воронку."
        )
    if churn_high:
        entries.append(
            f"{len(entries)+1}) 📉 Риск ухода: факт {churn_total}, к обработке {churn_high} — назначить ответственного за удержание и отчитаться в течение 48 часов."
        )

    if not entries:
        return "Сегодня критичных действий нет — контролируйте дашборд.\n"
    return "\n".join(entries) + "\n"


def _build_week_actions_section(summary: Dict[str, Any]) -> str:
    coverage = summary.get("coverage") or {}
    operator_entry = coverage.get("operator") or {}
    utm_entry = coverage.get("utm") or {}
    operator_cov = operator_entry.get("percent") or 0.0
    utm_cov = utm_entry.get("percent") or 0.0
    refusal_cov = (coverage.get("refusal") or {}).get("percent") or 0.0
    utm_breakdown = summary.get("utm_breakdown") or []
    period_days = summary.get("period_days")
    period_label = f"\nЗа последние {period_days} дн.:\n" if period_days else "\n"
    notes: List[str] = []
    if operator_cov < 20:
        notes.append(
            f"Операторы заполнены: {operator_cov:.0f}% — сравнение по операторам ограничено."
        )
    if utm_cov < 20:
        notes.append(
            f"Источник обращения заполнен: {utm_cov:.0f}% — разбор по источникам ограничен."
        )
    if refusal_cov < 20:
        notes.append(
            f"Причины отказа заполнены: {refusal_cov:.0f}% — аналитика потерь неточна."
        )

    if notes:
        return period_label + "\n".join(notes) + "\n"

    if not utm_breakdown:
        return period_label + "Источник обращения: данных нет.\n"

    def _format_share(value: Any) -> str:
        try:
            share_val = float(value)
        except (TypeError, ValueError):
            return "0%"
        if share_val.is_integer():
            return f"{int(share_val)}%"
        return f"{share_val:.1f}%"

    lines = [period_label + "Источник обращения"]
    for item in utm_breakdown:
        label = item.get("label") or "Не указан"
        if label.lower() in {"не указан", "не указано"}:
            continue
        count = int(item.get("count") or 0)
        share = _format_share(item.get("share"))
        lines.append(f"{label}: {count} штук ({share})")

    return "\n".join(lines) + "\n"


def _build_indicators_block(summary: Dict[str, Any], calls_total: int) -> str:
    metrics = summary.get("metrics", {})
    flags = summary.get("flags", {})
    churn = summary.get("churn", {}) or {}
    base = summary.get("base", {}) or {}
    blocks: List[str] = []

    def _status_phrase(code: str) -> Optional[str]:
        return {
            "green": "в норме",
            "yellow": "ниже нормы",
            "red": "критично",
        }.get(code)

    def _add_block(
        title: str,
        description: str,
        *,
        status: Optional[str] = None,
        icon: Optional[str] = None,
        fallback: Optional[str] = None,
    ) -> None:
        symbol = icon or STATUS_ICONS.get(status or "", "⚪")
        status_text = _status_phrase(status) if status else None
        if fallback:
            status_text = fallback
        header = f"{symbol} {title}"
        if status_text:
            header += f" — {status_text}"
        block_lines = [header, description]
        blocks.append("\n".join(block_lines))

    # Качество общения
    quality = metrics.get("normalized_call_score", {})
    q_value = quality.get("avg")
    if q_value is None:
        _add_block("Качество общения", "Средняя оценка разговоров", icon="⚪", fallback="данных недостаточно")
    else:
        status = _status_from_value(q_value, 70, 65)
        _add_block("Качество общения", "Средняя оценка разговоров", status=status, icon=STATUS_ICONS.get(status))

    # Записи с обращений
    conversion = metrics.get("conversion_score", {})
    c_value = conversion.get("avg")
    if c_value is None:
        _add_block("Записи с обращений", "Сколько целевых звонков дошли до записи", icon="⚪", fallback="данных недостаточно")
    else:
        status = _status_from_value(c_value, 70, 60)
        _add_block("Записи с обращений", "Сколько целевых звонков дошли до записи", status=status, icon=STATUS_ICONS.get(status))

    # Риск жалоб
    complaint_metrics = metrics.get("complaint_risk_flag", {}) or {}
    complaint_count = complaint_metrics.get("alert_count", 0)
    complaint_sample = complaint_metrics.get("count") or calls_total
    if not complaint_sample:
        _add_block("Риск жалоб", "Звонки с признаками недовольства", icon="⚪", fallback="данных недостаточно")
    elif complaint_sample < MIN_SAMPLE_SIZE:
        _add_block("Риск жалоб", "Звонки с признаками недовольства", icon="⚪", fallback="недостаточно данных")
    else:
        status = "red" if complaint_count else "green"
        status_text = "есть сигналы" if complaint_count else "в норме"
        _add_block("Риск жалоб", "Звонки с признаками недовольства", status=status, icon="⚠️", fallback=status_text)

    # Требуют перезвона
    followup_meta = flags.get("followup_needed_flag", {}) or {}
    followup_total = int(followup_meta.get("total") or 0)
    followup_count = int(followup_meta.get("true_count") or 0)
    followup_denominator = followup_total if followup_total else calls_total
    followup_share = _safe_ratio(followup_count, followup_denominator)
    if followup_share is None:
        _add_block("Требуют перезвона", "Клиент ждал обратной связи", icon="📞", fallback="данных недостаточно")
    else:
        status = _status_from_share(followup_share, 0.20, 0.10)
        _add_block("Требуют перезвона", "Клиент ждал обратной связи", status=status, icon="📞")

    # Потерянные обращения
    lost_total = base.get("lost_opportunity_count")
    if lost_total is None:
        lost_total = metrics.get("lost_opportunity_score", {}).get("count", 0)
    lost_total = int(lost_total or 0)
    lost_count = metrics.get("lost_opportunity_score", {}).get("alert_count", 0)
    lost_denominator = lost_total if lost_total else calls_total
    lost_share = _safe_ratio(lost_count, lost_denominator) if lost_denominator else None
    if lost_share is None:
        _add_block("Потерянные обращения", "Целевые звонки без записи", icon="💸", fallback="данных недостаточно")
    else:
        status = _status_from_share(lost_share, 0.08, 0.15)
        _add_block("Потерянные обращения", "Целевые звонки без записи", status=status, icon="💸")

    # Риск ухода клиентов
    churn_counts = {k: int(v or 0) for k, v in churn.items()}
    churn_total = sum(churn_counts.values())
    churn_high = churn_counts.get("high", 0) + churn_counts.get("critical", 0)
    if churn_total == 0:
        _add_block("Риск ухода клиентов", "Клиенты с признаками оттока", icon="📉", fallback="данных недостаточно")
    else:
        if churn_high > 0:
            status = "red"
        elif churn_counts.get("medium", 0) > 0:
            status = "yellow"
        else:
            status = "green"
        _add_block("Риск ухода клиентов", "Клиенты с признаками оттока", status=status, icon="📉")

    return "\n\n".join(blocks) + "\n"


def _build_action_lists_description(
    complaint_count: int,
    followup_count: int,
    followup_total: int,
    lost_count: int,
    lost_total: int,
    churn_high: int,
    churn_total: int,
) -> str:
    lines = [
        f"1. ⚠️ Возможные жалобы ({complaint_count})",
        f"2. 📞 Нужно перезвонить: факт {followup_total}, к обработке {followup_count}.",
        f"3. 💸 Потерянные обращения: факт {lost_total}, к обработке {lost_count}.",
        f"4. 📉 Риск ухода: факт {churn_total}, к обработке {churn_high}.",
        "⬅️ Назад | 🔄 Обновить",
    ]
    return "\n".join(lines) + "\n"


def _build_coverage_text(coverage: Optional[Dict[str, Any]]) -> str:
    if not coverage:
        return "н/д"
    parts = []
    labels = {
        "transcript": "транскрипт",
        "outcome": "исход",
        "refusal": "причина отказа",
        "operator": "оператор",
    }
    for key, label in labels.items():
        entry = coverage.get(key) if coverage else None
        if entry and entry.get("percent") is not None:
            parts.append(f"{label}={entry['percent']:.1f}%")
        else:
            parts.append(f"{label}=н/д")
    return ", ".join(parts)


def _build_data_quality_section(summary: Dict[str, Any], compact_line: str) -> str:
    coverage = summary.get("coverage") or {}
    if not coverage:
        return "<b>⚠️ Нет данных о заполненности — аналитика ограничена.</b>\n"

    warning_lines: List[str] = []
    info_lines: List[str] = [compact_line]
    refusal = (coverage.get("refusal") or {}).get("percent") or 0.0
    operator = (coverage.get("operator") or {}).get("percent") or 0.0

    if refusal < 60:
        warning_lines.append(f"Причина отказа заполнена на {refusal:.0f}% — анализ потерь ограничен.")
    if operator < 80:
        warning_lines.append(f"Данные по операторам заполнены на {operator:.0f}% — сложнее вести разборы качества.")

    bookings = summary.get("bookings") or []
    if bookings:
        top_strings = []
        for row in bookings[:3]:
            cat = row.get("call_category") or "Без категории"
            cnt = row.get("cnt") or 0
            top_strings.append(f"{cat}: {cnt}")
        if top_strings:
            info_lines.append("Записи по каналам за период: " + ", ".join(top_strings))

    if warning_lines:
        return (
            "<b>⚠️ ВНИМАНИЕ: данные ограничены</b>\n"
            + "\n".join(warning_lines)
            + ("\n" + "\n".join(info_lines) if info_lines else "")
            + "\n"
        )

    if len(info_lines) == 1:
        info_lines.append("Заполнение ключевых полей достаточное — можно смотреть драйверы.")
    return "\n".join(info_lines) + "\n"


def _build_loss_breakdown_section(summary: Dict[str, Any]) -> str:
    breakdown = summary.get("loss_breakdown") or []
    if not breakdown:
        return ""
    lines: List[str] = []
    for item in breakdown[:3]:
        label = item.get("label") or "Не указано"
        count = int(item.get("count") or 0)
        share_val = item.get("share")
        share_text = ""
        try:
            share_float = float(share_val)
            if share_float > 0:
                share_text = f" ({share_float:.0f}%)"
        except (TypeError, ValueError):
            share_text = ""
        lines.append(f"{label}: {count}{share_text}")
    if not lines:
        return ""
    return "\n".join(lines) + "\n"


def render_lm_methodology_screen(back_callback: Optional[str] = None) -> Screen:
    """Экран с методикой расчёта LM-метрик."""
    lines: List[str] = ["📘 <b>Методика расчёта LM</b>", "Каждая метрика — детерминированное правило без ИИ."]
    for section in METHODOLOGY_SECTIONS:
        lines.append(f"\n<b>{section['title']}</b>")
        for detail in section["lines"]:
            lines.append(f"• {detail}")
    lines.append("\nСловари и факты срабатывания: таблицы <code>lm_dictionary_terms</code> и <code>lm_dictionary_hits</code>.")
    keyboard: List[List[InlineKeyboardButton]] = []
    if back_callback:
        keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data=back_callback)])
    return Screen(text="\n".join(lines), keyboard=keyboard)


def _format_with_word(count: int, forms: Tuple[str, str, str]) -> str:
    return f"{count} {forms[_word_form_index(count)]}"


def _word_form_index(count: int) -> int:
    count = abs(count)
    if 11 <= count % 100 <= 14:
        return 2
    last = count % 10
    if last == 1:
        return 0
    if 2 <= last <= 4:
        return 1
    return 2


_HTML_TAG_RE = re.compile(r"</?[^>]+>")


def _strip_html(text: str) -> str:
    """Удаляет HTML-теги из текста для безопасного отображения."""
    return _HTML_TAG_RE.sub("", text)


def _format_period_label(start: Optional[date], end: Optional[date]) -> str:
    if not start or not end:
        return "Недостаточно данных"
    if start == end:
        return start.strftime("%d %b %Y")
    same_month = start.month == end.month and start.year == end.year
    if same_month:
        return f"{start.strftime('%d')}–{end.strftime('%d %b %Y')}"
    return f"{start.strftime('%d %b')}–{end.strftime('%d %b %Y')}"


def _format_datetime(value: Optional[Any]) -> str:
    if not value:
        return "н/д"
    try:
        if isinstance(value, datetime):
            dt = value
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            dt = dt.astimezone(MOSCOW_TZ)
            return f"{dt.strftime('%d %b %Y %H:%M:%S')} MSK"
        return str(value)
    except Exception:
        return str(value)


def _format_share(count: int, total: int) -> str:
    if not total:
        return ""
    percent = (count / total) * 100
    return f" ({percent:.0f}%)"


def _format_score(value: Optional[Any], *, precision: int = 1) -> str:
    if value is None:
        return "—"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "—"
    fmt = f"{{:.{precision}f}}"
    text = fmt.format(number)
    if precision == 0:
        return text.split(".")[0]
    return text.rstrip("0").rstrip(".")


def _format_percent(value: Optional[Any]) -> str:
    if value is None:
        return "—"
    try:
        number = float(value) * 100
    except (TypeError, ValueError):
        return "—"
    return f"{number:.0f}%"


def _format_delta_suffix(value: Optional[Any], *, precision: int = 1) -> str:
    if value is None:
        return ""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return ""
    if abs(number) < 10 ** (-precision):
        return ""
    fmt = f"{{:+.{precision}f}}"
    text = fmt.format(number)
    if precision == 0:
        text = text.split(".")[0]
    return f" ({text} к прошлому периоду)"


def _status_from_value(value: Optional[Any], green_from: float, yellow_from: float) -> str:
    if value is None:
        return "gray"
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "gray"
    if numeric >= green_from:
        return "green"
    if numeric >= yellow_from:
        return "yellow"
    return "red"


def _status_from_share(value: Optional[Any], green_limit: float, yellow_limit: float) -> str:
    if value is None:
        return "gray"
    if value <= green_limit:
        return "green"
    if value <= yellow_limit:
        return "yellow"
    return "red"


def _safe_ratio(count: Optional[int], total: int) -> Optional[float]:
    if not total:
        return None
    try:
        return float(count or 0) / total
    except ZeroDivisionError:
        return None


def _describe_action_item(action_type: str, item: Dict[str, Any]) -> Tuple[str, str]:
    """
    Возвращает (причины, действие) для элемента списка действий.
    """
    from app.services.lm_rules import EVIDENCE_RULES, METRIC_CONFIG
    
    rules = EVIDENCE_RULES.get(action_type, [])
    found_reasons: List[str] = []

    meta_payload = item.get('value_json')
    meta_dict: Optional[Dict[str, Any]] = None
    if isinstance(meta_payload, str):
        try:
            meta_payload = json.loads(meta_payload)
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            logger.warning("LM screens: не удалось распарсить meta_payload: %s", exc)
            meta_payload = None
        except Exception:
            logger.exception("LM screens: непредвиденная ошибка парсинга meta_payload")
            raise
    if isinstance(meta_payload, dict):
        meta_dict = meta_payload
        meta_reasons = meta_payload.get('reasons') or []
        if meta_reasons:
            found_reasons.extend(meta_reasons[:2])
        elif meta_payload.get('reason'):
            found_reasons.append(meta_payload['reason'])
        hits = meta_payload.get('hits') or []
        for hit in hits[:1]:
            term = hit.get("term")
            snippet = hit.get("snippet")
            if term:
                line = f"Триггер «{term}»"
                if snippet:
                    line += f": {snippet}"
                found_reasons.append(line)
        snippets = meta_payload.get("snippets") or []
        if snippets:
            found_reasons.append(f"⤷ {snippets[0]}")
        if meta_payload.get("loss_category"):
            found_reasons.append(f"Категория: {meta_payload['loss_category']}")
        if meta_payload.get("requires_reason"):
            found_reasons.append("⚠️ Требуется заполнить причину отказа.")
    
    for r in rules:
        if len(found_reasons) >= 2:
            break
        try:
            condition = r.get('condition')
            reason_template = r.get('text')
            if condition and condition(item):
                reason_text = reason_template.format(**item) if reason_template else ""
                found_reasons.append(reason_text)
        except (KeyError, TypeError, ValueError) as exc:
            logger.warning("LM screens: ошибка построения причины (%s)", exc)
            continue
        except Exception:
            logger.exception("LM screens: непредвиденная ошибка правила")
            raise
                
    if action_type == "followup":
        source_bits: List[str] = []
        reason_codes = set((meta_dict or {}).get("reason_codes") or [])
        if "OPERATOR_WILL_CLARIFY" in reason_codes:
            source_bits.append("оператор обещал уточнить")
        refusal_group = item.get("refusal_group")
        if refusal_group:
            source_bits.append(f"группа отказа: {refusal_group}")
        result_text = str(item.get("result") or "").strip()
        if result_text:
            snippet = result_text[:120]
            if len(result_text) > 120:
                snippet = snippet.rstrip() + "…"
            source_bits.append(f"result: {snippet}")
        if source_bits:
            found_reasons.append("Источник: " + " | ".join(source_bits))

    reasons = "; ".join(found_reasons) if found_reasons else "другие критерии"
    
    mapping = {
        "followup": "followup_needed_flag",
        "complaints": "complaint_risk_flag",
        "lost": "lost_opportunity_score",
        "churn": "churn_risk_level"
    }
    
    conf_key = mapping.get(action_type)
    action_text = METRIC_CONFIG.get(conf_key, {}).get("action_text", "Разобрать кейс.")
    if action_type == "followup":
        sla_hours = None
        if meta_dict:
            sla_hours = meta_dict.get("sla_hours")
        sla_value = int(sla_hours) if isinstance(sla_hours, (int, float)) else 24
        action_text = f"{action_text} (SLA ≤ {sla_value} ч.)"
    
    return reasons, action_text


def _shorten_text(value: Optional[str], limit: int = 220) -> str:
    """Обрезает текст до указанной длины для списков LM."""
    if not value:
        return "—"
    text = str(value).strip()
    if len(text) <= limit:
        return text
    trimmed = text[: limit - 1].rstrip()
    return trimmed + "…"


def _extract_operator_name(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    name = raw.strip()
    if not name:
        return None
    digits = re.sub(r"\D+", "", name)
    if digits and len(digits) >= 7:
        return None
    return name
