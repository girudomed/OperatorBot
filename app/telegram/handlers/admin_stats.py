# Файл: app/telegram/handlers/admin_stats.py

"""
Хендлер статистики для админ-панели.
"""

from datetime import datetime, timedelta
from typing import Dict, Any, Optional

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest
from telegram.ext import ContextTypes, Application

from app.db.repositories.admin import AdminRepository
from app.services.metrics_service import MetricsService
from app.telegram.middlewares.permissions import PermissionsManager
from app.logging_config import get_watchdog_logger
from app.utils.error_handlers import log_async_exceptions
from app.utils.rate_limit import rate_limit_hit
from app.telegram.utils.messages import safe_edit_message
from app.telegram.utils.callback_data import AdminCB
from app.telegram.utils.admin_registry import register_admin_callback_handler

logger = get_watchdog_logger(__name__)


class AdminStatsHandler:
    """Статистика и метрики для админов."""
    
    def __init__(
        self,
        admin_repo: AdminRepository,
        metrics_service: MetricsService,
        permissions: PermissionsManager
    ):
        self.admin_repo = admin_repo
        self.metrics = metrics_service
        self.permissions = permissions
    
    @log_async_exceptions
    async def show_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показывает выбор периода статистики или конкретный период."""
        query = update.callback_query
        user = update.effective_user
        if not query or not user:
            return

        allowed = await self.permissions.can_view_all_stats(user.id, user.username)
        if not allowed:
            try:
                await query.answer("Недостаточно прав", show_alert=True)
            except BadRequest:
                pass
            logger.warning(
                "Denied admin stats access for user_id=%s username=%s",
                user.id,
                user.username,
            )
            return

        action, args = AdminCB.parse(query.data or "")
        sub_action = args[0] if args else None

        user_id = user.id
        if user_id and rate_limit_hit(
            context.application.bot_data,
            user_id,
            "admin_stats",
            cooldown_seconds=1.5,
        ):
            try:
                await query.answer("Слишком часто обновляете статистику. Подождите.", show_alert=True)
            except BadRequest:
                pass
            return
        try:
            await query.answer()
        except BadRequest:
            pass
        if sub_action == "period" and len(args) > 1:
            await self._show_period_summary(query, period_key=args[1])
            return

        pending_count = await self._safe_count(self.admin_repo.get_pending_users)
        admin_count = await self._safe_count(self.admin_repo.get_admins)
        period_previews = await self._collect_period_previews()

        await self._show_period_picker(
            query,
            pending_count=pending_count,
            admin_count=admin_count,
            period_previews=period_previews,
        )

    async def _show_period_picker(
        self,
        query,
        *,
        pending_count: int = 0,
        admin_count: int = 0,
        period_previews: Optional[Dict[str, Optional[Dict[str, Any]]]] = None,
    ) -> None:
        text = (
            "📈 <b>Статистика системы</b>\n"
            "Выберите интересующий период, чтобы открыть детализацию качества."
        )
        preview_lines = []
        for label, key, _ in self._period_configs():
            summary = (period_previews or {}).get(key)
            if summary:
                preview_lines.append(
                    f"{label}: {summary.get('total_calls', 0)} звонков, ⭐ {summary.get('avg_score', 0):.1f}"
                )
            else:
                preview_lines.append(f"{label}: нет данных")
        if preview_lines:
            text += "\n\n" + "\n".join(preview_lines)
        text += f"\n\n👥 Админов: {admin_count}\n⏳ Заявок в очереди: {pending_count}"
        keyboard = []
        row: list[InlineKeyboardButton] = []
        for idx, (label, key, _) in enumerate(self._period_configs()):
            row.append(
                InlineKeyboardButton(
                    label,
                    callback_data=AdminCB.create(AdminCB.STATS, "period", key),
                )
            )
            if len(row) == 2:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data=AdminCB.create(AdminCB.BACK))])
        await safe_edit_message(
            query,
            text=text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="HTML",
        )

    async def _show_period_summary(self, query, *, period_key: str) -> None:
        config = next((cfg for cfg in self._period_configs() if cfg[1] == period_key), None)
        if not config:
            try:
                await query.answer("Неизвестный период", show_alert=True)
            except BadRequest:
                pass
            return
        label, _, days = config
        today = datetime.now().date()
        start_date = today if days == 1 else today - timedelta(days=days - 1)
        try:
            summary = await self.metrics.calculate_quality_summary(
                start_date=start_date.isoformat(),
                end_date=today.isoformat(),
            )
            text = self._format_quality_summary(label, summary)
        except Exception as exc:
            logger.error("Failed to calculate quality summary for %s: %s", label, exc)
            text = f"{label}: данные временно недоступны."
        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "🔄 Обновить",
                        callback_data=AdminCB.create(AdminCB.STATS, "period", period_key),
                    )
                ],
                [InlineKeyboardButton("◀️ Назад", callback_data=AdminCB.create(AdminCB.STATS))],
            ]
        )
        await safe_edit_message(
            query,
            text=text,
            reply_markup=keyboard,
            parse_mode="HTML",
        )

    def _period_configs(self):
        return [
            ("24 ч", "24h", 1),
            ("7 дней", "7d", 7),
            ("14 дней", "14d", 14),
            ("30 дней", "30d", 30),
            ("180 дней", "180d", 180),
        ]

    def _format_quality_summary(self, label: str, summary: dict) -> str:
        start_label = summary.get("start_date")
        end_label = summary.get("end_date")
        try:
            start_fmt = datetime.fromisoformat(start_label).strftime("%d.%m.%Y") if start_label else "?"
            end_fmt = datetime.fromisoformat(end_label).strftime("%d.%m.%Y") if end_label else "?"
        except ValueError:
            start_fmt = start_label or "?"
            end_fmt = end_label or "?"
        lines = [
            f"{label} ({start_fmt} — {end_fmt}):",
            f"📞 Всего звонков: {summary.get('total_calls', 0)}",
            f"❌ Пропущено: {summary.get('missed_calls', 0)} ({summary.get('missed_rate', 0):.1f}%)",
            f"⭐ Средний скор: {summary.get('avg_score', 0):.1f}",
            f"🎯 Лиды / Записи: {summary.get('booked_leads', 0)}",
            f"🟡 Лиды без записи: {summary.get('leads_no_record', 0)}",
            f"✅ Конверсия: {summary.get('lead_conversion', 0):.1f}%",
            f"♻️ Отмен: {summary.get('cancellations', 0)}",
        ]
        return "\n".join(lines)

    async def _safe_count(self, coro_factory) -> int:
        try:
            items = await coro_factory()
            if items is None:
                return 0
            return len(items)
        except Exception as exc:
            logger.error("Failed to fetch admin stats count: %s", exc)
            return 0

    async def _collect_period_previews(self) -> Dict[str, Optional[Dict[str, Any]]]:
        previews: Dict[str, Optional[Dict[str, Any]]] = {}
        today = datetime.now().date()
        for _, key, days in self._period_configs():
            start_date = today if days == 1 else today - timedelta(days=days - 1)
            try:
                previews[key] = await self.metrics.calculate_quality_summary(
                    start_date=start_date.isoformat(),
                    end_date=today.isoformat(),
                )
            except Exception as exc:
                logger.warning("Failed to load preview for %s: %s", key, exc)
                previews[key] = None
        return previews


def register_admin_stats_handlers(
    application: Application,
    admin_repo: AdminRepository,
    metrics_service: MetricsService,
    permissions: PermissionsManager
):
    """Регистрирует хендлеры статистики."""
    handler = AdminStatsHandler(admin_repo, metrics_service, permissions)
    register_admin_callback_handler(application, AdminCB.STATS, handler.show_stats)
    logger.info("Admin stats handlers registered")
