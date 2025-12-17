# Файл: app/telegram/handlers/admin_stats.py

"""
Хендлер статистики для админ-панели.
"""

from datetime import datetime, timedelta

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackQueryHandler, ContextTypes, Application

from app.db.repositories.admin import AdminRepository
from app.services.metrics_service import MetricsService
from app.telegram.middlewares.permissions import PermissionsManager
from app.logging_config import get_watchdog_logger
from app.utils.error_handlers import log_async_exceptions
from app.utils.rate_limit import rate_limit_hit
from app.telegram.utils.messages import safe_edit_message
from app.telegram.utils.callback_data import AdminCB

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
        """Показывает общую статистику системы."""
        query = update.callback_query
        user = update.effective_user
        user_id = user.id if user else 0
        if user_id and rate_limit_hit(
            context.application.bot_data,
            user_id,
            "admin_stats",
            cooldown_seconds=2.0,
        ):
            await query.answer("Слишком часто обновляете статистику. Подождите пару секунд.", show_alert=True)
            return
        await query.answer()
        
        # Получаем базовую статистику
        pending_users = await self.admin_repo.get_pending_users()
        all_admins = await self.admin_repo.get_admins()
        
        # Получаем метрики качества по нескольким окнам
        quality_lines = await self._collect_quality_lines()
        
        message = (
            f"📈 <b>Статистика системы</b>\n\n"
            f"<b>Пользователи:</b>\n"
            f"⏳ Ожидают утверждения: {len(pending_users)}\n"
            f"👑 Администраторов: {len(all_admins)}\n\n"
            f"<b>Качество по периодам:</b>\n"
            f"{quality_lines}"
        )
        
        keyboard = [
            [InlineKeyboardButton("🔄 Обновить", callback_data=AdminCB.create(AdminCB.STATS))],
            [InlineKeyboardButton("◀️ Назад", callback_data=AdminCB.create(AdminCB.BACK))]
        ]
        
        await safe_edit_message(
            query,
            text=message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )

    async def _collect_quality_lines(self) -> str:
        today = datetime.now().date()
        period_configs = [
            ("За последние 24 часа", 1),
            ("За 7 дней", 7),
            ("За 14 дней", 14),
            ("За 30 дней", 30),
            ("За 180 дней", 180),
        ]
        blocks = []
        for label, days in period_configs:
            try:
                start_date = today if days == 1 else today - timedelta(days=days - 1)
                summary = await self.metrics.calculate_quality_summary(
                    start_date=start_date.isoformat(),
                    end_date=today.isoformat(),
                )
                blocks.append(self._format_quality_summary(label, summary))
            except Exception as exc:
                logger.error("Failed to calculate quality summary for %s: %s", label, exc)
                blocks.append(f"{label}: данные временно недоступны.")
        return "\n\n".join(blocks)

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


def register_admin_stats_handlers(
    application: Application,
    admin_repo: AdminRepository,
    metrics_service: MetricsService,
    permissions: PermissionsManager
):
    """Регистрирует хендлеры статистики."""
    handler = AdminStatsHandler(admin_repo, metrics_service, permissions)
    
    application.add_handler(
        CallbackQueryHandler(
            handler.show_stats,
            pattern=rf"^{AdminCB.PREFIX}:{AdminCB.STATS}",
        )
    )
    
    logger.info("Admin stats handlers registered")
