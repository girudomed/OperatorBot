"""
Хендлер статистики для админ-панели.
"""

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackQueryHandler, ContextTypes, Application

from app.db.repositories.admin import AdminRepository
from app.services.metrics_service import MetricsService
from app.telegram.middlewares.permissions import PermissionsManager
from app.logging_config import get_watchdog_logger
from app.utils.error_handlers import log_async_exceptions

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
        await query.answer()
        
        # Получаем базовую статистику
        pending_users = await self.admin_repo.get_pending_users()
        all_admins = await self.admin_repo.get_admins()
        
        # Получаем метрики качества
        try:
            quality_summary = await self.metrics.calculate_quality_summary(period='weekly')
        except Exception as e:
            logger.error(f"Failed to get quality summary: {e}")
            quality_summary = {}
        
        message = (
            f"📈 <b>Статистика системы</b>\n\n"
            f"<b>Пользователи:</b>\n"
            f"⏳ Ожидают утверждения: {len(pending_users)}\n"
            f"👑 Администраторов: {len(all_admins)}\n\n"
            f"<b>Качество (неделя):</b>\n"
            f"📞 Всего звонков: {quality_summary.get('total_calls', 0)}\n"
            f"❌ Пропущено: {quality_summary.get('missed_calls', 0)} "
            f"({quality_summary.get('missed_rate', 0):.1f}%)\n"
            f"⭐ Средний скор: {quality_summary.get('avg_score', 0):.1f}\n"
            f"🎯 Лидов: {quality_summary.get('total_leads', 0)}\n"
            f"✅ Конверсия: {quality_summary.get('lead_conversion', 0):.1f}%\n"
        )
        
        keyboard = [
            [InlineKeyboardButton("🔄 Обновить", callback_data="admin:stats")],
            [InlineKeyboardButton("◀️ Назад", callback_data="admin:back")]
        ]
        
        await query.edit_message_text(
            text=message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )


def register_admin_stats_handlers(
    application: Application,
    admin_repo: AdminRepository,
    metrics_service: MetricsService,
    permissions: PermissionsManager
):
    """Регистрирует хендлеры статистики."""
    handler = AdminStatsHandler(admin_repo, metrics_service, permissions)
    
    application.add_handler(
        CallbackQueryHandler(handler.show_stats, pattern=r"^admin:stats$")
    )
    
    logger.info("Admin stats handlers registered")
