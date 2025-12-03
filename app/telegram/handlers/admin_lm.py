"""
Хендлер для отображения LM метрик в админ-панели.

Показывает:
- Operational метрики (скорость, эффективность)
- Conversion метрики (конверсия, потери, cross-sell)
- Quality метрики (покрытие чек-листа, скор, риски скрипта)
- Risk метрики (отток, жалобы, фоллоу-ап)
- Forecast метрики (прогноз конверсии, повторных звонков, жалоб)
"""

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, Application, CallbackQueryHandler

from app.db.repositories.lm_repository import LMRepository
from app.telegram.middlewares.permissions import PermissionsManager
from app.telegram.utils.messages import safe_edit_message
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)


class AdminLMHandler:
    """Хендлер для показа LM метрик."""
    
    def __init__(
        self,
        lm_repo: LMRepository,
        permissions: PermissionsManager
    ):
        self.lm_repo = lm_repo
        self.permissions = permissions
    
    async def show_lm_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показывает главное меню LM метрик."""
        query = update.callback_query
        await query.answer()
        
        text = (
            "🧠 <b>LM Метрики и Прогнозирование</b>\n\n"
            "Выберите категорию метрик:"
        )
        
        keyboard = [
            [
                InlineKeyboardButton("⚡ Операционные", callback_data="admin:lm:operational"),
                InlineKeyboardButton("💰 Конверсии", callback_data="admin:lm:conversion")
            ],
            [
                InlineKeyboardButton("⭐ Качество", callback_data="admin:lm:quality"),
                InlineKeyboardButton("⚠️ Риски", callback_data="admin:lm:risk")
            ],
            [
                InlineKeyboardButton("🔮 Прогнозы", callback_data="admin:lm:forecast"),
                InlineKeyboardButton("📊 Сводка", callback_data="admin:lm:summary")
            ],
            [InlineKeyboardButton("◀️ Назад", callback_data="admin:back")]
        ]
        
        await safe_edit_message(
            query,
            text=text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    
    async def show_operational_metrics(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показывает операционные метрики."""
        query = update.callback_query
        await query.answer()
        
        try:
            # Получаем агрегированные данные
            metrics = await self.lm_repo.get_aggregated_metrics(
                metric_group='operational',
                days=7
            )
            
            text = (
                "⚡ <b>Операционные метрики (7 дней)</b>\n\n"
                "<b>Скорость реакции:</b>\n"
                f"└ Средний скор: {metrics.get('response_speed_score', {}).get('avg', 0):.1f}/100\n\n"
                "<b>Эффективность разговора:</b>\n"
                f"└ Средний скор: {metrics.get('talk_time_efficiency', {}).get('avg', 0):.1f}/100\n\n"
                "<b>Влияние на очередь:</b>\n"
                f"└ Средний индекс: {metrics.get('queue_impact_index', {}).get('avg', 0):.1f}/100\n\n"
                "<i>Метрики рассчитываются автоматически для каждого звонка</i>"
            )
        except Exception as e:
            logger.error(f"Error loading operational metrics: {e}", exc_info=True)
            text = (
                "⚡ <b>Операционные метрики</b>\n\n"
                "❌ Ошибка загрузки данных\n\n"
                "<i>Проверьте, что LM воркер запущен и данные рассчитаны</i>"
            )
        
        keyboard = [
            [InlineKeyboardButton("◀️ К категориям", callback_data="admin:lm:menu")]
        ]
        
        await safe_edit_message(
            query,
            text=text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    
    async def show_conversion_metrics(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показывает конверсионные метрики."""
        query = update.callback_query
        await query.answer()
        
        try:
            metrics = await self.lm_repo.get_aggregated_metrics(
                metric_group='conversion',
                days=7
            )
            
            text = (
                "💰 <b>Конверсионные метрики (7 дней)</b>\n\n"
                "<b>Скор конверсии:</b>\n"
                f"└ Средний: {metrics.get('conversion_score', {}).get('avg', 0):.1f}/100\n\n"
                "<b>Потерянные возможности:</b>\n"
                f"└ Скор потерь: {metrics.get('lost_opportunity_score', {}).get('avg', 0):.1f}/100\n\n"
                "<b>Cross-sell потенциал:</b>\n"
                f"└ Средний: {metrics.get('cross_sell_potential', {}).get('avg', 0):.1f}/100\n\n"
                "<i>100 = максимальная конверсия, 0 = нет конверсии</i>"
            )
        except Exception as e:
            logger.error(f"Error loading conversion metrics: {e}", exc_info=True)
            text = (
                "💰 <b>Конверсионные метрики</b>\n\n"
                "❌ Ошибка загрузки данных"
            )
        
        keyboard = [
            [InlineKeyboardButton("◀️ К категориям", callback_data="admin:lm:menu")]
        ]
        
        await safe_edit_message(
            query,
            text=text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    
    async def show_forecast_metrics(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показывает прогнозные метрики."""
        query = update.callback_query
        await query.answer()
        
        try:
            metrics = await self.lm_repo.get_aggregated_metrics(
                metric_group='forecast',
                days=7
            )
            
            conv_prob = metrics.get('conversion_prob_forecast', {}).get('avg', 0)
            second_call = metrics.get('second_call_prob', {}).get('avg', 0)
            complaint = metrics.get('complaint_prob', {}).get('avg', 0)
            
            text = (
                "🔮 <b>Прогнозные метрики (7 дней)</b>\n\n"
                "<b>Прогноз конверсии:</b>\n"
                f"└ Вероятность: {conv_prob*100:.1f}%\n"
                f"└ Статус: {'🟢 Высокая' if conv_prob > 0.5 else '🟡 Средняя' if conv_prob > 0.2 else '🔴 Низкая'}\n\n"
                "<b>Повторный звонок:</b>\n"
                f"└ Вероятность: {second_call*100:.1f}%\n\n"
                "<b>Риск жалобы:</b>\n"
                f"└ Вероятность: {complaint*100:.1f}%\n"
                f"└ Статус: {'🔴 Высокий' if complaint > 0.3 else '🟡 Средний' if complaint > 0.1 else '🟢 Низкий'}\n\n"
                "<i>Прогнозы рассчитываются на основе LM моделей</i>"
            )
        except Exception as e:
            logger.error(f"Error loading forecast metrics: {e}", exc_info=True)
            text = (
                "🔮 <b>Прогнозные метрики</b>\n\n"
                "❌ Ошибка загрузки данных"
            )
        
        keyboard = [
            [InlineKeyboardButton("◀️ К категориям", callback_data="admin:lm:menu")]
        ]
        
        await safe_edit_message(
            query,
            text=text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    
    async def show_risk_metrics(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показывает метрики рисков."""
        query = update.callback_query
        await query.answer()
        
        try:
            # Получаем данные о рисках
            risks = await self.lm_repo.get_risk_summary(days=7)
            
            churn_high = risks.get('churn_risk_high', 0)
            churn_medium = risks.get('churn_risk_medium', 0)
            complaint_count = risks.get('complaint_risk_count', 0)
            followup_count = risks.get('followup_needed_count', 0)
            
            text = (
                "⚠️ <b>Метрики рисков (7 дней)</b>\n\n"
                "<b>Риск оттока клиентов:</b>\n"
                f"└ Высокий: {churn_high} звонков\n"
                f"└ Средний: {churn_medium} звонков\n\n"
                "<b>Риск жалоб:</b>\n"
                f"└ Звонков с риском: {complaint_count}\n\n"
                "<b>Требуется фоллоу-ап:</b>\n"
                f"└ Звонков: {followup_count}\n\n"
                "<i>Рекомендуется связаться с клиентами из группы риска</i>"
            )
        except Exception as e:
            logger.error(f"Error loading risk metrics: {e}", exc_info=True)
            text = (
                "⚠️ <b>Метрики рисков</b>\n\n"
                "❌ Ошибка загрузки данных"
            )
        
        keyboard = [
            [InlineKeyboardButton("🔍 Список фоллоу-апов", callback_data="admin:lm:followup_list")],
            [InlineKeyboardButton("◀️ К категориям", callback_data="admin:lm:menu")]
        ]
        
        await safe_edit_message(
            query,
            text=text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    
    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Роутер для LM callback."""
        query = update.callback_query
        data = query.data
        
        if data == "admin:lm:menu":
            await self.show_lm_menu(update, context)
        elif data == "admin:lm:operational":
            await self.show_operational_metrics(update, context)
        elif data == "admin:lm:conversion":
            await self.show_conversion_metrics(update, context)
        elif data == "admin:lm:forecast":
            await self.show_forecast_metrics(update, context)
        elif data == "admin:lm:risk":
            await self.show_risk_metrics(update, context)
        elif data == "admin:lm:quality":
            await query.answer("⭐ Раздел в разработке")
        elif data == "admin:lm:summary":
            await query.answer("📊 Раздел в разработке")
        elif data == "admin:lm:followup_list":
            await query.answer("🔍 Раздел в разработке")
        else:
            await query.answer("❌ Неизвестная команда")


def register_admin_lm_handlers(
    application: Application,
    lm_repo: LMRepository,
    permissions: PermissionsManager
):
    """Регистрирует LM хендлеры."""
    handler = AdminLMHandler(lm_repo, permissions)
    
    application.add_handler(
        CallbackQueryHandler(handler.handle_callback, pattern=r"^admin:lm:")
    )
    
    logger.info("Admin LM handlers registered")
