"""
Единая админ-панель с модульной структурой.

ВАЖНО: Сохраняет ВСЮ существующую логику отчётов, метрик, поиска.
Просто предоставляет красивый интерфейс вместо помойки команд.
"""

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, CommandHandler, CallbackQueryHandler

from app.db.repositories.admin import AdminRepository
from app.db.repositories.operators import OperatorRepository
from app.telegram.middlewares.permissions import PermissionsManager
from app.services.notifications import NotificationService
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)


class AdminMenu:
    """Единая модульная админ-панель."""
    
    def __init__(
        self,
        admin_repo: AdminRepository,
        operator_repo: OperatorRepository,
        permissions: PermissionsManager,
        notifications: NotificationService
    ):
        self.admin_repo = admin_repo
        self.operator_repo = operator_repo
        self.permissions = permissions
        self.notifications = notifications
    
    async def show_main_panel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показывает главную админ-панель."""
        user = update.effective_user
        
        # Проверка прав
        if not await self.permissions.is_admin(user.id, user.username):
            await update.message.reply_text("⛔ У вас нет доступа к админ-панели")
            return
        
        keyboard = [
            [
                InlineKeyboardButton("📊 Dashboard", callback_data="admin:dashboard"),
                InlineKeyboardButton("👥 Пользователи", callback_data="admin:users")
            ],
            [
                InlineKeyboardButton("👑 Администраторы", callback_data="admin:admins"),
                InlineKeyboardButton("📈 Статистика", callback_data="admin:stats")
            ],
            [
                InlineKeyboardButton("🔍 Поиск звонка", callback_data="admin:lookup"),
                 InlineKeyboardButton("🧠 LM Метрики", callback_data="admin:lm:menu")
            ],
            [InlineKeyboardButton("⚙️ Настройки", callback_data="admin:settings")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            "👑 <b>Админ-панель</b>\n\n"
            "Выберите раздел:"
        )
        
        if update.message:
            await update.message.reply_text(text, reply_markup=reply_markup, parse_mode='HTML')
        elif update.callback_query:
            await update.callback_query.edit_message_text(
                text, reply_markup=reply_markup, parse_mode='HTML'
            )
    
    async def show_dashboard(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показывает Dashboard с основной статистикой."""
        query = update.callback_query
        await query.answer()
        
        try:
            # Получаем статистику
            counters = await self.admin_repo.get_users_counters()
            
            total = counters.get('total_users', 0)
            pending = counters.get('pending_count', 0)
            approved = counters.get('approved_count', 0)
            blocked = counters.get('blocked_count', 0)
            admins = counters.get('admin_count', 0)
            
            text = (
                "📊 <b>LIVE Dashboard</b>\n\n"
                f"👥 Всего пользователей: <b>{total}</b>\n"
                f"├─ ⏳ Pending: {pending}\n"
                f"├─ ✅ Approved: {approved}\n"
                f"└─ 🚫 Blocked: {blocked}\n\n"
                f"👑 Администраторов: <b>{admins}</b>\n\n"
                "Быстрые действия:"
            )
            
            keyboard = []
            
            if pending > 0:
                keyboard.append([
                    InlineKeyboardButton(
                        f"⚡ Pending пользователи ({pending})", 
                        callback_data="admin:users:list:pending"
                    )
                ])
            
            keyboard.append([
                InlineKeyboardButton("📈 Статистика 7д", callback_data="admin:stats:7")
            ])
            keyboard.append([
                InlineKeyboardButton("◀️ Назад", callback_data="admin:back")
            ])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='HTML')
            
        except Exception as e:
            logger.error(f"Error in show_dashboard: {e}", exc_info=True)
            await query.edit_message_text(
                "❌ Ошибка при загрузке Dashboard",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("◀️ Назад", callback_data="admin:back")
                ]])
            )
    
    async def show_users_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показывает меню управления пользователями."""
        query = update.callback_query
        await query.answer()
        
        try:
            counters = await self.admin_repo.get_users_counters()
            
            pending = counters.get('pending_count', 0)
            approved = counters.get('approved_count', 0)
            blocked = counters.get('blocked_count', 0)
            
            text = (
                "👥 <b>Управление пользователями</b>\n\n"
                "Статистика:\n"
                f"⏳ Pending: {pending} | ✅ Approved: {approved} | 🚫 Blocked: {blocked}"
            )
            
            keyboard = [
                [InlineKeyboardButton(f"📋 Pending ({pending})", callback_data="admin:users:list:pending")],
                [InlineKeyboardButton(f"✅ Одобренные ({approved})", callback_data="admin:users:list:approved")],
                [InlineKeyboardButton(f"🚫 Заблокированные ({blocked})", callback_data="admin:users:list:blocked")],
                [InlineKeyboardButton("◀️ Назад", callback_data="admin:back")]
            ]
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='HTML')
            
        except Exception as e:
            logger.error(f"Error in show_users_menu: {e}", exc_info=True)
            await query.edit_message_text(
                "❌ Ошибка при загрузке меню пользователей",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("◀️ Назад", callback_data="admin:back")
                ]])
            )
    
    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Роутер для всех admin: callback."""
        query = update.callback_query
        data = query.data
        
        if data == "admin:back":
            await self.show_main_panel(update, context)
        elif data == "admin:dashboard":
            await self.show_dashboard(update, context)
        elif data == "admin:users":
            await self.show_users_menu(update, context)
        elif data.startswith("admin:users:list:"):
            # Делегируем в существующий admin_users.py
            from app.telegram.handlers.admin_users import handle_users_list_callback
            await handle_users_list_callback(update, context)
        elif data == "admin:admins":
            # Делегируем в существующий admin_admins.py
            from app.telegram.handlers.admin_admins import handle_admins_menu
            await handle_admins_menu(update, context)
        elif data.startswith("admin:stats"):
            # Делегируем в существующий admin_stats.py
            from app.telegram.handlers.admin_stats import handle_stats_callback
            await handle_stats_callback(update, context)
        elif data == "admin:lookup":
            # Делегируем в call_lookup
            await query.answer("🔍 Функция поиска звонка")
            await query.edit_message_text(
                "🔍 <b>Поиск звонка</b>\n\n"
                "Используйте команду /call_lookup <номер>\n"
                "Формат: +7XXXXXXXXXX или XXXXXXXXXX",
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("◀️ Назад", callback_data="admin:back")
                ]])
            )
        elif data == "admin:settings":
            await query.answer("⚙️ Раздел в разработке")
            await query.edit_message_text(
                "⚙️ <b>Настройки системы</b>\n\n"
                "Раздел в разработке",
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("◀️ Назад", callback_data="admin:back")
                ]])
            )
        else:
            await query.answer("❌ Неизвестная команда")


def register_admin_menu_handlers(application, admin_menu: AdminMenu):
    """Регистрирует обработчики единой админ-панели."""
    application.add_handler(CommandHandler("admin", admin_menu.show_main_panel))
    application.add_handler(CallbackQueryHandler(
        admin_menu.handle_callback,
        pattern="^admin:"
    ))
