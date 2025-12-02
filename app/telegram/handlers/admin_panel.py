"""
Главный обработчик админ-панели.

Предоставляет точку входа /admin и основное меню.
"""

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    Application
)

from app.db.repositories.admin import AdminRepository
from app.telegram.middlewares.permissions import PermissionsManager
from app.logging_config import get_watchdog_logger
from app.utils.error_handlers import log_async_exceptions

logger = get_watchdog_logger(__name__)


class AdminPanelHandler:
    """Основной хендлер админ-панели."""
    
    def __init__(
        self,
        admin_repo: AdminRepository,
        permissions: PermissionsManager
    ):
        self.admin_repo = admin_repo
        self.permissions = permissions
    
    @log_async_exceptions
    async def admin_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /admin - вход в админ-панель."""
        user = update.effective_user
        
        # Проверка прав доступа
        has_access = await self.permissions.can_access_admin_panel(
            user.id, user.username
        )
        
        if not has_access:
            await update.message.reply_text(
                "❌ У вас нет доступа к админ-панели.\n"
                "Требуется роль администратора."
            )
            return
        
        # Показываем главное меню
        await self._show_main_menu(update, context)
    
    async def _show_main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отображает главное меню админ-панели."""
        keyboard = [
            [
                InlineKeyboardButton("📊 Dashboard", callback_data="admin_dashboard"),
                InlineKeyboardButton("👥 Операторы", callback_data="admin_users")
            ],
            [
                InlineKeyboardButton("👑 Администраторы", callback_data="admin_admins"),
                InlineKeyboardButton("📈 Статистика", callback_data="admin_stats")
            ],
            [
                InlineKeyboardButton("📂 Расшифровки", callback_data="admin_lookup"),
                InlineKeyboardButton("⚙️ Настройки", callback_data="admin_settings")
            ]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message_text = (
            "👑 <b>Админ-панель</b>\n\n"
            "Выберите раздел для управления:"
        )
        
        # Если это callback, редактируем сообщение
        if update.callback_query:
            await update.callback_query.edit_message_text(
                text=message_text,
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
        else:
            await update.message.reply_text(
                text=message_text,
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
    
    @log_async_exceptions
    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Роутер для callback-запросов админ-панели."""
        query = update.callback_query
        await query.answer()
        
        data = query.data
        
        if data == "admin_back":
            await self._show_main_menu(update, context)
        elif data == "admin_dashboard":
            await self._show_dashboard(update, context)
        elif data == "admin_settings":
            await query.edit_message_text(
                "⚙️ Настройки в разработке"
            )
    
    async def _show_dashboard(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показывает dashboard с основными метриками."""
        query = update.callback_query
        
        # Получаем статистику
        pending_count = len(await self.admin_repo.get_pending_users())
        all_admins = await self.admin_repo.get_admins()
        
        message = (
            f"📊 <b>Dashboard</b>\n\n"
            f"👥 Ожидают утверждения: <b>{pending_count}</b>\n"
            f"👑 Администраторов: <b>{len(all_admins)}</b>\n\n"
            f"Последние действия:\n"
            f"<i>Скоро будет доступно</i>"
        )
        
        keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data="admin_back")]]
        
        await query.edit_message_text(
            text=message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )


def register_admin_panel_handlers(
    application: Application,
    admin_repo: AdminRepository,
    permissions: PermissionsManager
):
    """Регистрирует хендлеры админ-панели."""
    handler = AdminPanelHandler(admin_repo, permissions)
    
    # Команда /admin
    application.add_handler(CommandHandler("admin", handler.admin_command))
    
    # Callback handlers
    application.add_handler(
        CallbackQueryHandler(
            handler.handle_callback,
            pattern="^admin_(back|dashboard|settings)$"
        )
    )
    
    logger.info("Admin panel handlers registered")
