# Файл: app/telegram/handlers/help.py

"""
Обновленный /help handler с блочной структурой по ролям.

Краткие блоки команд, NO spam, role-based контент.
"""

from telegram import Update
from telegram.ext import ContextTypes, CommandHandler

from app.db.manager import DatabaseManager
from app.db.repositories.users import UserRepository
from app.db.repositories.roles import RolesRepository
from app.telegram.middlewares.permissions import PermissionsManager
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)


class HelpHandler:
    """Handler для команды /help с role-based контентом."""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.user_repo = UserRepository(db_manager)
        self.roles_repo = RolesRepository(db_manager)
        self.permissions = PermissionsManager(db_manager)
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Команда /help - справка с блочной структурой.
        """
        user_id = update.effective_user.id
        username = update.effective_user.username
        message = update.effective_message
        
        logger.info(f"[HELP] Command from {user_id}")
        
        # Получить роль
        user = await self.user_repo.get_user_by_telegram_id(user_id)
        
        if not user or user.get('status') != 'approved':
            if message:
                await message.reply_text(
                    "ℹ️ **Справка**\n\n"
                    "Для доступа к функциям бота необходимо:\n"
                    "1. Зарегистрироваться: /register\n"
                    "2. Дождаться одобрения администратора\n\n"
                    "После одобрения вам будут доступны функции в зависимости от роли.",
                    parse_mode='Markdown'
                )
            return
        
        role_id = user.get('role_id', 1)
        perms = await self.roles_repo.get_user_permissions(role_id)
        is_supreme = self.permissions.is_supreme_admin(user_id, username)
        is_dev = self.permissions.is_dev_admin(user_id, username)
        
        # Построить help в зависимости от прав
        help_text = "📋 **Справка**\n\n"
        
        # Блок Отчёты
        if perms.get('can_view_own_stats') or perms.get('can_view_all_stats'):
            help_text += "📊 **Отчёты:**\n"
            if perms.get('can_view_all_stats'):
                help_text += (
                    "  • Еженедельный отчёт — автоматическая сводка\n"
                    "  • Отчёт за период — выберите даты\n"
                    "  • По оператору — детальная статистика\n"
                    "  • Сводка по всем — общий обзор\n"
                )
            else:
                help_text += (
                    "  • Моя статистика — ваши показатели\n"
                    "  • Отчёт за период — выберите даты\n"
                )
            help_text += "\n"
        
        # Блок Поиск
        help_text += "🔍 **Поиск звонков:**\n"
        help_text += (
            "  • По номеру телефона\n"
            "  • По дате или интервалу\n"
            "  • По оператору (если доступ есть)\n"
            "  • Последние звонки\n\n"
        )
        
        # Блок Управление (только для админов)
        if perms.get('can_manage_users'):
            help_text += "👥 **Управление пользователями:**\n"
            help_text += (
                "  • Одобрение заявок\n"
                "  • Изменение ролей\n"
                "  • Блокировка/разблокировка\n"
                "  • Просмотр списков\n\n"
            )
        
        # Блок Система (только для SuperAdmin/Dev)
        if is_supreme or is_dev or perms.get('can_debug'):
            help_text += "⚙️ **Системные функции:**\n"
            help_text += (
                "  • `/sync_analytics` — синхронизация БД\n"
                "  • Диагностика состояния\n"
                "  • Просмотр логов ошибок\n"
                "  • Проверка подключений\n\n"
            )
        
        # Футер
        help_text += (
            "💡 **Совет:** Используйте кнопки меню для\n"
            "удобной навигации по функциям.\n\n"
            "❓ Вопросы? Обратитесь к администратору."
        )
        
        if message:
            await message.reply_text(help_text, parse_mode='Markdown')
        
        logger.info(f"[HELP] Sent help for {user_id}, role_id={role_id}")
    
    def get_handler(self):
        """Получить CommandHandler для регистрации."""
        return CommandHandler('help', self.help_command)
