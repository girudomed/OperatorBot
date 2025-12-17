# Файл: app/telegram/utils/keyboard_builder.py

"""
Keyboard Builder для создания клавиатур по ролям.

Строит reply и inline клавиатуры на основе прав пользователя.
"""

from telegram import ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton, KeyboardButton
from typing import List, Optional, Dict

from app.db.repositories.roles import RolesRepository
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)


class KeyboardBuilder:
    """Builder для динамических клавиатур по ролям."""
    
    def __init__(self, roles_repo: RolesRepository):
        self.roles_repo = roles_repo
    
    async def build_main_keyboard(
        self,
        role_id: int,
        is_supreme: bool = False,
        is_dev: bool = False,
        perms_override: Optional[Dict[str, bool]] = None,
    ) -> ReplyKeyboardMarkup:
        """
        Построить главную reply клавиатуру на основе роли.
        
        Args:
            role_id: ID роли пользователя
            is_supreme: Supreme Admin флаг
            is_dev: Dev Admin флаг
        
        Returns:
            ReplyKeyboardMarkup с кнопками по правам
        """
        logger.debug(f"[KEYBOARD] Building main keyboard for role_id={role_id}")
        
        perms = perms_override or await self.roles_repo.get_user_permissions(role_id)
        
        keyboard = []
        
        # Все могут видеть свою статистику
        if perms.get('can_view_own_stats'):
            keyboard.append([KeyboardButton("📊 Моя статистика")])
        
        # Отчёты для тех кто может видеть всех
        if perms.get('can_view_all_stats'):
            keyboard.append([
                KeyboardButton("📊 Отчёты"),
                KeyboardButton("🔍 Поиск звонка")
            ])
        
        # Управление пользователями
        if perms.get('can_manage_users'):
            keyboard.append([KeyboardButton("👥 Пользователи и роли")])
        
        if is_supreme or is_dev or perms.get('can_manage_users'):
            keyboard.append([KeyboardButton("👑 Админ-панель")])
        
        # Системные функции только для Dev/SuperAdmin
        if is_supreme or is_dev or perms.get('can_debug'):
            keyboard.append([KeyboardButton("⚙️ Система")])
        
        # Всегда добавляем помощь
        keyboard.append([KeyboardButton("ℹ️ Помощь")])
        keyboard.append([KeyboardButton("📘 Мануал")])
        
        return ReplyKeyboardMarkup(
            keyboard,
            resize_keyboard=True,
            one_time_keyboard=False
        )
    
    def build_reports_menu(self, can_view_all: bool) -> InlineKeyboardMarkup:
        """
        Построить inline меню отчётов.
        
        Args:
            can_view_all: Может ли видеть отчёты по всем операторам
        """
        keyboard = []
        
        if can_view_all:
            # Для руководства/админов
            keyboard.extend([
                [InlineKeyboardButton("📅 Еженедельный отчёт", callback_data="reports_weekly")],
                [InlineKeyboardButton("📆 Отчёт за период", callback_data="reports_period")],
                [InlineKeyboardButton("👤 Отчёт по оператору", callback_data="reports_operator")],
                [InlineKeyboardButton("📊 Сводка по всем", callback_data="reports_all")],
            ])
        else:
            # Для операторов
            keyboard.extend([
                [InlineKeyboardButton("📅 Мой отчёт за неделю", callback_data="reports_my_week")],
                [InlineKeyboardButton("📆 Мой отчёт за период", callback_data="reports_my_period")],
            ])
        
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="main_menu")])
        
        return InlineKeyboardMarkup(keyboard)
    
    def build_call_lookup_menu(self) -> InlineKeyboardMarkup:
        """Построить меню поиска звонков."""
        keyboard = [
            [InlineKeyboardButton("📞 По номеру телефона", callback_data="lookup_phone")],
            [InlineKeyboardButton("📅 По дате/интервалу", callback_data="lookup_date")],
            [InlineKeyboardButton("👤 По оператору", callback_data="lookup_operator")],
            [InlineKeyboardButton("🕐 Последние 10 звонков", callback_data="lookup_recent")],
            [InlineKeyboardButton("◀️ Назад", callback_data="main_menu")],
        ]
        return InlineKeyboardMarkup(keyboard)
    
    def build_users_management_menu(self, pending_count: int = 0) -> InlineKeyboardMarkup:
        """
        Построить меню управления пользователями.
        
        Args:
            pending_count: Количество пользователей ожидающих одобрения
        """
        pending_text = f"⏳ Ожидают одобрения ({pending_count})" if pending_count > 0 else "⏳ Ожидают одобрения"
        
        keyboard = [
            [InlineKeyboardButton(pending_text, callback_data="users_pending")],
            [InlineKeyboardButton("📋 Список пользователей", callback_data="users_list")],
            [InlineKeyboardButton("👑 Список админов", callback_data="users_admins")],
            [InlineKeyboardButton("🔄 Изменить роль", callback_data="users_change_role")],
            [InlineKeyboardButton("◀️ Назад", callback_data="main_menu")],
        ]
        return InlineKeyboardMarkup(keyboard)
    
    def build_system_menu(self, include_cache_reset: bool = False) -> InlineKeyboardMarkup:
        """Построить системное меню (только для привилегированных пользователей)."""
        keyboard = [
            [InlineKeyboardButton("🔍 Состояние бота", callback_data="system_status")],
            [InlineKeyboardButton("❌ Последние ошибки", callback_data="system_errors")],
            [InlineKeyboardButton("🔌 Проверка БД/Mango", callback_data="system_check")],
            [InlineKeyboardButton("🔄 Синхронизация аналитики", callback_data="system_sync")],
        ]
        if include_cache_reset:
            keyboard.append([InlineKeyboardButton("🗑️ Очистить кеш", callback_data="system_clear_cache")])
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="main_menu")])
        return InlineKeyboardMarkup(keyboard)
    
    def build_back_button(self, callback_data: str = "main_menu") -> InlineKeyboardMarkup:
        """Простая кнопка Назад."""
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("◀️ Назад", callback_data=callback_data)]
        ])
    
    def build_approval_buttons(
        self,
        user_id: int
    ) -> InlineKeyboardMarkup:
        """
        Кнопки одобрения/отклонения для пользователя.
        
        Args:
            user_id: Internal DB ID пользователя
        """
        keyboard = [
            [
                InlineKeyboardButton("✅ Одобрить", callback_data=f"approve_{user_id}"),
                InlineKeyboardButton("❌ Отклонить", callback_data=f"decline_{user_id}")
            ]
        ]
        return InlineKeyboardMarkup(keyboard)
    
    def build_confirmation_buttons(
        self,
        action: str,
        target_id: int
    ) -> InlineKeyboardMarkup:
        """
        Кнопки подтверждения действия.
        
        Args:
            action: Действие (block, unblock, promote, etc)
            target_id: ID целевого пользователя
        """
        keyboard = [
            [
                InlineKeyboardButton("✅ Да, подтверждаю", callback_data=f"confirm_{action}_{target_id}"),
                InlineKeyboardButton("❌ Отмена", callback_data="cancel")
            ]
        ]
        return InlineKeyboardMarkup(keyboard)
