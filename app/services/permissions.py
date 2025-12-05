"""
Сервис для управления правами доступа на основе ролей.
Определяет, какие команды и действия доступны пользователям разных уровней.
"""

from typing import Dict, List, Optional
from functools import wraps

from telegram import Update
from telegram.ext import ContextTypes

from app.db.manager import DatabaseManager
from app.db.repositories.users import UserRepository
from app.db.repositories.roles import RolesRepository
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)


# Константы ролей (должны соответствовать role_id в БД)
ROLE_OPERATOR = 1
ROLE_ADMINISTRATOR = 2
ROLE_MARKETER = 3
ROLE_ZAV_REG = 4
ROLE_ST_ADMIN = 5
ROLE_MANAGEMENT = 6
ROLE_SUPER_ADMIN = 7
ROLE_DEV = 8

# Названия ролей
ROLE_NAMES = {
    ROLE_OPERATOR: "Оператор",
    ROLE_ADMINISTRATOR: "Администратор",
    ROLE_MARKETER: "Маркетолог",
    ROLE_ZAV_REG: "Зав. Рег.",
    ROLE_ST_ADMIN: "СТ Админ",
    ROLE_MANAGEMENT: "Руководство",
    ROLE_SUPER_ADMIN: "SuperAdmin",
    ROLE_DEV: "Dev"
}


class PermissionChecker:
    """Класс для проверки прав доступа пользователей."""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.user_repo = UserRepository(db_manager)
        self.roles_repo = RolesRepository(db_manager)
    
    async def get_user_role(self, telegram_id: int) -> Optional[int]:
        """Получить role_id пользователя по telegram ID."""
        user = await self.user_repo.get_user_by_telegram_id(telegram_id)
        if not user:
            return None
        return user.get('role_id', ROLE_OPERATOR)
    
    async def can_view_own_stats(self, telegram_id: int) -> bool:
        """Может ли пользователь видеть свою статистику."""
        role_id = await self.get_user_role(telegram_id)
        if not role_id:
            return False
        return await self.roles_repo.check_permission(role_id, 'can_view_own_stats')
    
    async def can_view_all_stats(self, telegram_id: int) -> bool:
        """Может ли пользователь видеть статистику всех операторов."""
        role_id = await self.get_user_role(telegram_id)
        if not role_id:
            return False
        return await self.roles_repo.check_permission(role_id, 'can_view_all_stats')
    
    async def can_view_dashboard(self, telegram_id: int) -> bool:
        """Доступ к dashboard."""
        role_id = await self.get_user_role(telegram_id)
        # Все зарегистрированные
        return role_id is not None
    
    async def can_generate_reports(self, telegram_id: int) -> bool:
        """Может ли генерировать отчеты."""
        role_id = await self.get_user_role(telegram_id)
        # Все роли
        return role_id is not None
    
    async def can_view_transcripts(self, telegram_id: int) -> bool:
        """Доступ к расшифровкам звонков."""
        role_id = await self.get_user_role(telegram_id)
        # Все роли
        return role_id is not None
    
    async def can_view_other_transcripts(self, telegram_id: int) -> bool:
        """Может ли видеть чужие расшифровки."""
        role_id = await self.get_user_role(telegram_id)
        if not role_id:
            return False
        # Админы и выше (используем логику из RolesRepository если есть, или оставляем >= ADMIN)
        # В RolesRepository нет отдельного флага для этого, но обычно это связано с can_manage_users или can_view_all_stats
        # Оставим пока старую логику или привяжем к can_view_all_stats как наиболее близкому
        return await self.roles_repo.check_permission(role_id, 'can_view_all_stats')
    
    async def can_manage_users(self, telegram_id: int) -> bool:
        """Управление пользователями."""
        role_id = await self.get_user_role(telegram_id)
        if not role_id:
            return False
        return await self.roles_repo.check_permission(role_id, 'can_manage_users')
    
    async def can_debug(self, telegram_id: int) -> bool:
        """Команды отладки."""
        role_id = await self.get_user_role(telegram_id)
        if not role_id:
            return False
        return await self.roles_repo.check_permission(role_id, 'can_debug')
    
    async def can_message_dev(self, telegram_id: int) -> bool:
        """Может ли отправлять сообщения разработчику."""
        role_id = await self.get_user_role(telegram_id)
        # Все зарегистрированные
        return role_id is not None
    
    async def get_available_commands(self, telegram_id: int) -> List[str]:
        """
        Получить список доступных команд для пользователя.
        
        Returns:
            Список названий команд
        """
        role_id = await self.get_user_role(telegram_id)
        
        if not role_id:
            return ['/start']
        
        commands = [
            '/start',
            '/dashboard',
            '/report',
            '/transcript',
            '/message_dev'
        ]
        
        # Команды для админов
        if role_id >= ROLE_ADMINISTRATOR:
            commands.extend([
                '/admin',
                '/users',
                '/stats'
            ])
        
        # Команды для разработчиков
        if role_id == ROLE_DEV:
            commands.extend([
                '/debug',
                '/logs',
                '/db_info'
            ])
        
        return commands


# Декоратор для проверки прав доступа
def require_role(min_role_id: int = ROLE_OPERATOR, 
                 permission_check: Optional[str] = None):
    """
    Декоратор для проверки роли пользователя.
    
    Args:
        min_role_id: минимальный role_id для доступа
        permission_check: название метода PermissionChecker для дополнительной проверки
            например: 'can_view_all_stats'
    
    Example:
        @require_role(ROLE_ADMINISTRATOR)
        async def admin_command(update, context):
            ...
        
        @require_role(permission_check='can_view_all_stats')
        async def all_stats_command(update, context):
            ...
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(self_or_update, *args, **kwargs):
            # Определяем, это метод класса или функция
            if hasattr(self_or_update, 'db_manager'):
                # Это метод класса handler
                self_obj = self_or_update
                update = args[0] if args else kwargs.get('update')
                context = args[1] if len(args) > 1 else kwargs.get('context')
            else:
                # Это обычная функция
                self_obj = None
                update = self_or_update
                context = args[0] if args else kwargs.get('context')
            
            telegram_id = update.effective_user.id
            
            # Получаем db_manager
            if self_obj and hasattr(self_obj, 'db_manager'):
                db_manager = self_obj.db_manager
            elif context and hasattr(context, 'application') and hasattr(context.application, 'db_manager'):
                db_manager = context.application.db_manager
            else:
                logger.error("Не удалось получить db_manager для проверки прав")
                await self._send_no_permission(update)
                return
            
            checker = PermissionChecker(db_manager)
            
            # Проверка роли
            user_role = await checker.get_user_role(telegram_id)
            
            if user_role is None:
                await self._send_not_registered(update)
                return
            
            if user_role < min_role_id:
                await self._send_no_permission(update)
                return
            
            # Дополнительная проверка через метод
            if permission_check:
                check_method = getattr(checker, permission_check, None)
                if check_method and not await check_method(telegram_id):
                    await self._send_no_permission(update)
                    return
            
            # Вызываем оригинальную функцию
            if self_obj:
                return await func(self_obj, update, context, *args[2:], **kwargs)
            else:
                return await func(update, context, *args[1:], **kwargs)
        
        return wrapper
    
    @staticmethod
    async def _send_not_registered(update: Update):
        """Отправить сообщение о необходимости регистрации."""
        message = (
            "❌ Вы не зарегистрированы в системе.\n"
            "Используйте /start для регистрации."
        )
        if update.message:
            await update.message.reply_text(message)
        elif update.callback_query:
            await update.callback_query.answer(message, show_alert=True)
    
    @staticmethod
    async def _send_no_permission(update: Update):
        """Отправить сообщение об отсутствии прав."""
        message = (
            "🔒 У вас нет прав для выполнения этой команды.\n"
            "Обратитесь к администратору."
        )
        if update.message:
            await update.message.reply_text(message)
        elif update.callback_query:
            await update.callback_query.answer(message, show_alert=True)
    
    return decorator


def get_role_name(role_id: int) -> str:
    """Получить название роли по ID."""
    return ROLE_NAMES.get(role_id, "Неизвестная роль")


def is_admin_role(role_id: int) -> bool:
    """Проверить, является ли роль админской."""
    return role_id >= ROLE_ADMINISTRATOR
