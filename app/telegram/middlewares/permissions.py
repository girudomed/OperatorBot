# Файл: app/telegram/middlewares/permissions.py

"""
Менеджер прав доступа для админ-панели.

Использует таблицу UsersTelegaBot с полями role_id/status.
Таблица users - это Mango справочник (НЕ использовать для ролей!).
Поддерживает Supreme Admin и Dev Admin из конфигурации.
"""

import time
from typing import Optional, Literal, Dict, Set, Tuple
from app.db.manager import DatabaseManager
from app.logging_config import get_watchdog_logger
from app.config import SUPREME_ADMIN_ID, SUPREME_ADMIN_USERNAME, DEV_ADMIN_ID, DEV_ADMIN_USERNAME
from app.core.roles import (
    role_name_from_id, 
    get_role_permissions, 
    is_admin_role, 
    is_superadmin_or_higher,
    can_manage_users as role_can_manage_users,
    can_view_all_stats as role_can_view_all_stats,
    ADMIN_ROLE_IDS,
    STATS_VIEWER_ROLE_IDS,
)

logger = get_watchdog_logger(__name__)

# Типы ролей (все 8)
Role = Literal[
    'operator',
    'admin',
    'superadmin',
    'developer',
    'head_of_registry',
    'founder',
    'marketing_director',
]
Status = Literal['pending', 'approved', 'blocked']

# Права доступа по ролям (для обратной совместимости)
ROLE_PERMISSIONS: Dict[str, Set[str]] = {
    'operator': {'call_lookup', 'weekly_quality', 'report'},
    'admin': {'call_lookup', 'weekly_quality', 'admin_panel', 'user_management', 'report', 'all_stats'},
    'head_of_registry': {'call_lookup', 'weekly_quality', 'admin_panel', 'user_management', 'manage_roles', 'report', 'all_stats'},
    'marketing_director': {'call_lookup', 'weekly_quality', 'report', 'all_stats'},
    'superadmin': {'call_lookup', 'weekly_quality', 'admin_panel', 'user_management', 'manage_roles', 'report', 'all_stats', 'debug'},
    'developer': {'call_lookup', 'weekly_quality', 'admin_panel', 'user_management', 'manage_roles', 'report', 'all_stats', 'debug'},
    'founder': {'call_lookup', 'weekly_quality', 'admin_panel', 'user_management', 'manage_roles', 'report', 'all_stats', 'debug'},
}

ADMIN_ROLES: Set[Role] = {'admin', 'head_of_registry', 'superadmin', 'developer', 'founder'}
SUPER_ROLES: Set[Role] = {'superadmin', 'developer', 'founder'}
ROLE_MANAGE_ROLES: Set[Role] = {'head_of_registry', 'superadmin', 'developer', 'founder'}
TOP_PRIVILEGE_ROLES: Set[Role] = {'developer', 'founder'}
LEADERSHIP_ROLES: Set[Role] = {'head_of_registry'} | TOP_PRIVILEGE_ROLES
SUPERADMIN_MANAGEABLE_ROLES: Set[Role] = {'admin', 'operator', 'marketing_director'}
ADMIN_MANAGEABLE_ROLES: Set[Role] = {'operator'}

CACHE_TTL_SECONDS = 10.0

class PermissionsManager:
    """
    Управление правами доступа на основе ролей из таблицы UsersTelegaBot.
    
    ВАЖНО: НЕ ПУТАТЬ с таблицей users (Mango phone справочник)!
    
    Иерархия ролей:
    - operator: базовый доступ
    - admin: может утверждать пользователей, управлять операторами
    - superadmin: полный доступ, может назначать админов
    - supreme/dev admin: bootstrap админы из конфигурации
    """
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        # user_id -> (role, status, timestamp)
        self._user_cache: Dict[int, Tuple[Optional[Role], Optional[Status], float]] = {}

    def invalidate_cache(self, user_id: int) -> None:
        """Сбрасывает кэш роли/статуса пользователя."""
        self._user_cache.pop(user_id, None)
    
    def clear_cache(self) -> None:
        """Полностью очищает кэш ролей/статусов."""
        self._user_cache.clear()

    def _get_cached_entry(self, user_id: int) -> Optional[Tuple[Optional[Role], Optional[Status]]]:
        entry = self._user_cache.get(user_id)
        if not entry:
            return None
        role, status, ts = entry
        if time.monotonic() - ts > CACHE_TTL_SECONDS:
            self._user_cache.pop(user_id, None)
            return None
        return role, status

    def _set_cache_entry(self, user_id: int, role: Optional[Role], status: Optional[Status]) -> None:
        self._user_cache[user_id] = (role, status, time.monotonic())
    
    async def get_user_role(self, user_id: int) -> Optional[Role]:
        """
        Получает роль пользователя по telegram_id.
        Учитывает только пользователей со статусом 'approved'.
        
        Returns:
            Role или None если пользователь not found/not approved
        """
        cached = self._get_cached_entry(user_id)
        if cached:
            role, status = cached
            if status != 'approved':
                logger.debug(f"User {user_id} status is {status}, not approved (cached)")
                return None
            return role

        try:
            query = """
                SELECT role_id, status
                FROM UsersTelegaBot
                WHERE user_id = %s OR telegram_id = %s
                ORDER BY 
                    CASE WHEN user_id = %s THEN 0 ELSE 1 END
                LIMIT 1
            """
            row = await self.db_manager.execute_with_retry(
                query, params=(user_id, user_id, user_id), fetchone=True
            )
            
            if not row:
                logger.debug(f"User {user_id} not found in DB")
                return None
            
            status = row.get('status')
            role = role_name_from_id(row.get('role_id'))
            self._set_cache_entry(user_id, role, status)

            if status != 'approved':
                logger.debug(f"User {user_id} status is {row.get('status')}, not approved")
                return None
            
            return role
            
        except Exception as e:
            logger.error(f"Error getting role for user {user_id}: {e}", exc_info=True)
            return None
    
    async def get_user_status(self, user_id: int) -> Optional[Status]:
        """
        Получает статус пользователя.
        
        Returns:
            Status ('pending', 'approved', 'blocked') или None
        """
        cached = self._get_cached_entry(user_id)
        if cached:
            _, status = cached
            return status

        try:
            query = """
                SELECT status 
                FROM UsersTelegaBot 
                WHERE user_id = %s OR telegram_id = %s
                ORDER BY 
                    CASE WHEN user_id = %s THEN 0 ELSE 1 END
                LIMIT 1
            """
            row = await self.db_manager.execute_with_retry(
                query, params=(user_id, user_id, user_id), fetchone=True
            )
            status = row.get('status') if row else None
            self._set_cache_entry(user_id, None, status)
            return status
        except Exception as e:
            logger.error(f"Error getting status for user {user_id}: {e}", exc_info=True)
            return None
    
    def is_supreme_admin(self, user_id: int, username: Optional[str] = None) -> bool:
        """
        Проверяет является ли пользователь Supreme Admin (из конфига).
        """
        if SUPREME_ADMIN_ID and str(user_id) == str(SUPREME_ADMIN_ID):
            return True
        if SUPREME_ADMIN_USERNAME and username and username.lower() == SUPREME_ADMIN_USERNAME.lower():
            return True
        return False
    
    def is_dev_admin(self, user_id: int, username: Optional[str] = None) -> bool:
        """
        Проверяет является ли пользователь Dev Admin (из конфига).
        """
        if DEV_ADMIN_ID and str(user_id) == str(DEV_ADMIN_ID):
            return True
        if DEV_ADMIN_USERNAME and username and username.lower() == DEV_ADMIN_USERNAME.lower():
            return True
        return False
    
    async def is_admin(self, user_id: int, username: Optional[str] = None) -> bool:
        """
        Проверяет имеет ли пользователь роль admin или выше.
        Включает supreme/dev админов и все роли, у которых есть доступ в админ-панель.
        """
        # Проверяем bootstrap админов
        if self.is_supreme_admin(user_id, username) or self.is_dev_admin(user_id, username):
            return True
        
        role = await self.get_user_role(user_id)
        return role in ADMIN_ROLES
    
    async def is_superadmin(self, user_id: int, username: Optional[str] = None) -> bool:
        """
        Проверяет имеет ли пользователь роль superadmin/developer/founder.
        """
        if self.is_supreme_admin(user_id, username) or self.is_dev_admin(user_id, username):
            return True
        
        role = await self.get_user_role(user_id)
        return role in SUPER_ROLES
    
    async def can_approve(self, user_id: int, username: Optional[str] = None) -> bool:
        """
        Проверяет может ли пользователь утверждать заявки (approve/decline).
        Требуется роль admin или выше.
        """
        return await self.is_admin(user_id, username)
    
    async def can_promote(
        self, 
        actor_id: int, 
        target_role: Role,
        actor_username: Optional[str] = None
    ) -> bool:
        """
        Проверяет может ли actor повысить кого-то до target_role.
        
        Правила:
        - supreme/dev и роли founder/developer могут назначать любую роль;
        - superadmin/head_of_registry работают только с admin/operator (включая marketing_director);
        - admin может назначать только операторов;
        - operator не может никого назначать.
        """
        # Supreme и Dev могут всё
        if self.is_supreme_admin(actor_id, actor_username) or self.is_dev_admin(actor_id, actor_username):
            return True
        
        actor_role = await self.get_user_role(actor_id)
        if not actor_role:
            return False
        
        if actor_role in TOP_PRIVILEGE_ROLES:
            return True  # Founder/Developer могут всё
        
        if actor_role == 'superadmin':
            return target_role in SUPERADMIN_MANAGEABLE_ROLES
        
        if actor_role == 'head_of_registry':
            return target_role in SUPERADMIN_MANAGEABLE_ROLES
        
        if actor_role == 'admin':
            return target_role in ADMIN_MANAGEABLE_ROLES
        
        return False
    
    async def can_demote(
        self,
        actor_id: int,
        target_id: int,
        actor_username: Optional[str] = None
    ) -> bool:
        """
        Проверяет может ли actor понизить target.
        
        Правила:
        - supreme/dev и роли founder/developer могут менять любые роли;
        - superadmin/head_of_registry могут понижать admin/operator/marketing_director;
        - admin может понижать только операторов;
        - руководителей (head_of_registry) и топ-ролей (founder/developer) могут снимать только founder/developer.
        """
        if self.is_supreme_admin(actor_id, actor_username) or self.is_dev_admin(actor_id, actor_username):
            return True
        
        actor_role = await self.get_user_role(actor_id)
        target_role = await self.get_user_role(target_id)
        
        if not actor_role or not target_role:
            return False

        if target_role in TOP_PRIVILEGE_ROLES:
            return actor_role in TOP_PRIVILEGE_ROLES
        
        if target_role == 'head_of_registry':
            return actor_role in TOP_PRIVILEGE_ROLES
        
        if actor_role in TOP_PRIVILEGE_ROLES:
            return True
        
        if actor_role == 'superadmin':
            return target_role in SUPERADMIN_MANAGEABLE_ROLES
        
        if actor_role == 'head_of_registry':
            return target_role in SUPERADMIN_MANAGEABLE_ROLES
        
        if actor_role == 'admin':
            return target_role in ADMIN_MANAGEABLE_ROLES
        
        return False
    
    async def can_access_admin_panel(self, user_id: int, username: Optional[str] = None) -> bool:
        """
        Проверяет доступ к админ-панели.
        Требуется role >= admin.
        """
        return await self.is_admin(user_id, username)
    
    async def can_view_all_operators(self, user_id: int, username: Optional[str] = None) -> bool:
        """
        Проверяет может ли пользователь видеть всех операторов.
        """
        return await self.is_admin(user_id, username)
    
    async def can_access_call_lookup(self, user_id: int, username: Optional[str] = None) -> bool:
        """
        Проверяет доступ к поиску звонков.
        Доступно approved пользователям в соответствии с правами роли.
        """
        if self.is_supreme_admin(user_id, username) or self.is_dev_admin(user_id, username):
            return True
        status = await self.get_user_status(user_id)
        if status != 'approved':
            return False
        role = await self.get_effective_role(user_id, username)
        return await self.check_permission(role, 'call_lookup')
    
    async def get_effective_role(self, user_id: int, username: Optional[str] = None) -> Role:
        """
        Получает эффективную роль с учетом bootstrap админов.
        """
        if self.is_supreme_admin(user_id, username):
            return 'founder'
        if self.is_dev_admin(user_id, username):
            return 'developer'
        
        role = await self.get_user_role(user_id)
        return role if role else 'operator'

    async def check_permission(self, role_name: Role, required_permission: str) -> bool:
        """
        Проверяет, есть ли у роли доступ к указанному разрешению.
        SuperAdmin и Dev имеют доступ ко всем действиям.
        """
        if role_name in SUPER_ROLES:
            return True
        allowed = ROLE_PERMISSIONS.get(role_name, set())
        return required_permission in allowed

    async def can_manage_roles(self, user_id: int, username: Optional[str] = None) -> bool:
        """
        Проверяет, доступно ли управление ролями.
        """
        if self.is_supreme_admin(user_id, username) or self.is_dev_admin(user_id, username):
            return True
        role = await self.get_user_role(user_id)
        return role in ROLE_MANAGE_ROLES
