# Файл: app/db/repositories/roles.py

"""
Repository для работы с ролями через таблицу roles_reference.

Заменяет магические числа (role_id == 1, 2, 3...) на проверку прав из БД.
"""

import traceback
from typing import Optional, Dict, List, Any

from app.config import DB_CONFIG
from app.core.roles import role_display_name_from_name
from app.db.manager import DatabaseManager
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)

ROLE_PRIORITY = {
    "operator": 10,
    "admin": 50,
    "superadmin": 70,
    "head_of_registry": 80,
    "marketing_director": 85,
    "developer": 90,
    "founder": 100,
}


class RolesRepository:
    """
    Repository для работы с ролями из roles_reference.
    
    Предоставляет проверку прав через can_* флаги вместо hardcoded role_id.
    """
    
    def __init__(self, db_manager: DatabaseManager, db_name: Optional[str] = None):
        self.db = db_manager
        self.db_name = db_name or DB_CONFIG.get("db")
        self._roles_cache: Dict[int, Dict[str, Any]] = {}
    
    async def get_role_by_id(self, role_id: int) -> Optional[Dict[str, Any]]:
        """
        Получить роль из roles_reference по role_id.
        
        Returns:
            Dict с полями:
            - role_id, role_name (slug), display_name
            - can_view_own_stats, can_view_all_stats
            - can_manage_users, can_debug
        """
        logger.debug(f"[ROLES] Getting role by id: {role_id}")
        
        # Проверка кеша
        if role_id in self._roles_cache:
            logger.debug(f"[ROLES] Cache HIT for role_id={role_id}")
            return self._roles_cache[role_id]
        
        try:
            query = """
                SELECT 
                    rr.role_id,
                    rr.role_name AS slug,
                    rr.can_view_own_stats,
                    rr.can_view_all_stats,
                    rr.can_manage_users,
                    rr.can_debug
                FROM roles_reference rr
                WHERE rr.role_id = %s
            """

            result = await self.db.execute_with_retry(
                query,
                params=(role_id,),
                fetchone=True
            )
            
            if result:
                # Конвертировать tinyint в bool
                role = dict(result)
                slug = role.get("slug")
                role["display_name"] = role_display_name_from_name(slug) if slug else "Неизвестная роль"
                role['can_view_own_stats'] = bool(role.get('can_view_own_stats'))
                role['can_view_all_stats'] = bool(role.get('can_view_all_stats'))
                role['can_manage_users'] = bool(role.get('can_manage_users'))
                role['can_debug'] = bool(role.get('can_debug'))
                
                # Кешировать
                self._roles_cache[role_id] = role
                
                logger.debug(
                    f"[ROLES] Found role: {role.get('display_name')}"
                )
                return role
            else:
                logger.warning(f"[ROLES] Role {role_id} not found in roles_reference")
                return None
                
        except Exception as e:
            logger.error(
                f"[ROLES] Error getting role {role_id}: {e}\n{traceback.format_exc()}"
            )
            return None
    
    async def get_all_roles(self) -> List[Dict[str, Any]]:
        """
        Получить все роли из roles_reference.
        
        Returns:
            Список всех ролей, отсортированных по приоритетам ROLE_PRIORITY
        """
        logger.info("[ROLES] Getting all roles")
        
        try:
            query = """
                SELECT 
                    rr.role_id,
                    rr.role_name AS slug,
                    rr.can_view_own_stats,
                    rr.can_view_all_stats,
                    rr.can_manage_users,
                    rr.can_debug
                FROM roles_reference rr
            """
            
            results = await self.db.execute_with_retry(
                query,
                fetchall=True
            ) or []
            
            # Конвертировать tinyint в bool
            roles = []
            for result in results:
                role = dict(result)
                slug = role.get("slug")
                role["display_name"] = role_display_name_from_name(slug) if slug else "Неизвестная роль"
                role['can_view_own_stats'] = bool(role.get('can_view_own_stats'))
                role['can_view_all_stats'] = bool(role.get('can_view_all_stats'))
                role['can_manage_users'] = bool(role.get('can_manage_users'))
                role['can_debug'] = bool(role.get('can_debug'))
                roles.append(role)
                
                # Кешировать
                self._roles_cache[role['role_id']] = role
            
            roles.sort(key=lambda role: ROLE_PRIORITY.get(role.get("slug"), 0))
            logger.info(f"[ROLES] Found {len(roles)} roles")
            return roles
            
        except Exception as e:
            logger.error(
                f"[ROLES] Error getting all roles: {e}\n{traceback.format_exc()}"
            )
            return []
    
    async def check_permission(
        self, 
        role_id: int, 
        permission: str
    ) -> bool:
        """
        Проверить наличие конкретного права у роли.
        
        Args:
            role_id: ID роли
            permission: Название права ('can_view_own_stats', 'can_view_all_stats', 
                       'can_manage_users', 'can_debug')
        
        Returns:
            True если право есть, False иначе
        """
        logger.debug(f"[ROLES] Checking permission: role_id={role_id}, permission={permission}")
        
        valid_permissions = {
            'can_view_own_stats',
            'can_view_all_stats', 
            'can_manage_users',
            'can_debug'
        }
        
        if permission not in valid_permissions:
            logger.warning(f"[ROLES] Invalid permission: {permission}")
            return False
        
        role = await self.get_role_by_id(role_id)
        
        if not role:
            logger.warning(f"[ROLES] Role {role_id} not found, permission denied")
            return False
        
        has_permission = role.get(permission, False)
        
        logger.debug(
            f"[ROLES] Permission check result: {permission}={has_permission} "
            f"for role {role.get('slug')}"
        )
        
        return has_permission
    
    async def get_roles_with_permission(self, permission: str) -> List[Dict[str, Any]]:
        """
        Получить все роли, имеющие указанное право.
        
        Args:
            permission: Название права
        
        Returns:
            Список ролей с этим правом
        """
        logger.info(f"[ROLES] Getting roles with permission: {permission}")
        
        all_roles = await self.get_all_roles()
        
        roles_with_perm = [
            role for role in all_roles 
            if role.get(permission, False)
        ]
        
        logger.info(f"[ROLES] Found {len(roles_with_perm)} roles with {permission}")
        
        return roles_with_perm
    
    async def get_user_permissions(self, user_role_id: int) -> Dict[str, bool]:
        """
        Получить все права пользователя по его role_id.
        
        Args:
            user_role_id: role_id пользователя из UsersTelegaBot
        
        Returns:
            Dict с всеми can_* правами
        """
        logger.debug(f"[ROLES] Getting all permissions for role_id={user_role_id}")
        
        role = await self.get_role_by_id(user_role_id)
        
        if not role:
            # Дефолтные права (нет прав)
            return {
                'can_view_own_stats': False,
                'can_view_all_stats': False,
                'can_manage_users': False,
                'can_debug': False
            }
        
        return {
            'can_view_own_stats': role.get('can_view_own_stats', False),
            'can_view_all_stats': role.get('can_view_all_stats', False),
            'can_manage_users': role.get('can_manage_users', False),
            'can_debug': role.get('can_debug', False)
        }
    
    def clear_cache(self):
        """Очистить кеш ролей (при изменении roles_reference)."""
        logger.info("[ROLES] Clearing roles cache")
        self._roles_cache.clear()
    
    async def get_role_name(self, role_id: int) -> str:
        """
        Получить название роли по ID.
        
        Returns:
            Название роли или "Неизвестная роль"
        """
        role = await self.get_role_by_id(role_id)
        if not role:
            return 'Неизвестная роль'
        return role.get('display_name') or role.get('slug', 'Неизвестная роль')
    
    async def get_manageable_roles(self, manager_role_id: int) -> List[Dict[str, Any]]:
        """
        Получить роли, которыми может управлять данная роль.
        
        Правило: можно назначать роли с приоритетом ниже или равным своему.
        
        Args:
            manager_role_id: role_id управляющего
        
        Returns:
            Список ролей, которые можно назначить
        """
        logger.info(f"[ROLES] Getting manageable roles for role_id={manager_role_id}")
        
        manager_role = await self.get_role_by_id(manager_role_id)
        
        if not manager_role:
            return []
        
        manager_priority = ROLE_PRIORITY.get(manager_role.get('slug'), 0)
        all_roles = await self.get_all_roles()
        
        # Можно назначать роли <= своего уровня
        manageable = [
            role for role in all_roles
            if ROLE_PRIORITY.get(role.get('slug'), 0) <= manager_priority
        ]
        
        logger.info(
            f"[ROLES] Role {manager_role.get('display_name')} (priority={manager_priority}) "
            f"can manage {len(manageable)} roles"
        )
        
        return manageable


# Глобальные константы для быстрого доступа (legacy совместимость)
# Но в новом коде использовать RolesRepository!

ROLE_OPERATOR = 1
ROLE_ADMINISTRATOR = 2
ROLE_SUPERADMIN = 3
ROLE_DEVELOPER = 4
ROLE_HEAD_OF_REGISTRY = 5
ROLE_FOUNDER = 6
ROLE_MARKETING_DIRECTOR = 7

ROLE_NAMES = {
    ROLE_OPERATOR: "Operator",
    ROLE_ADMINISTRATOR: "Admin",
    ROLE_SUPERADMIN: "SuperAdmin",
    ROLE_DEVELOPER: "Developer",
    ROLE_HEAD_OF_REGISTRY: "Head of Registry",
    ROLE_FOUNDER: "Founder",
    ROLE_MARKETING_DIRECTOR: "Marketing Director",
}
