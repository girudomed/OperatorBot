"""
Менеджер прав доступа для админ-панели.

Использует таблицу users с полями role_id/status.
Поддерживает Supreme Admin и Dev Admin из конфигурации.
"""

from typing import Optional, Literal, Dict, Set
from app.db.manager import DatabaseManager
from app.logging_config import get_watchdog_logger
from app.config import SUPREME_ADMIN_ID, SUPREME_ADMIN_USERNAME, DEV_ADMIN_ID, DEV_ADMIN_USERNAME
from app.core.roles import role_name_from_id

logger = get_watchdog_logger(__name__)

# Типы ролей
Role = Literal['operator', 'admin', 'superadmin']
Status = Literal['pending', 'approved', 'blocked']

ROLE_PERMISSIONS: Dict[Role, Set[str]] = {
    'operator': {'call_lookup', 'weekly_quality', 'report'},
    'admin': {'call_lookup', 'weekly_quality', 'admin_panel', 'user_management', 'report'},
    'superadmin': {'call_lookup', 'weekly_quality', 'admin_panel', 'user_management', 'manage_roles', 'report'},
}


class PermissionsManager:
    """
    Управление правами доступа на основе ролей из таблицы users.
    
    Иерархия ролей:
    - operator: базовый доступ
    - admin: может утверждать пользователей, управлять операторами
    - superadmin: полный доступ, может назначать админов
    - supreme/dev admin: bootstrap админы из конфигурации
    """
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    async def get_user_role(self, user_id: int) -> Optional[Role]:
        """
        Получает роль пользователя по telegram_id.
        Учитывает только пользователей со статусом 'approved'.
        
        Returns:
            Role или None если пользователь not found/not approved
        """
        try:
            query = """
                SELECT role_id, status 
                FROM users 
                WHERE user_id = %s
            """
            row = await self.db_manager.execute_with_retry(
                query, params=(user_id,), fetchone=True
            )
            
            if not row:
                logger.debug(f"User {user_id} not found in DB")
                return None
            
            if row.get('status') != 'approved':
                logger.debug(f"User {user_id} status is {row.get('status')}, not approved")
                return None
            
            return role_name_from_id(row.get('role_id'))
            
        except Exception as e:
            logger.error(f"Error getting role for user {user_id}: {e}", exc_info=True)
            return None
    
    async def get_user_status(self, user_id: int) -> Optional[Status]:
        """
        Получает статус пользователя.
        
        Returns:
            Status ('pending', 'approved', 'blocked') или None
        """
        try:
            query = "SELECT status FROM users WHERE user_id = %s"
            row = await self.db_manager.execute_with_retry(
                query, params=(user_id,), fetchone=True
            )
            return row.get('status') if row else None
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
        Включает supreme/dev админов.
        """
        # Проверяем bootstrap админов
        if self.is_supreme_admin(user_id, username) or self.is_dev_admin(user_id, username):
            return True
        
        role = await self.get_user_role(user_id)
        return role in ('admin', 'superadmin')
    
    async def is_superadmin(self, user_id: int, username: Optional[str] = None) -> bool:
        """
        Проверяет имеет ли пользователь роль superadmin или выше.
        """
        if self.is_supreme_admin(user_id, username) or self.is_dev_admin(user_id, username):
            return True
        
        role = await self.get_user_role(user_id)
        return role == 'superadmin'
    
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
        - supreme/dev/superadmin могут назначать всех
        - обычный admin может назначать только admin (не superadmin)
        - operator не может никого назначать
        """
        # Supreme и Dev могут всё
        if self.is_supreme_admin(actor_id, actor_username) or self.is_dev_admin(actor_id, actor_username):
            return True
        
        actor_role = await self.get_user_role(actor_id)
        
        if actor_role == 'superadmin':
            return True  # Может назначать всех
        
        if actor_role == 'admin':
            return target_role in ('operator', 'admin')  # Не может назначать superadmin
        
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
        - supreme/dev могут понижать всех
        - superadmin может понижать admin и operator (но не другого superadmin)
        - admin может понижать только operator
        """
        if self.is_supreme_admin(actor_id, actor_username) or self.is_dev_admin(actor_id, actor_username):
            return True
        
        actor_role = await self.get_user_role(actor_id)
        target_role = await self.get_user_role(target_id)
        
        if not actor_role or not target_role:
            return False
        
        if actor_role == 'superadmin':
            return target_role in ('admin', 'operator')
        
        if actor_role == 'admin':
            return target_role == 'operator'
        
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
        if self.is_supreme_admin(user_id, username) or self.is_dev_admin(user_id, username):
            return 'superadmin'
        
        role = await self.get_user_role(user_id)
        return role if role else 'operator'

    async def check_permission(self, role_name: Role, required_permission: str) -> bool:
        """
        Проверяет, есть ли у роли доступ к указанному разрешению.
        Суперадмины имеют доступ ко всем действиям.
        """
        if role_name == 'superadmin':
            return True
        allowed = ROLE_PERMISSIONS.get(role_name, set())
        return required_permission in allowed
