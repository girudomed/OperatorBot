"""
Упрощённый (legacy) менеджер прав доступа.

Работает только с таблицей users и полями role_id/status.
Не просит пользователя вводить роль или пароли — роль задаётся системой
при регистрации/повышении/понижении.
"""

from typing import Optional, Dict, Set, List

from app.db.manager import DatabaseManager
from app.logging_config import get_watchdog_logger
from app.core.roles import ROLE_NAME_TO_ID, role_name_from_id

logger = get_watchdog_logger(__name__)

# Какие действия доступны каждой роли
ROLE_PERMISSIONS: Dict[str, Set[str]] = {
    "operator": {"report", "weekly_quality", "call_lookup"},
    "admin": {"report", "weekly_quality", "call_lookup", "admin_panel", "user_management"},
    "superadmin": {"report", "weekly_quality", "call_lookup", "admin_panel", "user_management", "manage_roles"},
}


class PermissionsManager:
    """Legacy-версия менеджера прав, использующая только users.role_id/status."""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    async def get_user_role(self, user_id: int) -> Optional[str]:
        """Возвращает роль пользователя (operator/admin/superadmin)."""
        try:
            query = "SELECT role_id FROM UsersTelegaBot WHERE user_id = %s"
            row = await self.db_manager.execute_with_retry(query, params=(user_id,), fetchone=True)
            if not row:
                logger.warning("User %s not found when fetching role", user_id)
                return None
            return role_name_from_id(row.get("role_id"))
        except Exception as exc:
            logger.error("Cannot fetch role for user %s: %s", user_id, exc, exc_info=True)
            return None

    async def get_user_status(self, user_id: int) -> Optional[str]:
        """Возвращает статус пользователя (pending/approved/blocked)."""
        try:
            query = "SELECT status FROM UsersTelegaBot WHERE user_id = %s"
            row = await self.db_manager.execute_with_retry(query, params=(user_id,), fetchone=True)
            return row.get("status") if row else None
        except Exception as exc:
            logger.error("Cannot fetch status for user %s: %s", user_id, exc, exc_info=True)
            return None

    async def has_permission(self, user_id: int, permission: str) -> bool:
        """Проверяет разрешение на основе роли из БД."""
        role = await self.get_user_role(user_id)
        if not role:
            return False
        allowed = ROLE_PERMISSIONS.get(role, set())
        return permission in allowed

    async def set_user_role(self, user_id: int, role_name: str) -> bool:
        """Обновляет role_id пользователя. Используется при повышении/понижении."""
        role_id = ROLE_NAME_TO_ID.get(role_name)
        if not role_id:
            logger.warning("Unknown role '%s' for user %s", role_name, user_id)
            return False
        try:
            query = "UPDATE UsersTelegaBot SET role_id = %s WHERE user_id = %s"
            await self.db_manager.execute_with_retry(query, params=(role_id, user_id), commit=True)
            logger.info("User %s role updated to %s", user_id, role_name)
            return True
        except Exception as exc:
            logger.error("Cannot update role for user %s: %s", user_id, exc, exc_info=True)
            return False

    async def list_users_by_role(self, role_name: str) -> List[dict]:
        """Возвращает пользователей заданной роли."""
        role_id = ROLE_NAME_TO_ID.get(role_name)
        if not role_id:
            logger.warning("Unknown role '%s' when listing users", role_name)
            return []
        try:
            query = "SELECT user_id, full_name, username, status FROM UsersTelegaBot WHERE role_id = %s"
            rows = await self.db_manager.execute_with_retry(query, params=(role_id,), fetchall=True)
            return rows or []
        except Exception as exc:
            logger.error("Cannot list users for role %s: %s", role_name, exc, exc_info=True)
            return []

    async def list_admins(self) -> List[dict]:
        """Возвращает всех админов и супер-админов."""
        try:
            query = """
                SELECT user_id, full_name, username, role_id, status
                FROM UsersTelegaBot
                WHERE role_id IN (%s, %s)
                ORDER BY role_id DESC, full_name
            """
            rows = await self.db_manager.execute_with_retry(
                query,
                params=(ROLE_NAME_TO_ID.get("admin"), ROLE_NAME_TO_ID.get("superadmin")),
                fetchall=True,
            )
            return rows or []
        except Exception as exc:
            logger.error("Cannot fetch admins: %s", exc, exc_info=True)
            return []
