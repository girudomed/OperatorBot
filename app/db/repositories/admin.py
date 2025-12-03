"""
Репозиторий для админских операций.

Управление пользователями, ролями и аудит действий.
"""

import json
from typing import List, Optional, Dict, Any
from datetime import datetime

from app.db.manager import DatabaseManager
from app.db.models import UserRecord, AdminActionLog
from app.core.roles import role_name_from_id, ROLE_NAME_TO_ID
from app.logging_config import get_watchdog_logger
from app.utils.error_handlers import log_async_exceptions

logger = get_watchdog_logger(__name__)


class AdminRepository:
    """Репозиторий для админских операций с пользователями."""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    def _attach_role_names(self, rows: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        if not rows:
            return []
        for row in rows:
            row['role'] = role_name_from_id(row.get('role_id'))
        return rows

    def _attach_role_name(self, row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if row is None:
            return None
        row['role'] = role_name_from_id(row.get('role_id'))
        return row
    
    @log_async_exceptions
    async def get_pending_users(self) -> List[Dict[str, Any]]:
        """Получает список пользователей со статусом pending."""
        query = """
            SELECT id AS id, user_id AS telegram_id, username, full_name, extension,
                   role_id, status, created_at
            FROM users
            WHERE status = 'pending'
            ORDER BY created_at DESC
        """
        rows = await self.db.execute_with_retry(query, fetchall=True) or []
        return self._attach_role_names(rows)
    
    @log_async_exceptions
    async def get_user_by_telegram_id(self, telegram_id: int) -> Optional[Dict[str, Any]]:
        """Получает пользователя по telegram_id."""
        query = """
            SELECT id AS id, user_id AS telegram_id, username, full_name, extension,
                   role_id, status, approved_by, blocked_at, operator_id
            FROM users
            WHERE user_id = %s
        """
        row = await self.db.execute_with_retry(
            query, params=(telegram_id,), fetchone=True
        )
        return self._attach_role_name(row)

    @log_async_exceptions
    async def get_user_by_id(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Получает пользователя по внутреннему ID."""
        query = """
            SELECT id AS id, user_id AS telegram_id, username, full_name, extension,
                   role_id, status, approved_by, blocked_at, operator_id, created_at
            FROM users
            WHERE id = %s
            LIMIT 1
        """
        row = await self.db.execute_with_retry(
            query, params=(user_id,), fetchone=True
        )
        return self._attach_role_name(row)
    
    @log_async_exceptions
    async def approve_user(self, user_id: int, approver_id: int) -> bool:
        """
        Утверждает пользователя (pending -> approved).
        
        Args:
            user_id: ID пользователя в таблице users
            approver_id: Telegram ID утверждающего админа
        """
        # Получаем ID утверждающего из users
        approver_query = "SELECT id FROM users WHERE user_id = %s"
        approver_row = await self.db.execute_with_retry(
            approver_query, params=(approver_id,), fetchone=True
        )
        
        if not approver_row:
            logger.warning(f"Approver {approver_id} not found in DB")
            return False
        
        approver_db_id = approver_row['id'] if isinstance(approver_row, dict) else approver_row['id']
        
        # Обновляем пользователя
        query = """
            UPDATE users
            SET status = 'approved', approved_by = %s
            WHERE id = %s AND status = 'pending'
        """
        result = await self.db.execute_with_retry(
            query, params=(approver_db_id, user_id), commit=True
        )
        
        # Логируем действие
        await self.log_admin_action(
            actor_id=approver_db_id,
            action='approve',
            target_id=user_id,
            payload={'timestamp': datetime.now().isoformat()}
        )
        
        logger.info(f"User {user_id} approved by {approver_id}")
        return True
    
    @log_async_exceptions
    async def decline_user(self, user_id: int, decliner_id: int, reason: Optional[str] = None) -> bool:
        """Отклоняет заявку пользователя (удаляет или блокирует)."""
        decliner_query = "SELECT id FROM users WHERE user_id = %s"
        decliner_row = await self.db.execute_with_retry(
            decliner_query, params=(decliner_id,), fetchone=True
        )
        
        if not decliner_row:
            return False
        
        decliner_db_id = decliner_row['id']
        
        # Блокируем вместо удаления
        query = """
            UPDATE users
            SET status = 'blocked', blocked_at = NOW()
            WHERE id = %s
        """
        await self.db.execute_with_retry(query, params=(user_id,), commit=True)
        
        await self.log_admin_action(
            actor_id=decliner_db_id,
            action='decline',
            target_id=user_id,
            payload={'reason': reason}
        )
        
        logger.info(f"User {user_id} declined by {decliner_id}")
        return True
    
    @log_async_exceptions
    async def block_user(self, user_id: int, blocker_id: int, reason: Optional[str] = None) -> bool:
        """Блокирует пользователя."""
        blocker_query = "SELECT id FROM users WHERE user_id = %s"
        blocker_row = await self.db.execute_with_retry(
            blocker_query, params=(blocker_id,), fetchone=True
        )
        
        if not blocker_row:
            return False
        
        blocker_db_id = blocker_row['id']
        
        query = """
            UPDATE users
            SET status = 'blocked', blocked_at = NOW()
            WHERE id = %s
        """
        await self.db.execute_with_retry(query, params=(user_id,), commit=True)
        
        await self.log_admin_action(
            actor_id=blocker_db_id,
            action='block',
            target_id=user_id,
            payload={'reason': reason}
        )
        
        return True
    
    @log_async_exceptions
    async def unblock_user(self, user_id: int, unblocker_id: int) -> bool:
        """Разблокирует пользователя."""
        unblocker_query = "SELECT id FROM users WHERE user_id = %s"
        unblocker_row = await self.db.execute_with_retry(
            unblocker_query, params=(unblocker_id,), fetchone=True
        )
        
        if not unblocker_row:
            return False
        
        unblocker_db_id = unblocker_row['id']
        
        query = """
            UPDATE users
            SET status = 'approved', blocked_at = NULL
            WHERE id = %s
        """
        await self.db.execute_with_retry(query, params=(user_id,), commit=True)
        
        await self.log_admin_action(
            actor_id=unblocker_db_id,
            action='unblock',
            target_id=user_id
        )
        
        return True
    
    @log_async_exceptions
    async def promote_user(
        self, 
        user_id: int, 
        new_role: str, 
        promoter_id: int
    ) -> bool:
        """
        Повышает пользователя до новой роли.
        
        Args:
            user_id: ID пользователя в таблице users
            new_role: Новая роль ('admin' или 'superadmin')
            promoter_id: Telegram ID повышающего
        """
        promoter_query = "SELECT id FROM users WHERE user_id = %s"
        promoter_row = await self.db.execute_with_retry(
            promoter_query, params=(promoter_id,), fetchone=True
        )
        
        if not promoter_row:
            return False
        
        promoter_db_id = promoter_row['id']
        
        new_role_id = ROLE_NAME_TO_ID.get(new_role)
        if not new_role_id:
            logger.warning(f"Unknown role '{new_role}' for promotion")
            return False
        
        query = """
            UPDATE users
            SET role_id = %s
            WHERE id = %s AND status = 'approved'
        """
        await self.db.execute_with_retry(
            query, params=(new_role_id, user_id), commit=True
        )
        
        await self.log_admin_action(
            actor_id=promoter_db_id,
            action='promote',
            target_id=user_id,
            payload={'new_role': new_role}
        )
        
        logger.info(f"User {user_id} promoted to {new_role} by {promoter_id}")
        return True
    
    @log_async_exceptions
    async def demote_user(
        self,
        user_id: int,
        new_role: str,
        demoter_id: int
    ) -> bool:
        """Понижает роль пользователя."""
        demoter_query = "SELECT id FROM users WHERE user_id = %s"
        demoter_row = await self.db.execute_with_retry(
            demoter_query, params=(demoter_id,), fetchone=True
        )
        
        if not demoter_row:
            return False
        
        demoter_db_id = demoter_row['id']
        
        new_role_id = ROLE_NAME_TO_ID.get(new_role)
        if not new_role_id:
            logger.warning(f"Unknown role '{new_role}' for demotion")
            return False
        
        query = "UPDATE users SET role_id = %s WHERE id = %s"
        await self.db.execute_with_retry(
            query, params=(new_role_id, user_id), commit=True
        )
        
        await self.log_admin_action(
            actor_id=demoter_db_id,
            action='demote',
            target_id=user_id,
            payload={'new_role': new_role}
        )
        
        return True
    
    @log_async_exceptions
    async def get_admins(self) -> List[Dict[str, Any]]:
        """Получает список всех админов и супер-админов."""
        query = """
            SELECT id AS id, user_id AS telegram_id, username, full_name, extension, role_id, status
            FROM users
            WHERE role_id IN (2, 3) AND status = 'approved'
            ORDER BY role_id DESC, full_name
        """
        rows = await self.db.execute_with_retry(query, fetchall=True) or []
        return self._attach_role_names(rows)

    @log_async_exceptions
    async def get_admin_candidates(
        self,
        limit: int = 10,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Получает список утверждённых операторов для назначения админами."""
        query = """
            SELECT id AS id, user_id AS telegram_id, username, full_name, extension,
                   role_id, status
            FROM users
            WHERE status = 'approved' AND (role_id IS NULL OR role_id = %s)
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
        """
        rows = await self.db.execute_with_retry(
            query,
            params=(ROLE_NAME_TO_ID['operator'], limit, offset),
            fetchall=True,
        ) or []
        return self._attach_role_names(rows)
    
    @log_async_exceptions
    async def get_all_users(self, status_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """Получает всех пользователей с опциональным фильтром по статусу."""
        if status_filter:
            query = """
                SELECT id AS id, user_id AS telegram_id, username, full_name, extension,
                       role_id, status, created_at, approved_by
                FROM users
                WHERE status = %s
                ORDER BY created_at DESC
            """
            params = (status_filter,)
        else:
            query = """
                SELECT id AS id, user_id AS telegram_id, username, full_name, extension,
                       role_id, status, created_at, approved_by
                FROM users
                ORDER BY created_at DESC
            """
            params = None
        
        rows = await self.db.execute_with_retry(
            query, params=params, fetchall=True
        ) or []
        return self._attach_role_names(rows)

    @log_async_exceptions
    async def get_users_counters(self) -> Dict[str, int]:
        """Возвращает агрегированные счётчики пользователей по статусам и ролям."""
        query = f"""
            SELECT
                COUNT(*) AS total_users,
                SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending_users,
                SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END) AS approved_users,
                SUM(CASE WHEN status = 'blocked' THEN 1 ELSE 0 END) AS blocked_users,
                SUM(
                    CASE
                        WHEN status = 'approved' AND role_id IN (%s, %s) THEN 1
                        ELSE 0
                    END
                ) AS admins_count,
                SUM(
                    CASE
                        WHEN status = 'approved' AND (role_id IS NULL OR role_id = %s) THEN 1
                        ELSE 0
                    END
                ) AS operators_count
            FROM users
        """
        params = (
            ROLE_NAME_TO_ID.get('admin'),
            ROLE_NAME_TO_ID.get('superadmin'),
            ROLE_NAME_TO_ID.get('operator'),
        )
        row = await self.db.execute_with_retry(query, params=params, fetchone=True) or {}
        return {
            'total_users': int(row.get('total_users') or 0),
            'pending_users': int(row.get('pending_users') or 0),
            'approved_users': int(row.get('approved_users') or 0),
            'blocked_users': int(row.get('blocked_users') or 0),
            'admins': int(row.get('admins_count') or 0),
            'operators': int(row.get('operators_count') or 0),
        }

    @log_async_exceptions
    async def get_users_for_promotion(
        self,
        target_role: str = "admin",
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Возвращает список пользователей, которых можно повысить.

        По умолчанию — все утверждённые операторы.
        """
        role_id = ROLE_NAME_TO_ID.get("operator")
        if target_role == "superadmin":
            role_id = ROLE_NAME_TO_ID.get("admin")

        query = """
            SELECT id AS id, user_id AS telegram_id, username, full_name, extension, role_id, status, created_at
            FROM users
            WHERE status = 'approved' AND role_id = %s
            ORDER BY created_at DESC
            LIMIT %s
        """
        rows = await self.db.execute_with_retry(
            query,
            params=(role_id, limit),
            fetchall=True,
        ) or []
        return self._attach_role_names(rows)
    
    @log_async_exceptions
    async def log_admin_action(
        self,
        actor_id: int,
        action: str,
        target_id: Optional[int] = None,
        payload: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Записывает действие админа в лог.
        
        Args:
            actor_id: ID админа в таблице users (не telegram_id!)
            action: approve, decline, promote, demote, block, unblock, lookup
            target_id: ID целевого пользователя (optional)
            payload: Дополнительные данные в JSON
        """
        query = """
            INSERT INTO admin_action_logs 
            (actor_id, target_id, action, payload_json, created_at)
            VALUES (%s, %s, %s, %s, NOW())
        """
        payload_json = json.dumps(payload) if payload else None
        
        await self.db.execute_with_retry(
            query,
            params=(actor_id, target_id, action, payload_json),
            commit=True
        )
        
        logger.info(f"Admin action logged: {action} by {actor_id} on {target_id}")
        return True
    
    @log_async_exceptions
    async def get_admin_action_logs(
        self,
        limit: int = 50,
        actor_id: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Получает лог действий админов."""
        if actor_id:
            query = """
                SELECT l.*, 
                       a.username as actor_username,
                       t.username as target_username
                FROM admin_action_logs l
                LEFT JOIN users a ON l.actor_id = a.id
                LEFT JOIN users t ON l.target_id = t.id
                WHERE l.actor_id = %s
                ORDER BY l.created_at DESC
                LIMIT %s
            """
            params = (actor_id, limit)
        else:
            query = """
                SELECT l.*, 
                       a.username as actor_username,
                       t.username as target_username
                FROM admin_action_logs l
                LEFT JOIN users a ON l.actor_id = a.id
                LEFT JOIN users t ON l.target_id = t.id
                ORDER BY l.created_at DESC
                LIMIT %s
            """
            params = (limit,)
        
        return await self.db.execute_with_retry(
            query, params=params, fetchall=True
        ) or []
