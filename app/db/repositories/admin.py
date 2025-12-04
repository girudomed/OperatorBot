"""
Репозиторий для админских операций.

Управление пользователями Telegram бота, ролями и аудит действий.

ВАЖНО: Использует UsersTelegaBot для ролей/статусов, НЕ users (Mango справочник)!
"""

import json
import traceback
from typing import List, Optional, Dict, Any
from datetime import datetime

from app.db.manager import DatabaseManager
from app.db.models import UserRecord, AdminActionLog
from app.core.roles import role_name_from_id, ROLE_NAME_TO_ID
from app.logging_config import get_watchdog_logger
from app.utils.error_handlers import log_async_exceptions

logger = get_watchdog_logger(__name__)


class AdminRepository:
    """
    Репозиторий для админских операций с Telegram пользователями бота.
    
    ВАЖНО: Работает ТОЛЬКО с UsersTelegaBot!
    Таблица users - это Mango phone справочник, НЕ трогаем для ролей!
    """
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    def _attach_role_names(self, rows: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """Добавляет название роли к каждой записи."""
        if not rows:
            return []
        for row in rows:
            row['role'] = role_name_from_id(row.get('role_id'))
        return rows

    def _attach_role_name(self, row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Добавляет название роли к записи."""
        if row is None:
            return None
        row['role'] = role_name_from_id(row.get('role_id'))
        return row
    
    @log_async_exceptions
    async def get_pending_users(self) -> List[Dict[str, Any]]:
        """Получает список Telegram пользователей со статусом pending."""
        logger.info("[ADMIN_REPO] Getting pending users")
        
        try:
            query = """
                SELECT 
                    id,
                    user_id as telegram_id,
                    username,
                    full_name,
                    role_id,
                    status,
                    operator_name,
                    extension,
                    registered_at as created_at
                FROM UsersTelegaBot
                WHERE status = 'pending'
                ORDER BY registered_at DESC
            """
            rows = await self.db.execute_with_retry(query, fetchall=True) or []
            
            logger.info(f"[ADMIN_REPO] Found {len(rows)} pending users")
            return self._attach_role_names(rows)
        except Exception as e:
            logger.error(f"[ADMIN_REPO] Error getting pending users: {e}\n{traceback.format_exc()}")
            return []
    
    @log_async_exceptions
    async def get_user_by_telegram_id(self, telegram_id: int) -> Optional[Dict[str, Any]]:
        """Получает Telegram пользователя по telegram_id."""
        logger.debug(f"[ADMIN_REPO] Getting user by telegram_id: {telegram_id}")
        
        try:
            query = """
                SELECT 
                    id,
                    user_id as telegram_id,
                    username,
                    full_name,
                    role_id,
                    status,
                    operator_name,
                    extension,
                    approved_by,
                    blocked_at,
                    registered_at as created_at
                FROM UsersTelegaBot
                WHERE user_id = %s
            """
            row = await self.db.execute_with_retry(
                query, params=(telegram_id,), fetchone=True
            )
            
            if row:
                logger.debug(f"[ADMIN_REPO] Found user: {row.get('full_name')}")
            else:
                logger.warning(f"[ADMIN_REPO] User {telegram_id} not found")
            
            return self._attach_role_name(row)
        except Exception as e:
            logger.error(
                f"[ADMIN_REPO] Error getting user {telegram_id}: {e}\n{traceback.format_exc()}"
            )
            return None

    @log_async_exceptions
    async def get_user_by_id(self, user_id: int) -> Optional[Dict[str, Any]]:
        """
        Получает Telegram пользователя по внутреннему ID.
        
        Args:
            user_id: UsersTelegaBot.id (internal DB ID)
        """
        logger.debug(f"[ADMIN_REPO] Getting user by internal id: {user_id}")
        
        try:
            query = """
                SELECT 
                    id,
                    user_id as telegram_id,
                    username,
                    full_name,
                    role_id,
                    status,
                    operator_name, 
                    extension,
                    approved_by,
                    blocked_at,
                    registered_at as created_at
                FROM UsersTelegaBot
                WHERE id = %s
                LIMIT 1
            """
            row = await self.db.execute_with_retry(
                query, params=(user_id,), fetchone=True
            )
            
            return self._attach_role_name(row)
        except Exception as e:
            logger.error(
                f"[ADMIN_REPO] Error getting user by id {user_id}: {e}\n{traceback.format_exc()}"
            )
            return None
    
    @log_async_exceptions
    async def approve_user(self, telegram_id: int, approver_telegram_id: int) -> bool:
        """
        Утверждает Telegram пользователя (pending -> approved).
        
        Args:
            telegram_id: Telegram ID пользователя для одобрения
            approver_telegram_id: Telegram ID утверждающего админа
        """
        logger.info(f"[ADMIN_REPO] Approving user {telegram_id} by {approver_telegram_id}")
        
        try:
            # Обновляем статус пользователя
            query = """
                UPDATE UsersTelegaBot
                SET status = 'approved', approved_by = %s
                WHERE user_id = %s AND status = 'pending'
            """
            await self.db.execute_with_retry(
                query, params=(approver_telegram_id, telegram_id), commit=True
            )
            
            # Логируем действие (используем telegram IDs)
            await self.log_admin_action(
                actor_telegram_id=approver_telegram_id,
                action='approve',
                target_telegram_id=telegram_id,
                payload={'timestamp': datetime.now().isoformat()}
            )
            
            logger.info(f"[ADMIN_REPO] User {telegram_id} approved successfully")
            return True
        except Exception as e:
            logger.error(
                f"[ADMIN_REPO] Error approving user {telegram_id}: {e}\n{traceback.format_exc()}"
            )
            return False
    
    @log_async_exceptions
    async def decline_user(
        self, 
        telegram_id: int, 
        decliner_telegram_id: int, 
        reason: Optional[str] = None
    ) -> bool:
        """
        Отклоняет заявку Telegram пользователя (блокирует).
        
        Args:
            telegram_id: Telegram ID пользователя
            decliner_telegram_id: Telegram ID отклоняющего админа
            reason: Причина отклонения
        """
        logger.info(f"[ADMIN_REPO] Declining user {telegram_id} by {decliner_telegram_id}")
        
        try:
            # Блокируем вместо удаления
            query = """
                UPDATE UsersTelegaBot
                SET status = 'blocked', blocked_at = NOW()
                WHERE user_id = %s
            """
            await self.db.execute_with_retry(query, params=(telegram_id,), commit=True)
            
            await self.log_admin_action(
                actor_telegram_id=decliner_telegram_id,
                action='decline',
                target_telegram_id=telegram_id,
                payload={'reason': reason}
            )
            
            logger.info(f"[ADMIN_REPO] User {telegram_id} declined successfully")
            return True
        except Exception as e:
            logger.error(
                f"[ADMIN_REPO] Error declining user {telegram_id}: {e}\n{traceback.format_exc()}"
            )
            return False
    
    @log_async_exceptions
    async def block_user(
        self, 
        telegram_id: int, 
        blocker_telegram_id: int, 
        reason: Optional[str] = None
    ) -> bool:
        """
        Блокирует Telegram пользователя.
        
        Args:
            telegram_id: Telegram ID пользователя
            blocker_telegram_id: Telegram ID блокирующего админа
            reason: Причина блокировки
        """
        logger.info(f"[ADMIN_REPO] Blocking user {telegram_id} by {blocker_telegram_id}")
        
        try:
            query = """
                UPDATE UsersTelegaBot
                SET status = 'blocked', blocked_at = NOW()
                WHERE user_id = %s
            """
            await self.db.execute_with_retry(query, params=(telegram_id,), commit=True)
            
            await self.log_admin_action(
                actor_telegram_id=blocker_telegram_id,
                action='block',
                target_telegram_id=telegram_id,
                payload={'reason': reason}
            )
            
            logger.info(f"[ADMIN_REPO] User {telegram_id} blocked successfully")
            return True
        except Exception as e:
            logger.error(
                f"[ADMIN_REPO] Error blocking user {telegram_id}: {e}\n{traceback.format_exc()}"
            )
            return False
    
    @log_async_exceptions
    async def unblock_user(self, telegram_id: int, unblocker_telegram_id: int) -> bool:
        """
        Разблокирует Telegram пользователя.
        
        Args:
            telegram_id: Telegram ID пользователя
            unblocker_telegram_id: Telegram ID разблокирующего админа
        """
        logger.info(f"[ADMIN_REPO] Unblocking user {telegram_id} by {unblocker_telegram_id}")
        
        try:
            query = """
                UPDATE UsersTelegaBot
                SET status = 'approved', blocked_at = NULL
                WHERE user_id = %s
            """
            await self.db.execute_with_retry(query, params=(telegram_id,), commit=True)
            
            await self.log_admin_action(
                actor_telegram_id=unblocker_telegram_id,
                action='unblock',
                target_telegram_id=telegram_id
            )
            
            logger.info(f"[ADMIN_REPO] User {telegram_id} unblocked successfully")
            return True
        except Exception as e:
            logger.error(
                f"[ADMIN_REPO] Error unblocking user {telegram_id}: {e}\n{traceback.format_exc()}"
            )
            return False
    
    @log_async_exceptions
    async def promote_user(
        self, 
        telegram_id: int, 
        new_role: str, 
        promoter_telegram_id: int
    ) -> bool:
        """
        Повышает Telegram пользователя до новой роли.
        
        Args:
            telegram_id: Telegram ID пользователя
            new_role: Новая роль ('admin' или 'superadmin')
            promoter_telegram_id: Telegram ID повышающего админа
        """
        logger.info(
            f"[ADMIN_REPO] Promoting user {telegram_id} to {new_role} "
            f"by {promoter_telegram_id}"
        )
        
        try:
            new_role_id = ROLE_NAME_TO_ID.get(new_role)
            if not new_role_id:
                logger.warning(f"[ADMIN_REPO] Unknown role '{new_role}' for promotion")
                return False
            
            query = """
                UPDATE UsersTelegaBot
                SET role_id = %s
                WHERE user_id = %s AND status = 'approved'
            """
            await self.db.execute_with_retry(
                query, params=(new_role_id, telegram_id), commit=True
            )
            
            await self.log_admin_action(
                actor_telegram_id=promoter_telegram_id,
                action='promote',
                target_telegram_id=telegram_id,
                payload={'new_role': new_role}
            )
            
            logger.info(f"[ADMIN_REPO] User {telegram_id} promoted to {new_role} successfully")
            return True
        except Exception as e:
            logger.error(
                f"[ADMIN_REPO] Error promoting user {telegram_id}: {e}\n{traceback.format_exc()}"
            )
            return False
    
    @log_async_exceptions
    async def demote_user(
        self,
        telegram_id: int,
        new_role: str,
        demoter_telegram_id: int
    ) -> bool:
        """
        Понижает роль Telegram пользователя.
        
        Args:
            telegram_id: Telegram ID пользователя
            new_role: Новая роль (обычно 'operator')
            demoter_telegram_id: Telegram ID понижающего админа
        """
        logger.info(
            f"[ADMIN_REPO] Demoting user {telegram_id} to {new_role} "
            f"by {demoter_telegram_id}"
        )
        
        try:
            new_role_id = ROLE_NAME_TO_ID.get(new_role)
            if not new_role_id:
                logger.warning(f"[ADMIN_REPO] Unknown role '{new_role}' for demotion")
                return False
            
            query = "UPDATE UsersTelegaBot SET role_id = %s WHERE user_id = %s"
            await self.db.execute_with_retry(
                query, params=(new_role_id, telegram_id), commit=True
            )
            
            await self.log_admin_action(
                actor_telegram_id=demoter_telegram_id,
                action='demote',
                target_telegram_id=telegram_id,
                payload={'new_role': new_role}
            )
            
            logger.info(f"[ADMIN_REPO] User {telegram_id} demoted to {new_role} successfully")
            return True
        except Exception as e:
            logger.error(
                f"[ADMIN_REPO] Error demoting user {telegram_id}: {e}\n{traceback.format_exc()}"
            )
            return False
    
    @log_async_exceptions
    async def get_admins(self) -> List[Dict[str, Any]]:
        """Получает список всех админов и супер-админов из UsersTelegaBot."""
        logger.info("[ADMIN_REPO] Getting admins list")
        
        try:
            query = """
                SELECT 
                    id,
                    user_id as telegram_id,
                    username,
                    full_name,
                    extension,
                    role_id,
                    status,
                    operator_name
                FROM UsersTelegaBot
                WHERE role_id IN (%s, %s) AND status != 'blocked'
                ORDER BY role_id DESC, full_name
            """
            rows = await self.db.execute_with_retry(
                query,
                params=(ROLE_NAME_TO_ID.get('admin'), ROLE_NAME_TO_ID.get('superadmin')),
                fetchall=True
            ) or []
            
            logger.info(f"[ADMIN_REPO] Found {len(rows)} admins")
            return self._attach_role_names(rows)
        except Exception as e:
            logger.error(f"[ADMIN_REPO] Error getting admins: {e}\n{traceback.format_exc()}")
            return []

    @log_async_exceptions
    async def get_admin_candidates(
        self,
        limit: int = 10,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Получает список утверждённых операторов для назначения админами."""
        logger.info(f"[ADMIN_REPO] Getting admin candidates (limit={limit}, offset={offset})")
        
        try:
            query = """
                SELECT 
                    id,
                    user_id as telegram_id,
                    username,
                    full_name,
                    extension,
                    role_id,
                    status,
                    operator_name
                FROM UsersTelegaBot
                WHERE status = 'approved' AND (role_id IS NULL OR role_id = %s)
                ORDER BY registered_at DESC
                LIMIT %s OFFSET %s
            """
            rows = await self.db.execute_with_retry(
                query,
                params=(ROLE_NAME_TO_ID['operator'], limit, offset),
                fetchall=True,
            ) or []
            
            logger.info(f"[ADMIN_REPO] Found {len(rows)} admin candidates")
            return self._attach_role_names(rows)
        except Exception as e:
            logger.error(
                f"[ADMIN_REPO] Error getting admin candidates: {e}\n{traceback.format_exc()}"
            )
            return []
    
    @log_async_exceptions
    async def get_all_users(self, status_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Получает всех Telegram пользователей с опциональным фильтром по статусу.
        
        Args:
            status_filter: 'pending', 'approved', 'blocked' или None (все)
        """
        logger.info(f"[ADMIN_REPO] Getting all users (status_filter={status_filter})")
        
        try:
            if status_filter:
                query = """
                    SELECT 
                        id,
                        user_id as telegram_id,
                        username,
                        full_name,
                        extension,
                        role_id,
                        status,
                        operator_name,
                        registered_at as created_at,
                        approved_by
                    FROM UsersTelegaBot
                    WHERE status = %s
                    ORDER BY registered_at DESC
                """
                params = (status_filter,)
            else:
                query = """
                    SELECT 
                        id,
                        user_id as telegram_id,
                        username,
                        full_name,
                        extension,
                        role_id,
                        status,
                        operator_name,
                        registered_at as created_at,
                        approved_by
                    FROM UsersTelegaBot
                    ORDER BY registered_at DESC
                """
                params = None
            
            rows = await self.db.execute_with_retry(
                query, params=params, fetchall=True
            ) or []
            
            logger.info(f"[ADMIN_REPO] Found {len(rows)} users")
            return self._attach_role_names(rows)
        except Exception as e:
            logger.error(
                f"[ADMIN_REPO] Error getting all users: {e}\n{traceback.format_exc()}"
            )
            return []
    
    @log_async_exceptions
    async def get_users_counters(self) -> Dict[str, int]:
        """
        Возвращает счётчики пользователей для Dashboard.
        
        Логика:
        - operators - из users (Mango ВАТС операторы)
        - pending_users, approved_users, blocked_users, admins - из UsersTelegaBot
        """
        logger.info("[ADMIN_REPO] Getting user counters")
        
        try:
            # Операторы из users (Mango ВАТС) - это правильно!
            operators_query = """
                SELECT COUNT(*) as count
                FROM users
                WHERE extension IS NOT NULL
            """
            operators_row = await self.db.execute_with_retry(
                operators_query, fetchone=True
            ) or {}
            
            # Telegram пользователи из UsersTelegaBot
            telegram_query = """
                SELECT 
                    COUNT(*) as total_users,
                    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending_users,
                    SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END) as approved_users,
                    SUM(CASE WHEN status = 'blocked' THEN 1 ELSE 0 END) as blocked_users,
                    SUM(CASE WHEN role_id IN (%s, %s) THEN 1 ELSE 0 END) as admins_count
                FROM UsersTelegaBot
            """
            params = (
                ROLE_NAME_TO_ID.get('admin'),
                ROLE_NAME_TO_ID.get('superadmin'),
            )
            telegram_row = await self.db.execute_with_retry(
                telegram_query, params=params, fetchone=True
            ) or {}
            
            counters = {
                'total_users': int(telegram_row.get('total_users') or 0),
                'pending_users': int(telegram_row.get('pending_users') or 0),
                'approved_users': int(telegram_row.get('approved_users') or 0),
                'blocked_users': int(telegram_row.get('blocked_users') or 0),
                'admins': int(telegram_row.get('admins_count') or 0),
                'operators': int(operators_row.get('count') or 0),
            }
            
            logger.info(f"[ADMIN_REPO] Counters: {counters}")
            return counters
        except Exception as e:
            logger.error(
                f"[ADMIN_REPO] Error getting counters: {e}\n{traceback.format_exc()}"
            )
            return {
                'total_users': 0,
                'pending_users': 0,
                'approved_users': 0,
                'blocked_users': 0,
                'admins': 0,
                'operators': 0,
            }

    @log_async_exceptions
    async def get_users_for_promotion(
        self,
        target_role: str = "admin",
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Возвращает список Telegram пользователей, которых можно повысить.
        
        Args:
            target_role: Целевая роль ('admin' или 'superadmin')
            limit: Максимальное количество
        """
        logger.info(f"[ADMIN_REPO] Getting users for promotion to {target_role}")
        
        try:
            role_id = ROLE_NAME_TO_ID.get("operator")
            if target_role == "superadmin":
                role_id = ROLE_NAME_TO_ID.get("admin")

            query = """
                SELECT 
                    id,
                    user_id as telegram_id,
                    username,
                    full_name,
                    extension,
                    role_id,
                    status,
                    operator_name,
                    registered_at as created_at
                FROM UsersTelegaBot
                WHERE status = 'approved' AND role_id = %s
                ORDER BY registered_at DESC
                LIMIT %s
            """
            rows = await self.db.execute_with_retry(
                query,
                params=(role_id, limit),
                fetchall=True,
            ) or []
            
            logger.info(f"[ADMIN_REPO] Found {len(rows)} users for promotion")
            return self._attach_role_names(rows)
        except Exception as e:
            logger.error(
                f"[ADMIN_REPO] Error getting users for promotion: {e}\n{traceback.format_exc()}"
            )
            return []
    
    @log_async_exceptions
    async def log_admin_action(
        self,
        actor_telegram_id: int,
        action: str,
        target_telegram_id: Optional[int] = None,
        payload: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Записывает действие админа в лог.
        
        ВАЖНО: Использует telegram user_id (не внутренние DB IDs)!
        
        Args:
            actor_telegram_id: Telegram ID админа (кто совершил действие)
            action: approve, decline, promote, demote, block, unblock, lookup
            target_telegram_id: Telegram ID целевого пользователя (optional)
            payload: Дополнительные данные в JSON
        """
        logger.info(
            f"[ADMIN_REPO] Logging action: {action} by {actor_telegram_id} "
            f"on {target_telegram_id}"
        )
        
        try:
            query = """
                INSERT INTO admin_action_logs 
                (actor_id, target_id, action, payload_json, created_at)
                VALUES (%s, %s, %s, %s, NOW())
            """
            payload_json = json.dumps(payload) if payload else None
            
            await self.db.execute_with_retry(
                query,
                params=(actor_telegram_id, target_telegram_id, action, payload_json),
                commit=True
            )
            
            logger.info(f"[ADMIN_REPO] Action logged successfully")
            return True
        except Exception as e:
            logger.error(
                f"[ADMIN_REPO] Error logging action: {e}\n{traceback.format_exc()}"
            )
            return False
    
    @log_async_exceptions
    async def get_admin_action_logs(
        self,
        limit: int = 50,
        actor_telegram_id: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Получает лог действий админов.
        
        Args:
            limit: Максимальное количество записей
            actor_telegram_id: Фильтр по telegram ID актора (optional)
        """
        logger.info(
            f"[ADMIN_REPO] Getting action logs (limit={limit}, "
            f"actor={actor_telegram_id})"
        )
        
        try:
            if actor_telegram_id:
                query = """
                    SELECT l.*, 
                           a.username as actor_username,
                           t.username as target_username
                    FROM admin_action_logs l
                    LEFT JOIN UsersTelegaBot a ON l.actor_id = a.user_id
                    LEFT JOIN UsersTelegaBot t ON l.target_id = t.user_id
                    WHERE l.actor_id = %s
                    ORDER BY l.created_at DESC
                    LIMIT %s
                """
                params = (actor_telegram_id, limit)
            else:
                query = """
                    SELECT l.*, 
                           a.username as actor_username,
                           t.username as target_username
                    FROM admin_action_logs l
                    LEFT JOIN UsersTelegaBot a ON l.actor_id = a.user_id
                    LEFT JOIN UsersTelegaBot t ON l.target_id = t.user_id
                    ORDER BY l.created_at DESC
                    LIMIT %s
                """
                params = (limit,)
            
            rows = await self.db.execute_with_retry(
                query, params=params, fetchall=True
            ) or []
            
            logger.info(f"[ADMIN_REPO] Found {len(rows)} log entries")
            return rows
        except Exception as e:
            logger.error(
                f"[ADMIN_REPO] Error getting action logs: {e}\n{traceback.format_exc()}"
            )
            return []

    @log_async_exceptions
    async def get_users_with_chat_ids(self) -> List[Dict[str, Any]]:
        """
        Возвращает Telegram пользователей, у которых сохранён chat_id.
        
        Note: chat_id может быть в UsersTelegaBot если храним, или не использоваться.
        """
        logger.info("[ADMIN_REPO] Getting users with chat_ids")
        
        try:
            # Если chat_id есть в UsersTelegaBot
            query = """
                SELECT user_id
                FROM UsersTelegaBot
                WHERE user_id IS NOT NULL
            """
            rows = await self.db.execute_with_retry(query, fetchall=True) or []
            
            logger.info(f"[ADMIN_REPO] Found {len(rows)} users with IDs")
            return rows
        except Exception as e:
            logger.error(
                f"[ADMIN_REPO] Error getting users with chat_ids: {e}\n{traceback.format_exc()}"
            )
            return []
