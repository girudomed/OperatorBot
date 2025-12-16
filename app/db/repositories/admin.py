# Файл: app/db/repositories/admin.py

"""
Репозиторий для админских операций.

Управление пользователями Telegram бота, ролями и аудит действий.

ВАЖНО: Использует UsersTelegaBot для ролей/статусов, НЕ users (Mango справочник)!
"""

import asyncio
import json
import traceback
from typing import List, Optional, Dict, Any, Set
from datetime import datetime

from app.db.manager import DatabaseManager
from app.db.models import UserRecord, AdminActionLog
from app.core.roles import role_name_from_id, ADMIN_ROLE_IDS
from app.logging_config import get_watchdog_logger
from app.utils.error_handlers import log_async_exceptions

logger = get_watchdog_logger(__name__)


class AdminRepository:
    """
    Репозиторий для админских операций с Telegram пользователями бота.
    
    ВАЖНО: Работает ТОЛЬКО с UsersTelegaBot!
    Таблица users - это Mango phone справочник, НЕ трогаем для ролей!
    """
    
    USER_FIELDS_BASE = """
        SELECT
            u.id,
            u.user_id AS telegram_id,
            u.telegram_id AS legacy_telegram_id,
            u.username,
            u.full_name,
            u.extension,
            u.role_id,
            u.status,
            u.operator_name,
            u.approved_by,
            u.blocked_at,
            u.created_at,
            u.updated_at,
            r.role_name AS role_name
        FROM UsersTelegaBot u
        LEFT JOIN RolesTelegaBot r ON r.id = u.role_id
    """

    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self._roles_lock = asyncio.Lock()
        self._roles_loaded = False
        self._role_slug_to_id: Dict[str, int] = {}
        self._role_id_to_slug: Dict[int, str] = {}
        self._role_display: Dict[str, str] = {}
        self._admin_role_ids: Set[int] = set()

    def _attach_role_names(self, rows: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """Добавляет название роли к каждой записи."""
        if not rows:
            return []
        for row in rows:
            row['role'] = self._build_role_payload(row)
            row.pop("role_name", None)
        return rows

    def _attach_role_name(self, row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Добавляет название роли к записи."""
        if row is None:
            return None
        row['role'] = self._build_role_payload(row)
        row.pop("role_name", None)
        return row

    def _build_role_payload(self, row: Dict[str, Any]) -> Dict[str, Any]:
        role_id = row.get("role_id")
        slug = role_name_from_id(role_id)
        display_name = row.get("role_name") or slug
        return {
            "id": role_id,
            "slug": slug,
            "name": display_name,
        }

    @staticmethod
    def _normalize_role_slug(value: Optional[str]) -> str:
        return (value or "").strip().lower().replace(" ", "_")

    async def _load_roles(self, *, force: bool = False) -> None:
        if self._roles_loaded and not force:
            return
        async with self._roles_lock:
            if self._roles_loaded and not force:
                return
            try:
                query = """
                    SELECT
                        rr.role_id,
                        COALESCE(rt.role_name, rr.role_name) AS role_name,
                        rr.can_manage_users
                    FROM roles_reference rr
                    LEFT JOIN RolesTelegaBot rt ON rt.id = rr.role_id
                    ORDER BY rr.role_id
                """
                rows = await self.db.execute_with_retry(query, fetchall=True) or []
                slug_to_id: Dict[str, int] = {}
                id_to_slug: Dict[int, str] = {}
                display: Dict[str, str] = {}
                admin_ids: Set[int] = set()
                for row in rows:
                    role_id = int(row.get("role_id"))
                    display_name = row.get("role_name") or f"Role {role_id}"
                    slug = self._normalize_role_slug(display_name)
                    slug_to_id[slug] = role_id
                    id_to_slug[role_id] = slug
                    display[slug] = display_name
                    if bool(row.get("can_manage_users")):
                        admin_ids.add(role_id)
                if slug_to_id:
                    self._role_slug_to_id = slug_to_id
                    self._role_id_to_slug = id_to_slug
                    self._role_display = display
                    self._admin_role_ids = admin_ids
            except Exception as exc:
                logger.exception("[ADMIN_REPO] Не удалось загрузить роли: %s", exc)
            finally:
                self._roles_loaded = True

    async def _get_role_id_by_slug(self, slug: str) -> Optional[int]:
        normalized = self._normalize_role_slug(slug)
        await self._load_roles()
        role_id = self._role_slug_to_id.get(normalized)
        if role_id is not None:
            return role_id
        await self._load_roles(force=True)
        return self._role_slug_to_id.get(normalized)

    async def _get_admin_role_ids(self) -> Set[int]:
        await self._load_roles()
        if self._admin_role_ids:
            return set(self._admin_role_ids)
        return set(ADMIN_ROLE_IDS)

    async def _get_operator_role_id(self) -> Optional[int]:
        return await self._get_role_id_by_slug("operator")

    
    @log_async_exceptions
    async def get_pending_users(self) -> List[Dict[str, Any]]:
        """Получает список Telegram пользователей со статусом pending."""
        logger.info("[ADMIN_REPO] Getting pending users")
        
        try:
            query = """
                {base}
                WHERE u.status = 'pending'
                ORDER BY u.id DESC
            """
            rows = await self.db.execute_with_retry(
                query.format(base=self.USER_FIELDS_BASE),
                fetchall=True,
            ) or []
            
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
                {base}
                WHERE u.user_id = %s
            """
            row = await self.db.execute_with_retry(
                query.format(base=self.USER_FIELDS_BASE),
                params=(telegram_id,),
                fetchone=True,
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
                {base}
                WHERE u.id = %s
                LIMIT 1
            """
            row = await self.db.execute_with_retry(
                query.format(base=self.USER_FIELDS_BASE),
                params=(user_id,),
                fetchone=True,
            )
            
            return self._attach_role_name(row)
        except Exception as e:
            logger.error(
                f"[ADMIN_REPO] Error getting user by id {user_id}: {e}\n{traceback.format_exc()}"
            )
            return None
    
    @log_async_exceptions
    async def approve_user(self, user_id: int, approver_id: int) -> bool:
        """
        Утверждает пользователя (pending -> approved).
        
        Args:
            user_id: внутренний ID пользователя (UsersTelegaBot.id)
            approver_id: Telegram ID утверждающего админа
        """
        logger.info(f"[ADMIN_REPO] Approving user #{user_id} by {approver_id}")
        
        try:
            approver_row = await self.db.execute_with_retry(
                "SELECT id FROM UsersTelegaBot WHERE user_id = %s",
                params=(approver_id,),
                fetchone=True
            )
            if not approver_row:
                logger.warning(f"[ADMIN_REPO] Approver {approver_id} not found")
                return False
            approver_db_id = (
                approver_row["id"] if isinstance(approver_row, dict) else approver_row
            )
            
            target_row = await self.db.execute_with_retry(
                "SELECT user_id FROM UsersTelegaBot WHERE id = %s",
                params=(user_id,),
                fetchone=True
            )
            target_telegram_id = (
                target_row.get("user_id") if isinstance(target_row, dict) else None
            ) if target_row else None
            
            # Обновляем статус пользователя
            query = """
                UPDATE UsersTelegaBot
                SET status = 'approved', approved_by = %s
                WHERE id = %s AND status = 'pending'
            """
            await self.db.execute_with_retry(
                query, params=(approver_db_id, user_id), commit=True
            )
            
            # Логируем действие (используем telegram IDs)
            await self.log_admin_action(
                actor_telegram_id=approver_id,
                action='approve',
                target_telegram_id=target_telegram_id,
                payload={'timestamp': datetime.now().isoformat()}
            )
            
            logger.info(f"[ADMIN_REPO] User #{user_id} approved successfully")
            return True
        except Exception as e:
            logger.error(
                f"[ADMIN_REPO] Error approving user #{user_id}: {e}\n{traceback.format_exc()}"
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
    async def _update_user_role(
        self,
        telegram_id: int,
        new_role: str,
        actor_telegram_id: int,
        action: str,
        *,
        only_approved: bool = False,
    ) -> bool:
        """Базовый метод смены роли пользователя."""
        logger.info(
            "[ADMIN_REPO] %s user %s -> role %s by %s",
            action,
            telegram_id,
            new_role,
            actor_telegram_id,
        )
        try:
            new_role_id = await self._get_role_id_by_slug(new_role)
            if not new_role_id:
                logger.warning(f"[ADMIN_REPO] Unknown role '{new_role}' for action {action}")
                return False
            
            status_clause = "AND status = 'approved'" if only_approved else ""
            query = f"""
                UPDATE UsersTelegaBot
                SET role_id = %s
                WHERE user_id = %s {status_clause}
            """
            await self.db.execute_with_retry(query, params=(new_role_id, telegram_id), commit=True)

            await self.log_admin_action(
                actor_telegram_id=actor_telegram_id,
                action=action,
                target_telegram_id=telegram_id,
                payload={'new_role': new_role},
            )

            logger.info("[ADMIN_REPO] User %s role updated to %s", telegram_id, new_role)
            return True
        except Exception as e:
            logger.error(
                f"[ADMIN_REPO] Error updating role for user {telegram_id}: {e}\n{traceback.format_exc()}"
            )
            return False
    
    async def promote_user(
        self, 
        telegram_id: int, 
        new_role: str, 
        promoter_telegram_id: int
    ) -> bool:
        """
        Повышает Telegram пользователя до новой роли.
        """
        return await self._update_user_role(
            telegram_id,
            new_role,
            promoter_telegram_id,
            action='promote',
            only_approved=True,
        )
    
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
        
        return await self._update_user_role(
            telegram_id,
            new_role,
            demoter_telegram_id,
            action='demote',
            only_approved=False,
        )
    
    async def set_user_role(
        self,
        telegram_id: int,
        new_role: str,
        actor_telegram_id: int,
    ) -> bool:
        """Явно устанавливает роль пользователя (wrap вокруг _update_user_role)."""
        return await self._update_user_role(
            telegram_id,
            new_role,
            actor_telegram_id,
            action='set_role',
            only_approved=False,
        )
    
    @log_async_exceptions
    async def get_admins(self) -> List[Dict[str, Any]]:
        """Получает список всех админов (admin/superadmin/developer/head_of_registry/founder) из UsersTelegaBot."""
        logger.info("[ADMIN_REPO] Getting admins list")
        
        try:
            admin_role_ids = await self._get_admin_role_ids()
            if not admin_role_ids:
                logger.warning("[ADMIN_REPO] Нет админских ролей в системе")
                return []
            placeholders = ', '.join(['%s'] * len(admin_role_ids))
            query = f"""
                {self.USER_FIELDS_BASE}
                WHERE u.role_id IN ({placeholders}) AND u.status != 'blocked'
                ORDER BY u.role_id DESC, u.full_name
            """
            rows = await self.db.execute_with_retry(
                query,
                params=tuple(admin_role_ids),
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
            operator_role_id = await self._get_operator_role_id()
            if not operator_role_id:
                logger.warning("[ADMIN_REPO] Не удалось определить role_id оператора")
                return []
            query = """
                {base}
                WHERE u.status = 'approved' AND (u.role_id IS NULL OR u.role_id = %s)
                ORDER BY u.id DESC
                LIMIT %s OFFSET %s
            """
            rows = await self.db.execute_with_retry(
                query.format(base=self.USER_FIELDS_BASE),
                params=(operator_role_id, limit, offset),
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
                    {base}
                    WHERE u.status = %s
                    ORDER BY u.id DESC
                """.format(base=self.USER_FIELDS_BASE)
                params = (status_filter,)
            else:
                query = """
                    {base}
                    ORDER BY u.id DESC
                """.format(base=self.USER_FIELDS_BASE)
                params = None
            
            rows = await self.db.execute_with_retry(
                query,
                params=params,
                fetchall=True,
            ) or []
            
            logger.info(f"[ADMIN_REPO] Found {len(rows)} users")
            return self._attach_role_names(rows)
        except Exception as e:
            logger.error(
                f"[ADMIN_REPO] Error getting all users: {e}\n{traceback.format_exc()}"
            )
            return []
    
    @log_async_exceptions
    async def get_users_counters(self) -> Dict[str, Any]:
        """
        Возвращает счётчики пользователей для Dashboard.
        """
        logger.info("[ADMIN_REPO] Getting user counters")
        
        try:
            admin_role_ids = await self._get_admin_role_ids()
            operator_role_id = await self._get_operator_role_id()
            if not admin_role_ids or operator_role_id is None:
                logger.warning("[ADMIN_REPO] Не удалось определить роли для подсчёта пользователей")
                return {}
            admin_placeholders = ', '.join(['%s'] * len(admin_role_ids))
            query = f"""
                SELECT 
                    COUNT(*) as total_users,
                    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending_users,
                    SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END) as approved_users,
                    SUM(CASE WHEN status = 'blocked' THEN 1 ELSE 0 END) as blocked_users,
                    SUM(CASE WHEN status = 'approved' AND role_id IN ({admin_placeholders}) THEN 1 ELSE 0 END) as admins_count,
                    SUM(CASE WHEN status = 'approved' AND role_id = %s THEN 1 ELSE 0 END) as operators_count
                FROM UsersTelegaBot
            """
            params = tuple(admin_role_ids) + (operator_role_id,)
            row = await self.db.execute_with_retry(
                query, params=params, fetchone=True
            ) or {}
            await self._load_roles()
            role_breakdown_query = """
                SELECT 
                    role_id,
                    COUNT(*) AS total_count,
                    SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END) AS approved_count,
                    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending_count,
                    SUM(CASE WHEN status = 'blocked' THEN 1 ELSE 0 END) AS blocked_count
                FROM UsersTelegaBot
                GROUP BY role_id
            """
            role_rows = await self.db.execute_with_retry(
                role_breakdown_query,
                fetchall=True,
            ) or []
            
            role_breakdown: Dict[str, Dict[str, int]] = {
                slug: {
                    'display': self._role_display.get(slug, slug.title()),
                    'total': 0,
                    'approved': 0,
                    'pending': 0,
                    'blocked': 0,
                }
                for slug in self._role_slug_to_id.keys()
            }
            
            for role_row in role_rows:
                slug = self._role_id_to_slug.get(int(role_row.get('role_id', 0)))
                if not slug:
                    slug = role_name_from_id(role_row.get('role_id'))
                if slug not in role_breakdown:
                    role_breakdown[slug] = {
                        'display': self._role_display.get(slug, slug.title()),
                        'total': 0,
                        'approved': 0,
                        'pending': 0,
                        'blocked': 0,
                    }
                role_breakdown[slug]['total'] = int(role_row.get('total_count') or 0)
                role_breakdown[slug]['approved'] = int(role_row.get('approved_count') or 0)
                role_breakdown[slug]['pending'] = int(role_row.get('pending_count') or 0)
                role_breakdown[slug]['blocked'] = int(role_row.get('blocked_count') or 0)
            
            admins_approved = sum(
                role_breakdown.get(slug, {}).get('approved', 0)
                for slug, role_id in self._role_slug_to_id.items()
                if role_id in admin_role_ids
            )
            
            counters = {
                'total_users': int(row.get('total_users') or 0),
                'pending_users': int(row.get('pending_users') or 0),
                'approved_users': int(row.get('approved_users') or 0),
                'blocked_users': int(row.get('blocked_users') or 0),
                'admins': int(row.get('admins_count') or admins_approved),
                'operators': int(row.get('operators_count') or 0),
                'roles_breakdown': role_breakdown,
            }
            
            logger.info(f"[ADMIN_REPO] Counters: {counters}")
            return counters
        except Exception as e:
            logger.error(
                f"[ADMIN_REPO] Error getting counters: {e}\n{traceback.format_exc()}"
            )
            fallback_roles = list(self._role_slug_to_id.keys()) or ["operator", "admin"]
            return {
                'total_users': 0,
                'pending_users': 0,
                'approved_users': 0,
                'blocked_users': 0,
                'admins': 0,
                'operators': 0,
                'roles_breakdown': {
                    role: {
                        'display': self._role_display.get(role, role.title()),
                        'total': 0,
                        'approved': 0,
                        'pending': 0,
                        'blocked': 0,
                    }
                    for role in fallback_roles
                },
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
            if target_role == "superadmin":
                role_id = await self._get_role_id_by_slug("admin")
            else:
                role_id = await self._get_role_id_by_slug("operator")
            if not role_id:
                logger.warning("[ADMIN_REPO] Не удалось определить role_id для %s", target_role)
                return []

            query = """
                {base}
                WHERE u.status = 'approved' AND u.role_id = %s
                ORDER BY u.id DESC
                LIMIT %s
            """
            rows = await self.db.execute_with_retry(
                query.format(base=self.USER_FIELDS_BASE),
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
    
    async def _get_internal_id(self, telegram_id: int) -> Optional[int]:
        """Получает внутренний ID пользователя по Telegram ID."""
        if not telegram_id:
            return None
        row = await self.db.execute_with_retry(
            "SELECT id FROM UsersTelegaBot WHERE user_id = %s",
            params=(telegram_id,),
            fetchone=True
        )
        if row:
            return row['id'] if isinstance(row, dict) else row[0]
        return None

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
        
        ВАЖНО: Принимает telegram IDs, но сохраняет внутренние DB IDs!
        
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
            # Получаем внутренние ID
            actor_id = await self._get_internal_id(actor_telegram_id)
            target_id = await self._get_internal_id(target_telegram_id) if target_telegram_id else None
            
            if not actor_id:
                logger.warning(f"[ADMIN_REPO] Actor {actor_telegram_id} not found in DB, logging with NULL actor_id")
            
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
            # JOIN теперь по внутренним ID (l.actor_id = a.id)
            if actor_telegram_id:
                actor_internal_id = await self._get_internal_id(actor_telegram_id)
                if not actor_internal_id:
                    return []
                    
                query = """
                    SELECT l.*, 
                           a.full_name as actor_name,
                           t.full_name as target_name,
                           a.user_id as actor_telegram_id,
                           t.user_id as target_telegram_id
                    FROM admin_action_logs l
                    LEFT JOIN UsersTelegaBot a ON l.actor_id = a.id
                    LEFT JOIN UsersTelegaBot t ON l.target_id = t.id
                    WHERE l.actor_id = %s
                    ORDER BY l.created_at DESC
                    LIMIT %s
                """
                params = (actor_internal_id, limit)
            else:
                query = """
                    SELECT l.*, 
                           a.full_name as actor_name,
                           t.full_name as target_name,
                           a.user_id as actor_telegram_id,
                           t.user_id as target_telegram_id
                    FROM admin_action_logs l
                    LEFT JOIN UsersTelegaBot a ON l.actor_id = a.id
                    LEFT JOIN UsersTelegaBot t ON l.target_id = t.id
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
