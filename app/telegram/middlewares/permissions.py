# Файл: app/telegram/middlewares/permissions.py

"""
Менеджер прав доступа для админ-панели.

Использует таблицы UsersTelegaBot и roles_reference.
Таблица users - это Mango справочник (НЕ использовать для ролей!).
Поддерживает Supreme Admin и Dev Admin из конфигурации.
"""

from __future__ import annotations

import asyncio
import time
from copy import deepcopy
from typing import Optional, Dict, Set, Tuple, Any, Literal, List
from app.db.manager import DatabaseManager
from app.logging_config import get_watchdog_logger
from app.config import SUPREME_ADMIN_ID, SUPREME_ADMIN_USERNAME, DEV_ADMIN_ID, DEV_ADMIN_USERNAME
from app.core.roles import role_display_name_from_name, ROLE_ID_TO_NAME
logger = get_watchdog_logger(__name__)


def _normalize_username(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return value.lstrip("@").lower()


_SUPREME_ADMIN_USERNAME = _normalize_username(SUPREME_ADMIN_USERNAME)
_DEV_ADMIN_USERNAME = _normalize_username(DEV_ADMIN_USERNAME)

Role = str
Status = Literal['pending', 'approved', 'blocked']

# Настройка прав приложения по умолчанию (fallback, если не удалось загрузить из БД)
DEFAULT_APP_PERMISSIONS: Dict[str, Set[str]] = {
    'operator': {'call_lookup', 'weekly_quality', 'report'},
    'admin': {'call_lookup', 'weekly_quality', 'admin_panel', 'user_management', 'report', 'all_stats', 'commands'},
    'head_of_registry': {'call_lookup', 'weekly_quality', 'admin_panel', 'user_management', 'manage_roles', 'report', 'all_stats', 'commands'},
    'marketing_director': {'call_lookup', 'weekly_quality', 'report', 'all_stats', 'commands'},
    'superadmin': {'call_lookup', 'weekly_quality', 'admin_panel', 'user_management', 'manage_roles', 'report', 'all_stats', 'debug', 'commands'},
    'developer': {'call_lookup', 'weekly_quality', 'admin_panel', 'user_management', 'manage_roles', 'report', 'all_stats', 'debug', 'commands'},
    'founder': {'call_lookup', 'weekly_quality', 'admin_panel', 'user_management', 'manage_roles', 'report', 'all_stats', 'debug', 'commands'},
}

DEFAULT_ROLE_MATRIX: Dict[int, Dict[str, Any]] = {
    1: {
        "slug": "operator",
        "display_name": "Оператор",
        "can_view_own_stats": True,
        "can_view_all_stats": False,
        "can_manage_users": False,
        "can_debug": False,
        "app_permissions": DEFAULT_APP_PERMISSIONS["operator"],
    },
    2: {
        "slug": "admin",
        "display_name": "Администратор",
        "can_view_own_stats": True,
        "can_view_all_stats": True,
        "can_manage_users": True,
        "can_debug": False,
        "app_permissions": DEFAULT_APP_PERMISSIONS["admin"],
    },
    3: {
        "slug": "superadmin",
        "display_name": "Суперадмин",
        "can_view_own_stats": True,
        "can_view_all_stats": True,
        "can_manage_users": True,
        "can_debug": True,
        "app_permissions": DEFAULT_APP_PERMISSIONS["superadmin"],
    },
    4: {
        "slug": "developer",
        "display_name": "Developer",
        "can_view_own_stats": True,
        "can_view_all_stats": True,
        "can_manage_users": True,
        "can_debug": True,
        "app_permissions": DEFAULT_APP_PERMISSIONS["developer"],
    },
    5: {
        "slug": "head_of_registry",
        "display_name": "Зав. регистратуры",
        "can_view_own_stats": True,
        "can_view_all_stats": True,
        "can_manage_users": True,
        "can_debug": False,
        "app_permissions": DEFAULT_APP_PERMISSIONS["head_of_registry"],
    },
    6: {
        "slug": "founder",
        "display_name": "Founder",
        "can_view_own_stats": True,
        "can_view_all_stats": True,
        "can_manage_users": True,
        "can_debug": True,
        "app_permissions": DEFAULT_APP_PERMISSIONS["founder"],
    },
    7: {
        "slug": "marketing_director",
        "display_name": "Директор по маркетингу",
        "can_view_own_stats": True,
        "can_view_all_stats": True,
        "can_manage_users": False,
        "can_debug": False,
        "app_permissions": DEFAULT_APP_PERMISSIONS["marketing_director"],
    },
}

DEFAULT_TOP_PRIVILEGE_SLUGS = {'developer', 'founder'}
DEFAULT_SUPERADMIN_SLUGS = {'superadmin'}
DEFAULT_EXCLUDE_SLUGS = {
    'developer',
    'founder',
    'superadmin',
    'head_of_registry',
    'marketing_director',
}
SUPERADMIN_MANAGEABLE_ROLES: Set[Role] = {'superadmin', 'admin', 'operator', 'marketing_director'}
ADMIN_MANAGEABLE_ROLES: Set[Role] = {'admin', 'operator'}

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
        self._roles_loaded = False
        self._roles_lock = asyncio.Lock()
        self._role_matrix: Dict[int, Dict[str, Any]] = {}
        self._role_id_to_slug: Dict[int, str] = {}
        self._role_slug_to_id: Dict[str, int] = {}
        self._roles_by_slug: Dict[str, Dict[str, Any]] = {}
        self._admin_roles: Set[Role] = set()
        self._stats_roles: Set[Role] = set()
        self._debug_roles: Set[Role] = set()
        self._top_privilege_roles: Set[Role] = set()
        self._super_roles: Set[Role] = set()
        self._role_manage_roles: Set[Role] = set()
        self._exclude_roles: Set[Role] = set()
        self._set_role_matrix(DEFAULT_ROLE_MATRIX)

    def _set_role_matrix(self, matrix: Dict[int, Dict[str, Any]]) -> None:
        """Сохраняет матрицу ролей и производные множества."""
        # Глубокая копия, чтобы не мутировать дефолт
        cloned: Dict[int, Dict[str, Any]] = {}
        for role_id, meta in matrix.items():
            cloned_meta = deepcopy(meta)
            cloned_meta["app_permissions"] = set(cloned_meta.get("app_permissions", set()))
            cloned[role_id] = cloned_meta

        self._role_matrix = cloned
        self._role_id_to_slug = {
            int(role_id): cloned_meta["slug"] for role_id, cloned_meta in cloned.items()
        }
        self._role_slug_to_id = {
            cloned_meta["slug"]: int(role_id) for role_id, cloned_meta in cloned.items()
        }
        self._roles_by_slug = {
            cloned_meta["slug"]: cloned_meta for cloned_meta in cloned.values()
        }
        self._admin_roles = {
            meta["slug"]
            for meta in cloned.values()
            if meta.get("can_manage_users")
        }
        self._stats_roles = {
            meta["slug"]
            for meta in cloned.values()
            if meta.get("can_view_all_stats")
        }
        self._debug_roles = {
            meta["slug"]
            for meta in cloned.values()
            if meta.get("can_debug")
        }
        self._top_privilege_roles = {
            meta["slug"]
            for meta in cloned.values()
            if meta["slug"] in DEFAULT_TOP_PRIVILEGE_SLUGS
        }
        self._super_roles = {
            meta["slug"]
            for meta in cloned.values()
            if meta["slug"] in DEFAULT_SUPERADMIN_SLUGS
        }
        self._role_manage_roles = {'head_of_registry'} | self._super_roles | self._top_privilege_roles
        self._exclude_roles = {
            meta["slug"]
            for meta in cloned.values()
            if meta["slug"] in DEFAULT_EXCLUDE_SLUGS
        }

    @staticmethod
    def _normalize_slug(value: Optional[str]) -> str:
        slug = (value or "").strip().lower()
        slug = slug.replace(" ", "_")
        return slug or "role"

    async def _ensure_roles_loaded(self) -> None:
        if self._roles_loaded:
            return
        async with self._roles_lock:
            if self._roles_loaded:
                return
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
                    ORDER BY rr.role_id
                """
                rows_raw = await self.db_manager.execute_with_retry(
                    query,
                    fetchall=True,
                )
                rows: List[Dict[str, Any]] = []
                if isinstance(rows_raw, dict):
                    rows = [rows_raw]
                elif isinstance(rows_raw, list):
                    rows = [
                        row for row in rows_raw
                        if isinstance(row, dict)
                    ]
                elif rows_raw is None:
                    rows = []
                else:
                    logger.warning(
                        "[PERMISSIONS] roles_reference вернула неожиданный тип: %s",
                        type(rows_raw),
                    )
                if not rows:
                    logger.warning(
                        "[PERMISSIONS] roles_reference пустая, используем значения по умолчанию"
                    )
                    self._roles_loaded = True
                    return
                matrix: Dict[int, Dict[str, Any]] = {}
                for row in rows:
                    role_id = int(row.get("role_id"))
                    raw_name = (row.get("slug") or "").strip()
                    alias = ROLE_ID_TO_NAME.get(role_id)
                    slug = alias or self._normalize_slug(raw_name) or f"role_{role_id}"
                    display_name = raw_name or role_display_name_from_name(slug)
                    can_view_all_stats = bool(row.get("can_view_all_stats"))
                    can_manage_users = bool(row.get("can_manage_users"))
                    can_debug = bool(row.get("can_debug"))
                    can_view_own_stats = bool(row.get("can_view_own_stats"))
                    app_perm: Set[str] = {"call_lookup", "weekly_quality"}
                    if can_view_own_stats:
                        app_perm.add("report")
                    if can_view_all_stats:
                        app_perm.update({"all_stats", "report"})
                    if can_manage_users:
                        app_perm.update({"admin_panel", "user_management", "manage_roles"})
                    if can_debug:
                        app_perm.add("debug")
                    matrix[role_id] = {
                        "slug": slug,
                        "display_name": display_name or slug.replace("_", " ").title(),
                        "can_view_own_stats": can_view_own_stats,
                        "can_view_all_stats": can_view_all_stats,
                        "can_manage_users": can_manage_users,
                        "can_debug": can_debug,
                        "app_permissions": set(app_perm),
                    }
                self._set_role_matrix(matrix)
                logger.info("[PERMISSIONS] Загружено %s ролей из roles_reference", len(matrix))
            except Exception as exc:
                logger.exception(
                    "Не удалось загрузить роли из БД, используем значения по умолчанию: %s",
                    exc,
                )
            finally:
                self._roles_loaded = True

    async def get_role_display_name(self, role_slug: Role) -> str:
        await self._ensure_roles_loaded()
        meta = self._roles_by_slug.get(role_slug)
        if not meta:
            return role_slug
        return meta.get("display_name") or role_slug.capitalize()

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

    @staticmethod
    def _normalize_status_value(status: Optional[str]) -> Optional[str]:
        if status is None:
            return None
        normalized = status.strip().lower()
        return normalized or None

    def _set_cache_entry(self, user_id: int, role: Optional[Role], status: Optional[Status]) -> None:
        normalized = self._normalize_status_value(status)
        self._user_cache[user_id] = (role, normalized, time.monotonic())

    async def _role_slug_from_id(self, role_id: Optional[int]) -> Role:
        await self._ensure_roles_loaded()
        if role_id is None:
            return "operator"
        return self._role_id_to_slug.get(int(role_id), "operator")
    
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
            
            status = self._normalize_status_value(row.get('status'))
            role = await self._role_slug_from_id(row.get('role_id'))
            self._set_cache_entry(user_id, role, status)

            if status != 'approved':
                logger.debug(f"User {user_id} status is {status}, not approved")
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
            status = self._normalize_status_value(row.get('status') if row else None)
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
        normalized = _normalize_username(username)
        if _SUPREME_ADMIN_USERNAME and normalized == _SUPREME_ADMIN_USERNAME:
            return True
        return False
    
    def is_dev_admin(self, user_id: int, username: Optional[str] = None) -> bool:
        """
        Проверяет является ли пользователь Dev Admin (из конфига).
        """
        if DEV_ADMIN_ID and str(user_id) == str(DEV_ADMIN_ID):
            return True
        normalized = _normalize_username(username)
        if _DEV_ADMIN_USERNAME and normalized == _DEV_ADMIN_USERNAME:
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
        
        await self._ensure_roles_loaded()
        role = await self.get_user_role(user_id)
        return role in self._admin_roles
    
    async def is_superadmin(self, user_id: int, username: Optional[str] = None) -> bool:
        """
        Проверяет имеет ли пользователь роль superadmin/developer/founder.
        """
        if self.is_supreme_admin(user_id, username) or self.is_dev_admin(user_id, username):
            return True
        
        await self._ensure_roles_loaded()
        role = await self.get_user_role(user_id)
        return role in self._super_roles or role in self._top_privilege_roles

    async def has_top_privileges(self, user_id: int, username: Optional[str] = None) -> bool:
        """
        Возвращает True только для ролей developer/founder (и bootstrap-админов).
        Используется для операций, доступных исключительно владельцам продукта.
        """
        if self.is_supreme_admin(user_id, username) or self.is_dev_admin(user_id, username):
            return True
        await self._ensure_roles_loaded()
        role = await self.get_user_role(user_id)
        return role in self._top_privilege_roles
    
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
        await self._ensure_roles_loaded()
        # Supreme и Dev могут всё
        if self.is_supreme_admin(actor_id, actor_username) or self.is_dev_admin(actor_id, actor_username):
            return True
        
        actor_role = await self.get_user_role(actor_id)
        if not actor_role:
            return False
        
        if actor_role in self._top_privilege_roles:
            return True  # Founder/Developer могут всё
        
        if actor_role in self._super_roles:
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
        await self._ensure_roles_loaded()
        if self.is_supreme_admin(actor_id, actor_username) or self.is_dev_admin(actor_id, actor_username):
            return True
        
        actor_role = await self.get_user_role(actor_id)
        target_role = await self.get_user_role(target_id)
        
        if not actor_role or not target_role:
            return False

        if target_role in self._top_privilege_roles:
            return actor_role in self._top_privilege_roles
        
        if target_role == 'head_of_registry':
            return actor_role in self._top_privilege_roles
        
        if actor_role in self._top_privilege_roles:
            return True
        
        if actor_role in self._super_roles:
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

    async def can_manage_users(self, user_id: int, username: Optional[str] = None) -> bool:
        """
        Проверяет, может ли пользователь управлять учетками (approve/decline/block).
        """
        if self.is_supreme_admin(user_id, username) or self.is_dev_admin(user_id, username):
            return True
        await self._ensure_roles_loaded()
        role = await self.get_user_role(user_id)
        return role in self._admin_roles
    
    async def can_view_all_operators(self, user_id: int, username: Optional[str] = None) -> bool:
        """
        Проверяет может ли пользователь видеть всех операторов.
        """
        return await self.is_admin(user_id, username)
    
    async def can_view_all_stats(self, user_id: int, username: Optional[str] = None) -> bool:
        """
        Проверяет право can_view_all_stats.
        """
        if self.is_supreme_admin(user_id, username) or self.is_dev_admin(user_id, username):
            return True
        await self._ensure_roles_loaded()
        role = await self.get_user_role(user_id)
        if not role:
            return False
        return role in self._stats_roles
    
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
        role = await self.get_user_role(user_id)
        if role:
            return role
        if self.is_supreme_admin(user_id, username):
            return 'founder'
        if self.is_dev_admin(user_id, username):
            return 'developer'
        return 'operator'

    async def check_permission(self, role_name: Role, required_permission: str) -> bool:
        """
        Проверяет, есть ли у роли доступ к указанному разрешению.
        SuperAdmin и Dev имеют доступ ко всем действиям.
        """
        await self._ensure_roles_loaded()
        if role_name in self._top_privilege_roles:
            return True
        meta = self._roles_by_slug.get(role_name)
        if not meta:
            allowed = DEFAULT_APP_PERMISSIONS.get(role_name, set())
        else:
            allowed = meta.get("app_permissions", set())
        return required_permission in allowed

    async def has_permission(
        self,
        user_id: int,
        required_permission: str,
        username: Optional[str] = None,
        require_approved: bool = True,
    ) -> bool:
        """
        Универсальная проверка app_permission для пользователя.
        """
        if self.is_supreme_admin(user_id, username) or self.is_dev_admin(user_id, username):
            return True
        if require_approved:
            status = await self.get_user_status(user_id)
            if status != 'approved':
                return False
        role = await self.get_effective_role(user_id, username)
        return await self.check_permission(role, required_permission)

    async def list_roles(self) -> List[Dict[str, str]]:
        """
        Возвращает список ролей с отображаемыми именами.
        """
        await self._ensure_roles_loaded()
        return [
            {
                "slug": slug,
                "display_name": meta.get("display_name") or slug.capitalize(),
            }
            for slug, meta in self._roles_by_slug.items()
        ]

    async def can_manage_roles(self, user_id: int, username: Optional[str] = None) -> bool:
        """
        Проверяет, доступно ли управление ролями.
        """
        if self.is_supreme_admin(user_id, username) or self.is_dev_admin(user_id, username):
            return True
        await self._ensure_roles_loaded()
        role = await self.get_user_role(user_id)
        return role in self._role_manage_roles

    async def can_exclude_user(self, user_id: int, username: Optional[str] = None) -> bool:
        """
        Проверяет, может ли пользователь исключать (блокировать/удалять) других пользователей.
        """
        if self.is_supreme_admin(user_id, username) or self.is_dev_admin(user_id, username):
            return True
        await self._ensure_roles_loaded()
        role = await self.get_user_role(user_id)
        return role in self._exclude_roles
