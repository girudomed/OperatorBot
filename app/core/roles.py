"""
Роли и права пользователей на основе таблицы roles_reference.

Роли:
    1 - Оператор
    2 - Администратор (старший админ регистратуры)
    3 - Маркетолог
    4 - ЗавРег (заведующий регистратурой)
    5 - Ст.админ (старший администратор)
    6 - Руководство
    7 - SuperAdmin
    8 - Dev
"""

from typing import Dict, Optional
from dataclasses import dataclass


@dataclass
class RolePermissions:
    """Права роли согласно roles_reference."""
    can_view_own_stats: bool = False
    can_view_all_stats: bool = False
    can_manage_users: bool = False
    can_debug: bool = False


# Соответствие role_id -> имя роли
ROLE_ID_TO_NAME: Dict[int, str] = {
    1: "operator",
    2: "admin",          # Администратор / Ст.админ рег.
    3: "marketer",       # Маркетолог
    4: "zavreg",         # ЗавРег
    5: "senior_admin",   # Ст.админ
    6: "management",     # Руководство
    7: "superadmin",     # SuperAdmin
    8: "dev",            # Dev
}

ROLE_NAME_TO_ID: Dict[str, int] = {name: role_id for role_id, name in ROLE_ID_TO_NAME.items()}

# Человекочитаемые названия ролей
ROLE_DISPLAY_NAMES: Dict[int, str] = {
    1: "Оператор",
    2: "Администратор",
    3: "Маркетолог",
    4: "ЗавРег",
    5: "Ст.админ",
    6: "Руководство",
    7: "SuperAdmin",
    8: "Dev",
}

# Права ролей согласно roles_reference
ROLE_PERMISSIONS: Dict[int, RolePermissions] = {
    1: RolePermissions(can_view_own_stats=True, can_view_all_stats=False, can_manage_users=False, can_debug=False),
    2: RolePermissions(can_view_own_stats=True, can_view_all_stats=True, can_manage_users=True, can_debug=False),
    3: RolePermissions(can_view_own_stats=True, can_view_all_stats=True, can_manage_users=False, can_debug=False),
    4: RolePermissions(can_view_own_stats=True, can_view_all_stats=True, can_manage_users=True, can_debug=False),
    5: RolePermissions(can_view_own_stats=True, can_view_all_stats=True, can_manage_users=True, can_debug=False),
    6: RolePermissions(can_view_own_stats=True, can_view_all_stats=True, can_manage_users=False, can_debug=False),
    7: RolePermissions(can_view_own_stats=True, can_view_all_stats=True, can_manage_users=True, can_debug=True),
    8: RolePermissions(can_view_own_stats=True, can_view_all_stats=True, can_manage_users=True, can_debug=True),
}

# Роли с админскими правами (can_manage_users=True)
ADMIN_ROLE_IDS = {2, 4, 5, 7, 8}

# Роли с полным доступом к статистике (can_view_all_stats=True)
STATS_VIEWER_ROLE_IDS = {2, 3, 4, 5, 6, 7, 8}

# Роли с правами отладки (can_debug=True) 
DEBUG_ROLE_IDS = {7, 8}

DEFAULT_ROLE_ID = 1


def role_name_from_id(role_id: int | None) -> str:
    """Возвращает строковое имя роли по role_id."""
    if role_id is None:
        return ROLE_ID_TO_NAME[DEFAULT_ROLE_ID]
    return ROLE_ID_TO_NAME.get(int(role_id), ROLE_ID_TO_NAME[DEFAULT_ROLE_ID])


def role_id_from_name(role_name: str) -> int:
    """Возвращает role_id по имени роли."""
    normalized = (role_name or "").lower()
    return ROLE_NAME_TO_ID.get(normalized, DEFAULT_ROLE_ID)


def role_display_name(role_id: int | None) -> str:
    """Возвращает человекочитаемое название роли."""
    if role_id is None:
        return ROLE_DISPLAY_NAMES[DEFAULT_ROLE_ID]
    return ROLE_DISPLAY_NAMES.get(int(role_id), ROLE_DISPLAY_NAMES[DEFAULT_ROLE_ID])


def get_role_permissions(role_id: int | None) -> RolePermissions:
    """Возвращает права для указанной роли."""
    if role_id is None:
        return ROLE_PERMISSIONS[DEFAULT_ROLE_ID]
    return ROLE_PERMISSIONS.get(int(role_id), ROLE_PERMISSIONS[DEFAULT_ROLE_ID])


def can_view_own_stats(role_id: int | None) -> bool:
    """Может ли роль просматривать свою статистику."""
    return get_role_permissions(role_id).can_view_own_stats


def can_view_all_stats(role_id: int | None) -> bool:
    """Может ли роль просматривать статистику всех операторов."""
    return get_role_permissions(role_id).can_view_all_stats


def can_manage_users(role_id: int | None) -> bool:
    """Может ли роль управлять пользователями."""
    return get_role_permissions(role_id).can_manage_users


def can_debug(role_id: int | None) -> bool:
    """Имеет ли роль отладочные права."""
    return get_role_permissions(role_id).can_debug


def is_admin_role(role_id: int | None) -> bool:
    """Проверяет, является ли роль админской."""
    if role_id is None:
        return False
    return int(role_id) in ADMIN_ROLE_IDS


def is_superadmin_or_higher(role_id: int | None) -> bool:
    """Проверяет, имеет ли роль максимальные права (SuperAdmin/Dev)."""
    if role_id is None:
        return False
    return int(role_id) in {7, 8}

