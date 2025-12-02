"""
Вспомогательные структуры для работы с ролями пользователей.
"""

from typing import Dict

ROLE_ID_TO_NAME: Dict[int, str] = {
    1: "operator",
    2: "admin",
    3: "superadmin",
}

ROLE_NAME_TO_ID: Dict[str, int] = {name: role_id for role_id, name in ROLE_ID_TO_NAME.items()}

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
