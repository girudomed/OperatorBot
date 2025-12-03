"""
Репозиторий для работы с пользователями и ролями.
"""

from typing import Optional, Union, Dict

from app.db.manager import DatabaseManager
from app.db.models import UserRecord, RoleRecord
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)


class UserRepository:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    async def register_user_if_not_exists(
        self, 
        user_id: int, 
        username: str, 
        full_name: str, 
        operator_id: Optional[int] = None, 
        password: Optional[str] = None, 
        role_id: Optional[int] = None
    ) -> None:
        """Регистрация пользователя, если он не существует в базе данных."""
        if not await self.user_exists(user_id):
            if password is None:
                raise ValueError("Пароль не может быть пустым при регистрации нового пользователя.")
            query_insert = """
                INSERT INTO UsersTelegaBot (user_id, username, full_name, operator_id, password, role_id)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            await self.db_manager.execute_query(
                query_insert, 
                (user_id, username, full_name, operator_id, password, role_id)
            )
            logger.info(f"Пользователь '{full_name}' зарегистрирован.")
        else:
            logger.info(f"Пользователь '{full_name}' уже существует.")

    async def user_exists(self, user_id: int) -> bool:
        """Проверка существования пользователя по user_id."""
        query = "SELECT 1 FROM UsersTelegaBot WHERE user_id = %s"
        result = await self.db_manager.execute_query(query, (user_id,), fetchone=True)
        return bool(result)

    async def get_user_by_id(self, user_id: int) -> Optional[UserRecord]:
        """Получение пользователя по user_id."""
        query = "SELECT * FROM UsersTelegaBot WHERE user_id = %s"
        result = await self.db_manager.execute_query(query, (user_id,), fetchone=True)
        if not result or not isinstance(result, dict):
            logger.warning(f"Пользователь с ID {user_id} не найден.")
            return None
        return result

    async def get_user_role(self, user_id: int) -> Optional[int]:
        """Получение роли пользователя по user_id."""
        query = "SELECT role_id FROM UsersTelegaBot WHERE user_id = %s"
        user_role = await self.db_manager.execute_query(query, (user_id,), fetchone=True)
        if not user_role:
            logger.warning(f"Роль для пользователя с ID {user_id} не найдена.")
            return None
        return user_role.get('role_id')

    async def update_user_password(self, user_id: int, hashed_password: str) -> None:
        """Обновление хешированного пароля пользователя."""
        query = "UPDATE UsersTelegaBot SET password = %s WHERE user_id = %s"
        await self.db_manager.execute_query(query, (hashed_password, user_id))
        logger.info(f"Пароль для user_id {user_id} успешно обновлен.")

    async def get_user_password(self, user_id: int) -> Optional[Dict[str, str]]:
        """Получение хешированного пароля пользователя по его user_id."""
        query = "SELECT password FROM UsersTelegaBot WHERE user_id = %s"
        result = await self.db_manager.execute_query(query, (user_id,), fetchone=True)
        if not result or not isinstance(result, dict):
            logger.warning(f"Пользователь с ID {user_id} не найден.")
            return None
        return result

    async def get_role_password_by_id(self, role_id: int) -> Optional[str]:
        """Получает пароль роли по role_id из таблицы RolesTelegaBot."""
        query = "SELECT role_password FROM RolesTelegaBot WHERE id = %s"
        result = await self.db_manager.execute_query(query, (role_id,), fetchone=True)
        if result:
            return result.get('role_password')
        return None

    async def get_role_id_by_name(self, role_name: str) -> Optional[Dict[str, int]]:
        """Получение role_id по названию роли."""
        query = "SELECT id FROM RolesTelegaBot WHERE role_name = %s"
        result = await self.db_manager.execute_query(query, (role_name,), fetchone=True)
        if not result or not isinstance(result, dict):
            logger.warning(f"Роль с именем {role_name} не найдена.")
            return None
        return result

    async def get_role_name_by_id(self, role_id: int) -> Optional[Dict[str, str]]:
        """Получение названия роли по role_id."""
        query = "SELECT role_name FROM RolesTelegaBot WHERE id = %s"
        result = await self.db_manager.execute_query(query, (role_id,), fetchone=True)
        if not result or not isinstance(result, dict):
            logger.warning(f"Роль с ID {role_id} не найдена.")
            return None
        return result
