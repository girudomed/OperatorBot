# Файл: app/db/repositories/users.py

"""
Репозиторий для работы с Telegram пользователями и ролями.
ВАЖНО: Использует таблицу UsersTelegaBot, а НЕ users!
Таблица users - это справочник Mango (телефонные пользователи).
"""

from typing import Optional, Dict

from app.db.manager import DatabaseManager
from app.db.models import UserRecord
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)


class UserRepository:
    """
    Repository для работы с Telegram пользователями.
    Использует таблицу UsersTelegaBot для ролей, статусов, и связи с Telegram.
    """
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    async def register_telegram_user(
        self, 
        user_id: int,  # Telegram ID
        username: Optional[str], 
        full_name: str,
        role_id: int = 1,  # По умолчанию - Оператор
        status: str = 'pending',  # pending, approved, blocked
        operator_name: Optional[str] = None,
        extension: Optional[str] = None
    ) -> None:
        """
        Регистрация Telegram пользователя в UsersTelegaBot.
        
        Args:
            user_id: Telegram user ID
            username: Telegram username (@username)
            full_name: Полное имя
            role_id: ID роли (по умолчанию 1 = Оператор)
            status: Статус (pending/approved/blocked)
            operator_name: Имя оператора для связки с users (Mango)
            extension: Extension для связки с users (Mango)
        """
        logger.info(f"[USER_REPO] Registering Telegram user: {user_id}, {full_name}")
        
        try:
            if not await self.user_exists(user_id):
                query_insert = """
                    INSERT INTO UsersTelegaBot 
                        (user_id, full_name, role_id, status, operator_name, extension)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                await self.db_manager.execute_query(
                    query_insert, 
                    (user_id, full_name, role_id, status, operator_name, extension)
                )
                logger.info(f"[USER_REPO] Telegram user '{full_name}' registered successfully")
            else:
                logger.info(f"[USER_REPO] Telegram user '{full_name}' already exists")
        except Exception as e:
            logger.error(f"[USER_REPO] Error registering user {user_id}: {e}", exc_info=True)
            raise

    async def register_user_if_not_exists(
        self,
        *,
        user_id: int,
        username: Optional[str],
        full_name: str,
        operator_id: Optional[int] = None,
        role_id: int = 1,
    ) -> None:
        """
        Идемпотентная регистрация пользователя из Telegram.

        Используется новым AuthManager — создаёт запись со статусом pending,
        либо обновляет базовые данные, если пользователь уже существует.
        """
        logger.info(
            "[USER_REPO] Upserting Telegram user %s (role_id=%s, operator_id=%s)",
            user_id,
            role_id,
            operator_id,
        )
        existing = await self.db_manager.execute_with_retry(
            "SELECT id FROM UsersTelegaBot WHERE user_id = %s",
            params=(user_id,),
            fetchone=True,
        )
        if existing:
            await self.db_manager.execute_with_retry(
                """
                UPDATE UsersTelegaBot
                SET username = %s,
                    full_name = %s,
                    operator_id = %s,
                    updated_at = NOW()
                WHERE user_id = %s
                """,
                params=(username, full_name, operator_id, user_id),
                commit=True,
            )
            logger.info("[USER_REPO] Telegram user %s already existed, data refreshed.", user_id)
            return

        await self.db_manager.execute_with_retry(
            """
            INSERT INTO UsersTelegaBot (
                user_id,
                username,
                full_name,
                operator_id,
                role_id,
                status
            ) VALUES (%s, %s, %s, %s, %s, 'pending')
            """,
            params=(user_id, username, full_name, operator_id, role_id),
            commit=True,
        )
        logger.info("[USER_REPO] Telegram user %s inserted with status pending.", user_id)

    async def user_exists(self, user_id: int) -> bool:
        """Проверка существования Telegram пользователя по user_id."""
        query = "SELECT 1 FROM UsersTelegaBot WHERE user_id = %s"
        result = await self.db_manager.execute_query(query, (user_id,), fetchone=True)
        return bool(result)

    async def get_user_by_telegram_id(self, user_id: int) -> Optional[UserRecord]:
        """Получение Telegram пользователя по user_id."""
        logger.debug(f"[USER_REPO] Getting user by Telegram ID: {user_id}")
        
        try:
            query = """
            SELECT 
                user_id,
                full_name,
                role_id,
                status,
                operator_name,
                extension,
                approved_by,
                blocked_at
            FROM UsersTelegaBot 
            WHERE user_id = %s
            """
            result = await self.db_manager.execute_query(query, (user_id,), fetchone=True)
            
            if not result or not isinstance(result, dict):
                logger.warning(f"[USER_REPO] Telegram user with ID {user_id} not found")
                return None
            
            logger.debug(f"[USER_REPO] Found user: {result.get('full_name')}, role_id={result.get('role_id')}")
            return result
        except Exception as e:
            logger.error(f"[USER_REPO] Error getting user {user_id}: {e}", exc_info=True)
            return None

    async def get_user_role(self, user_id: int) -> Optional[int]:
        """Получение роли Telegram пользователя по user_id."""
        logger.debug(f"[USER_REPO] Getting role for user: {user_id}")
        
        try:
            query = "SELECT role_id FROM UsersTelegaBot WHERE user_id = %s"
            user_role = await self.db_manager.execute_query(query, (user_id,), fetchone=True)
            
            if not user_role:
                logger.warning(f"[USER_REPO] Role for user {user_id} not found")
                return None
            
            role_id = user_role.get('role_id')
            logger.debug(f"[USER_REPO] User {user_id} has role_id={role_id}")
            return role_id
        except Exception as e:
            logger.error(f"[USER_REPO] Error getting role for user {user_id}: {e}", exc_info=True)
            return None

    async def update_user_role(self, user_id: int, new_role_id: int) -> None:
        """Обновление роли пользователя."""
        logger.info(f"[USER_REPO] Updating role for user {user_id} to {new_role_id}")
        
        try:
            query = "UPDATE UsersTelegaBot SET role_id = %s WHERE user_id = %s"
            await self.db_manager.execute_query(query, (new_role_id, user_id))
            logger.info(f"[USER_REPO] Role updated successfully")
        except Exception as e:
            logger.error(f"[USER_REPO] Error updating role for user {user_id}: {e}", exc_info=True)
            raise

    async def update_user_status(self, user_id: int, new_status: str) -> None:
        """Обновление статуса пользователя (pending/approved/blocked)."""
        logger.info(f"[USER_REPO] Updating status for user {user_id} to {new_status}")
        
        try:
            query = "UPDATE UsersTelegaBot SET status = %s WHERE user_id = %s"
            await self.db_manager.execute_query(query, (new_status, user_id))
            logger.info(f"[USER_REPO] Status updated successfully")
        except Exception as e:
            logger.error(f"[USER_REPO] Error updating status for user {user_id}: {e}", exc_info=True)
            raise

    async def link_operator(
        self, 
        user_id: int, 
        operator_name: str,
        extension: Optional[str] = None
    ) -> None:
        """
        Связать Telegram пользователя с оператором из таблицы users (Mango).
        
        Args:
            user_id: Telegram ID
            operator_name: Имя оператора (из поля full_name в users)
            extension: Extension номер (из поля extension в users)
        """
        logger.info(f"[USER_REPO] Linking user {user_id} to operator: {operator_name}")
        
        try:
            query = """
            UPDATE UsersTelegaBot 
            SET operator_name = %s, extension = %s
            WHERE user_id = %s
            """
            await self.db_manager.execute_query(query, (operator_name, extension, user_id))
            logger.info(f"[USER_REPO] User linked to operator successfully")
        except Exception as e:
            logger.error(f"[USER_REPO] Error linking operator for user {user_id}: {e}", exc_info=True)
            raise

    async def get_role_id_by_name(self, role_name: str) -> Optional[int]:
        """Получение role_id по slug из roles_reference."""
        logger.debug(f"[USER_REPO] Getting role_id for role slug: %s", role_name)

        normalized = (role_name or "").strip().lower().replace(" ", "_")
        if not normalized:
            return None

        try:
            query = """
                SELECT role_id
                FROM roles_reference
                WHERE LOWER(role_name) = LOWER(%s)
                LIMIT 1
            """
            result = await self.db_manager.execute_with_retry(query, params=(normalized,), fetchone=True)
            if not result:
                logger.warning("[USER_REPO] Role %s not found in roles_reference", normalized)
                return None
            return int(result.get("role_id"))
        except Exception as exc:
            logger.error("[USER_REPO] Error getting role_id for %s: %s", normalized, exc, exc_info=True)
            return None

    async def get_role_name_by_id(self, role_id: int) -> Optional[str]:
        """Получение slug роли по role_id из roles_reference."""
        logger.debug(f"[USER_REPO] Getting role slug for id: {role_id}")

        try:
            query = """
                SELECT role_name
                FROM roles_reference
                WHERE role_id = %s
                LIMIT 1
            """
            result = await self.db_manager.execute_with_retry(query, params=(role_id,), fetchone=True)
            if not result:
                logger.warning("[USER_REPO] Role with ID %s not found in roles_reference", role_id)
                return None
            return result.get("role_name")
        except Exception as exc:
            logger.error("[USER_REPO] Error getting role slug for id %s: %s", role_id, exc, exc_info=True)
            return None

    async def _get_user_pk_by_telegram_id(self, telegram_id: int) -> Optional[int]:
        """
        Получить UsersTelegaBot.id (PK) по Telegram user_id.
        
        Args:
            telegram_id: Telegram user ID
            
        Returns:
            UsersTelegaBot.id (PK) или None
        """
        try:
            query = "SELECT id FROM UsersTelegaBot WHERE user_id = %s"
            result = await self.db_manager.execute_query(query, (telegram_id,), fetchone=True)
            return result.get('id') if result else None
        except Exception as e:
            logger.error(f"[USER_REPO] Error getting user PK: {e}")
            return None

    async def approve_user(self, user_id: int, approved_by_telegram_id: int) -> None:
        """
        Одобрить пользователя.
        
        ВАЖНО: approved_by в БД — это UsersTelegaBot.id (PK), не telegram_id!
        
        Args:
            user_id: Telegram ID пользователя для одобрения
            approved_by_telegram_id: Telegram ID админа, одобрившего
        """
        logger.info(f"[USER_REPO] Approving user {user_id} by telegram_id={approved_by_telegram_id}")
        
        try:
            # Получаем UsersTelegaBot.id (PK) для approved_by
            approved_by_pk = await self._get_user_pk_by_telegram_id(approved_by_telegram_id)
            if not approved_by_pk:
                logger.warning(f"[USER_REPO] Cannot find PK for approver telegram_id={approved_by_telegram_id}")
                # Если не нашли PK, записываем NULL чтобы не нарушить FK
                approved_by_pk = None
            
            query = """
            UPDATE UsersTelegaBot 
            SET status = 'approved', approved_by = %s
            WHERE user_id = %s
            """
            await self.db_manager.execute_query(query, (approved_by_pk, user_id))
            logger.info(f"[USER_REPO] User {user_id} approved successfully (approved_by PK={approved_by_pk})")
        except Exception as e:
            logger.error(f"[USER_REPO] Error approving user {user_id}: {e}", exc_info=True)
            raise

    async def block_user(self, user_id: int) -> None:
        """Заблокировать пользователя."""
        logger.info(f"[USER_REPO] Blocking user {user_id}")
        
        try:
            query = """
            UPDATE UsersTelegaBot 
            SET status = 'blocked', blocked_at = NOW()
            WHERE user_id = %s
            """
            await self.db_manager.execute_query(query, (user_id,))
            logger.info(f"[USER_REPO] User {user_id} blocked successfully")
        except Exception as e:
            logger.error(f"[USER_REPO] Error blocking user {user_id}: {e}", exc_info=True)
            raise

    async def unblock_user(self, user_id: int) -> None:
        """Разблокировать пользователя."""
        logger.info(f"[USER_REPO] Unblocking user {user_id}")
        
        try:
            query = """
            UPDATE UsersTelegaBot 
            SET status = 'approved', blocked_at = NULL
            WHERE user_id = %s
            """
            await self.db_manager.execute_query(query, (user_id,))
            logger.info(f"[USER_REPO] User {user_id} unblocked successfully")
        except Exception as e:
            logger.error(f"[USER_REPO] Error unblocking user {user_id}: {e}", exc_info=True)
            raise
