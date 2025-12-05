"""
Admin Action Logger Service.

Централизованное логирование всех админских действий в admin_action_logs.
ВАЖНО: actor_id и target_id = UsersTelegaBot.id (PK), НЕ user_id (Telegram ID)!
"""

import json

from typing import Optional, Dict, Any
from datetime import datetime

from app.db.manager import DatabaseManager
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)


class AdminActionLogger:
    """
    Сервис для логирования админских действий.
    
    ВАЖНО: В admin_action_logs.actor_id и target_id записываем
    UsersTelegaBot.id (PK), а не user_id (Telegram ID)!
    """
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
    
    async def _get_user_pk_by_telegram_id(self, telegram_id: int) -> Optional[int]:
        """
        Получить UsersTelegaBot.id (PK) по Telegram user_id.
        
        Args:
            telegram_id: Telegram user ID (user_id в таблице)
            
        Returns:
            UsersTelegaBot.id (PK) или None
        """
        try:
            query = "SELECT id FROM UsersTelegaBot WHERE user_id = %s"
            result = await self.db.execute_with_retry(
                query, params=(telegram_id,), fetchone=True
            )
            return result.get('id') if result else None
        except Exception as e:
            logger.error(f"[ADMIN_LOG] Error getting user PK: {e}", exc_info=True)
            return None
    
    async def log_action(
        self,
        actor_telegram_id: int,
        action: str,
        target_telegram_id: Optional[int] = None,
        payload: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Записать действие админа в лог.
        
        ВАЖНО: Принимает Telegram ID, но записывает UsersTelegaBot.id (PK)!
        
        Args:
            actor_telegram_id: Telegram ID того кто совершил действие
            action: Тип действия (approve, decline, make_admin, block, etc)
            target_telegram_id: Telegram ID над кем выполнено (optional)
            payload: Дополнительные данные (dict → JSON)
        
        Actions:
            - approve: одобрение пользователя
            - decline: отклонение заявки
            - make_admin: назначение админом
            - make_superadmin: назначение супер-админом
            - change_role: смена роли
            - block: блокировка
            - unblock: разблокировка
            - lookup: просмотр списков
            - system_action: системные действия
        
        Returns:
            True если успешно, False при ошибке
        """
        logger.info(
            f"[ADMIN_LOG] Logging action: {action} by telegram_id={actor_telegram_id} "
            f"on telegram_id={target_telegram_id}"
        )
        
        try:
            # Получаем UsersTelegaBot.id (PK) для actor и target
            actor_pk = await self._get_user_pk_by_telegram_id(actor_telegram_id)
            if not actor_pk:
                logger.warning(
                    f"[ADMIN_LOG] Cannot find actor UsersTelegaBot.id "
                    f"for telegram_id={actor_telegram_id}"
                )
                # Записываем в payload для отладки
                if payload is None:
                    payload = {}
                payload['_actor_telegram_id_fallback'] = actor_telegram_id
            
            target_pk = None
            if target_telegram_id:
                target_pk = await self._get_user_pk_by_telegram_id(target_telegram_id)
                if not target_pk:
                    logger.warning(
                        f"[ADMIN_LOG] Cannot find target UsersTelegaBot.id "
                        f"for telegram_id={target_telegram_id}"
                    )
                    if payload is None:
                        payload = {}
                    payload['_target_telegram_id_fallback'] = target_telegram_id
            
            # Если actor_pk не найден, не записываем (нарушит FK constraint)
            if not actor_pk:
                logger.error(f"[ADMIN_LOG] Actor not found, cannot log action")
                return False
            
            query = """
                INSERT INTO admin_action_logs 
                (actor_id, target_id, action, payload_json, created_at)
                VALUES (%s, %s, %s, %s, NOW())
            """
            
            payload_json = json.dumps(payload, ensure_ascii=False) if payload else None
            
            await self.db.execute_with_retry(
                query,
                params=(actor_pk, target_pk, action, payload_json),
                commit=True
            )
            
            logger.info(
                f"[ADMIN_LOG] Action '{action}' logged: "
                f"actor_id={actor_pk}, target_id={target_pk}"
            )
            return True
            
        except Exception as e:
            logger.error(
                f"[ADMIN_LOG] Error logging action '{action}': {e}",
                exc_info=True
            )
            return False
    
    async def log_user_approval(
        self,
        admin_telegram_id: int,
        user_telegram_id: int,
        **extra
    ) -> bool:
        """Логирование одобрения пользователя."""
        return await self.log_action(
            actor_telegram_id=admin_telegram_id,
            action='approve',
            target_telegram_id=user_telegram_id,
            payload={'timestamp': datetime.now().isoformat(), **extra}
        )
    
    async def log_user_decline(
        self,
        admin_telegram_id: int,
        user_telegram_id: int,
        reason: Optional[str] = None
    ) -> bool:
        """Логирование отклонения заявки."""
        return await self.log_action(
            actor_telegram_id=admin_telegram_id,
            action='decline',
            target_telegram_id=user_telegram_id,
            payload={'reason': reason} if reason else None
        )
    
    async def log_role_change(
        self,
        admin_telegram_id: int,
        user_telegram_id: int,
        old_role: str,
        new_role: str
    ) -> bool:
        """Логирование смены роли."""
        return await self.log_action(
            actor_telegram_id=admin_telegram_id,
            action='change_role',
            target_telegram_id=user_telegram_id,
            payload={'old_role': old_role, 'new_role': new_role}
        )
    
    async def log_user_block(
        self,
        admin_telegram_id: int,
        user_telegram_id: int,
        reason: Optional[str] = None
    ) -> bool:
        """Логирование блокировки пользователя."""
        return await self.log_action(
            actor_telegram_id=admin_telegram_id,
            action='block',
            target_telegram_id=user_telegram_id,
            payload={'reason': reason} if reason else None
        )
    
    async def log_user_unblock(
        self,
        admin_telegram_id: int,
        user_telegram_id: int
    ) -> bool:
        """Логирование разблокировки пользователя."""
        return await self.log_action(
            actor_telegram_id=admin_telegram_id,
            action='unblock',
            target_telegram_id=user_telegram_id
        )
    
    async def log_lookup(
        self,
        admin_telegram_id: int,
        lookup_type: str
    ) -> bool:
        """
        Логирование просмотра списков.
        
        Args:
            admin_telegram_id: Telegram ID админа
            lookup_type: 'users_list', 'admins_list', 'pending_list', etc
        """
        return await self.log_action(
            actor_telegram_id=admin_telegram_id,
            action='lookup',
            target_telegram_id=None,
            payload={'lookup_type': lookup_type}
        )
    
    async def log_system_action(
        self,
        admin_telegram_id: int,
        system_action: str,
        details: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Логирование системных действий.
        
        Args:
            admin_telegram_id: Telegram ID админа
            system_action: Описание действия
            details: Дополнительные детали
        """
        payload = {'system_action': system_action}
        if details:
            payload.update(details)
        
        return await self.log_action(
            actor_telegram_id=admin_telegram_id,
            action='system_action',
            target_telegram_id=None,
            payload=payload
        )
    
    async def get_actions_by_actor(
        self,
        actor_telegram_id: int,
        limit: int = 50
    ) -> list:
        """
        Получить действия конкретного админа.
        
        Args:
            actor_telegram_id: Telegram ID админа
            limit: Максимальное количество записей
        """
        try:
            actor_pk = await self._get_user_pk_by_telegram_id(actor_telegram_id)
            if not actor_pk:
                return []
            
            query = """
                SELECT 
                    l.id,
                    l.action,
                    l.target_id,
                    t.user_id as target_telegram_id,
                    t.full_name as target_name,
                    l.payload_json,
                    l.created_at
                FROM admin_action_logs l
                LEFT JOIN UsersTelegaBot t ON l.target_id = t.id
                WHERE l.actor_id = %s
                ORDER BY l.created_at DESC
                LIMIT %s
            """
            
            results = await self.db.execute_with_retry(
                query, params=(actor_pk, limit), fetchall=True
            )
            
            return [dict(row) for row in results] if results else []
            
        except Exception as e:
            logger.error(f"[ADMIN_LOG] Error getting actions: {e}", exc_info=True)
            return []
    
    async def get_actions_on_target(
        self,
        target_telegram_id: int,
        limit: int = 50
    ) -> list:
        """
        Получить все действия над конкретным пользователем.
        
        Args:
            target_telegram_id: Telegram ID пользователя
            limit: Максимальное количество записей
        """
        try:
            target_pk = await self._get_user_pk_by_telegram_id(target_telegram_id)
            if not target_pk:
                return []
            
            query = """
                SELECT 
                    l.id,
                    l.action,
                    l.actor_id,
                    a.user_id as actor_telegram_id,
                    a.full_name as actor_name,
                    l.payload_json,
                    l.created_at
                FROM admin_action_logs l
                JOIN UsersTelegaBot a ON l.actor_id = a.id
                WHERE l.target_id = %s
                ORDER BY l.created_at DESC
                LIMIT %s
            """
            
            results = await self.db.execute_with_retry(
                query, params=(target_pk, limit), fetchall=True
            )
            
            return [dict(row) for row in results] if results else []
            
        except Exception as e:
            logger.error(f"[ADMIN_LOG] Error getting target actions: {e}", exc_info=True)
            return []
    
    async def get_recent_actions(
        self,
        limit: int = 100,
        action_type: Optional[str] = None
    ) -> list:
        """
        Получить последние действия.
        
        Args:
            limit: Максимальное количество записей
            action_type: Фильтр по типу действия (optional)
        """
        try:
            if action_type:
                query = """
                    SELECT 
                        l.id,
                        l.action,
                        l.actor_id,
                        a.user_id as actor_telegram_id,
                        a.full_name as actor_name,
                        l.target_id,
                        t.user_id as target_telegram_id,
                        t.full_name as target_name,
                        l.payload_json,
                        l.created_at
                    FROM admin_action_logs l
                    JOIN UsersTelegaBot a ON l.actor_id = a.id
                    LEFT JOIN UsersTelegaBot t ON l.target_id = t.id
                    WHERE l.action = %s
                    ORDER BY l.created_at DESC
                    LIMIT %s
                """
                params = (action_type, limit)
            else:
                query = """
                    SELECT 
                        l.id,
                        l.action,
                        l.actor_id,
                        a.user_id as actor_telegram_id,
                        a.full_name as actor_name,
                        l.target_id,
                        t.user_id as target_telegram_id,
                        t.full_name as target_name,
                        l.payload_json,
                        l.created_at
                    FROM admin_action_logs l
                    JOIN UsersTelegaBot a ON l.actor_id = a.id
                    LEFT JOIN UsersTelegaBot t ON l.target_id = t.id
                    ORDER BY l.created_at DESC
                    LIMIT %s
                """
                params = (limit,)
            
            results = await self.db.execute_with_retry(
                query, params=params, fetchall=True
            )
            
            return [dict(row) for row in results] if results else []
            
        except Exception as e:
            logger.error(f"[ADMIN_LOG] Error getting recent actions: {e}", exc_info=True)
            return []
