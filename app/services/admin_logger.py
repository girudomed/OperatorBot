"""
Admin Action Logger Service.

Централизованное логирование всех админских действий в admin_action_logs.
Использует telegram user_id вместо внутренних DB IDs.
"""

import json
import traceback
from typing import Optional, Dict, Any
from datetime import datetime

from app.db.manager import DatabaseManager
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)


class AdminActionLogger:
    """
    Сервис для логирования админских действий.
    
    Все действия записываются с telegram user_id (не internal DB IDs).
    """
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
    
    async def log_action(
        self,
        actor_telegram_id: int,
        action: str,
        target_telegram_id: Optional[int] = None,
        payload: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Записать действие админа в лог.
        
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
            f"[ADMIN_LOG] Logging action: {action} by {actor_telegram_id} "
            f"on {target_telegram_id}"
        )
        
        try:
            query = """
                INSERT INTO admin_action_logs 
                (actor_id, target_id, action, payload_json, created_at)
                VALUES (%s, %s, %s, %s, NOW())
            """
            
            payload_json = json.dumps(payload, ensure_ascii=False) if payload else None
            
            await self.db.execute_with_retry(
                query,
                params=(actor_telegram_id, target_telegram_id, action, payload_json),
                commit=True
            )
            
            logger.info(f"[ADMIN_LOG] Action '{action}' logged successfully")
            return True
            
        except Exception as e:
            logger.error(
                f"[ADMIN_LOG] Error logging action '{action}': {e}\n{traceback.format_exc()}"
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
            admin_telegram_id: ID админа
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
            admin_telegram_id: ID админа
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
    
    async def get_recent_logs(
        self,
        limit: int = 50,
        actor_telegram_id: Optional[int] = None,
        action_filter: Optional[str] = None
    ) -> list:
        """
        Получить последние логи действий.
        
        Args:
            limit: Макс количество записей
            actor_telegram_id: Фильтр по актору (optional)
            action_filter: Фильтр по типу действия (optional)
        
        Returns:
            Список логов с join к UsersTelegaBot для имен
        """
        logger.info(
            f"[ADMIN_LOG] Getting recent logs: limit={limit}, "
            f"actor={actor_telegram_id}, action={action_filter}"
        )
        
        try:
            conditions = []
            params = []
            
            if actor_telegram_id:
                conditions.append("l.actor_id = %s")
                params.append(actor_telegram_id)
            
            if action_filter:
                conditions.append("l.action = %s")
                params.append(action_filter)
            
            where_clause = ""
            if conditions:
                where_clause = "WHERE " + " AND ".join(conditions)
            
            query = f"""
                SELECT 
                    l.*,
                    a.username as actor_username,
                    a.full_name as actor_name,
                    t.username as target_username,
                    t.full_name as target_name
                FROM admin_action_logs l
                LEFT JOIN UsersTelegaBot a ON l.actor_id = a.user_id
                LEFT JOIN UsersTelegaBot t ON l.target_id = t.user_id
                {where_clause}
                ORDER BY l.created_at DESC
                LIMIT %s
            """
            
            params.append(limit)
            
            results = await self.db.execute_with_retry(
                query, params=tuple(params), fetchall=True
            ) or []
            
            logger.info(f"[ADMIN_LOG] Found {len(results)} log entries")
            
            return [dict(row) for row in results]
            
        except Exception as e:
            logger.error(
                f"[ADMIN_LOG] Error getting logs: {e}\n{traceback.format_exc()}"
            )
            return []
    
    async def get_user_actions_count(
        self,
        admin_telegram_id: int,
        action: Optional[str] = None
    ) -> int:
        """
        Получить количество действий админа.
        
        Args:
            admin_telegram_id: Telegram ID админа
            action: Фильтр по типу действия (optional)
        
        Returns:
            Количество действий
        """
        try:
            if action:
                query = """
                    SELECT COUNT(*) as count
                    FROM admin_action_logs
                    WHERE actor_id = %s AND action = %s
                """
                params = (admin_telegram_id, action)
            else:
                query = """
                    SELECT COUNT(*) as count
                    FROM admin_action_logs
                    WHERE actor_id = %s
                """
                params = (admin_telegram_id,)
            
            result = await self.db.execute_with_retry(
                query, params=params, fetchone=True
            )
            
            count = result.get('count', 0) if result else 0
            
            logger.debug(
                f"[ADMIN_LOG] Admin {admin_telegram_id} has {count} "
                f"actions{f' of type {action}' if action else ''}"
            )
            
            return count
            
        except Exception as e:
            logger.error(
                f"[ADMIN_LOG] Error counting actions: {e}\n{traceback.format_exc()}"
            )
            return 0
