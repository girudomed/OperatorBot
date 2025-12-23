
"""
Кодек для callback-данных LM-метрик (namespace 'lm:').
Обеспечивает изоляцию LM-логики от общего AdminCB.
"""

import hashlib
import json
from typing import Any, Dict, List, Optional, Union

from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)

class LMCB:
    """
    Класс-утилита для работы с LM callback data.
    Использует префикс 'lm:' для изоляции от 'adm:'.
    """
    PREFIX = "lm:"
    HASH_PREFIX = "lm:h:"
    MAX_LENGTH = 64
    
    # Реестр для хешированных данных (в памяти, для прода лучше Redis)
    _hash_registry: Dict[str, str] = {}

    @classmethod
    def create(cls, action: str, *args) -> str:
        """
        Создает строку callback_data. 
        Пример: LMCB.create('view', 123) -> 'lm:view:123'
        """
        parts = [action] + [str(a) for a in args if a is not None]
        data = f"{cls.PREFIX}{':'.join(parts)}"
        
        if len(data.encode('utf-8')) > cls.MAX_LENGTH:
            # Хеширование для обхода ограничения Telegram в 64 байта
            digest = hashlib.md5(data.encode('utf-8')).hexdigest()[:12]
            short_data = f"{cls.HASH_PREFIX}{digest}"
            cls._hash_registry[short_data] = data
            return short_data
            
        return data

    @classmethod
    def parse(cls, data: str) -> Optional[List[str]]:
        """
        Парсит callback_data. Возвращает [action, arg1, arg2, ...]
        """
        if not data:
            return None
            
        # Восстановление из хеша, если нужно
        if data.startswith(cls.HASH_PREFIX):
            data = cls._hash_registry.get(data, data)
            
        if not data.startswith(cls.PREFIX):
            return None
            
        raw = data[len(cls.PREFIX):]
        return raw.split(':') if raw else []

    @classmethod
    def is_lm(cls, data: Optional[str]) -> bool:
        """Проверяет, относится ли callback к LM."""
        if not data:
            return False
        return data.startswith(cls.PREFIX) or data.startswith(cls.HASH_PREFIX)

    # Стандартные экшены для удобства
    ACTION_SUMMARY = "sum"
    ACTION_DETAILS = "det"
    ACTION_REFRESH = "ref"
    ACTION_BACK = "back"
    ACTION_PAGINATION = "pg"
    ACTION_LIST = "act"
    ACTION_METHOD = "meth"

    # Backward-compatible aliases
    SUMMARY = ACTION_SUMMARY
    ACTIONS = ACTION_LIST
