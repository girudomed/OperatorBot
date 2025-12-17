from typing import Tuple, Optional, List
import logging

logger = logging.getLogger(__name__)

class AdminCB:
    """
    Централизованный кодек для admin callback_data.
    Формат: prefix:action:arg1:arg2
    Лимит Telegram: 64 байта.
    """
    
    PREFIX = "adm"  # Короткий префикс для экономии места
    SEP = ":"
    
    # === ACTIONS (короткие алиасы) ===
    # Главное меню
    DASHBOARD = "dsh"
    DASHBOARD_DETAILS = "dshdet"
    ALERTS = "alrt"
    EXPORT = "exprt"
    CRITICAL = "crtl"
    USERS = "usr"
    ADMINS = "adms"
    STATS = "st"
    LOOKUP = "lk"
    SETTINGS = "set"
    LM_MENU = "lm"
    BACK = "back"
    APPROVALS = "aprv"
    PROMOTION = "prm"
    DEV_REPLY = "devr"
    
    # Команды
    COMMANDS = "cmds"
    COMMAND = "cmd"
    
    # Пользователи (adm:usr:...)
    LIST = "lst"
    DETAILS = "det"
    APPROVE = "apr"
    DECLINE = "dcl"
    BLOCK = "blk"
    UNBLOCK = "unb"
    
    # Статусы фильтров
    STATUS_PENDING = "p"    # pending
    STATUS_APPROVED = "a"   # approved
    STATUS_BLOCKED = "b"    # blocked
    
    # LM (adm:lm:...)
    lm_OPS = "ops"
    lm_CONV = "cnv"
    lm_QUAL = "qul"
    lm_RISK = "rsk"
    lm_FCST = "fst"
    lm_SUM = "sum"
    lm_FLW = "flw"
    
    # Legacy Support
    LEGACY_PREFIXES = ("admin", "admincmd")
    
    # Feature Modules
    CALL_LOOKUP = "cl"
    REPORTS = "rep"
    
    @classmethod
    def create(cls, action: str, *args) -> str:
        """
        Создает callback_data строку с валидацией длины.
        Пример: AdminCB.create(AdminCB.USERS, AdminCB.LIST, AdminCB.STATUS_PENDING)
        -> 'adm:usr:lst:p'
        """
        parts = [cls.PREFIX, action]
        parts.extend(map(str, args))
        data = cls.SEP.join(parts)
        
        if len(data.encode('utf-8')) > 64:
            logger.error(f"Callback data too long ({len(data)} bytes): {data}")
            # Можно рейзить ошибку или возвращать заглушку, 
            # но лучше залогировать и (если критично) вернуть урезанную/ошибочную.
            # Для надежности в проде рейзим, чтобы дев заметил сразу.
            raise ValueError(f"Callback data exceeds 64 bytes: {data}")
            
        return data

    @classmethod
    def parse(cls, data: str) -> Tuple[Optional[str], List[str]]:
        """
        Парсит callback_data.
        Возвращает (action, [args]).
        Если префикс не совпадает, возвращает (None, []).
        """
        if not data.startswith(cls.PREFIX + cls.SEP):
            return None, []
            
        parts = data.split(cls.SEP)
        # parts[0] == 'adm'
        if len(parts) < 2:
            return None, []
            
        action = parts[1]
        args = parts[2:]
        return action, args
    
    @classmethod
    def match(cls, data: str, action: str) -> bool:
        """Проверяет, соответствует ли callback указанному action."""
        if not data:
            return False
        parsed_action, _ = cls.parse(data)
        return parsed_action == action

    @classmethod
    def starts_with(cls, data: str, action: str) -> bool:
        """Проверяет начало (для групповых фильтров)."""
        return data.startswith(f"{cls.PREFIX}{cls.SEP}{action}")
