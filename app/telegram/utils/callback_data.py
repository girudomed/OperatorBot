from typing import Tuple, Optional, List, Dict
import logging
import hashlib
import os
import asyncio

logger = logging.getLogger(__name__)


class AdminCB:
    """
    Централизованный кодек для admin callback_data.
    Формат: prefix:action:arg1:arg2
    Лимит Telegram: 64 байта.

    Добавлена поддержка "hashed fallback" для длинных callback_data:
    - при превышении лимита создаётся короткий хеш adm:hd:<digest>
    - оригинальная строка сохраняется в in-memory registry _hash_registry
    - можно попытаться разрешить хеш через resolve_hash
    NOTE: registry в памяти — неустойчиво при рестартах. Для продакшена
    рекомендуется хранить сопоставления в Redis/БД (см. TODO в коде).
    """

    PREFIX = "adm"  # Короткий префикс для экономии места
    SEP = ":"

    # hashed fallback action (used when callback_data exceeds Telegram 64-byte limit)
    HD = "hd"

    # In-memory registry mapping short hash -> original callback_data.
    # NOTE: This is non-persistent (lost on process restart). For robust production usage
    # consider moving this registry to a persistent store (Redis/DB) and using AdminCB.register_hash.
    _hash_registry: Dict[str, str] = {}

    # === ACTIONS (короткие алиасы) ===
    # Главное меню
    DASHBOARD = "dsh"
    ALERTS = "alrt"
    EXPORT = "exprt"
    CRITICAL = "crtl"
    USERS = "usr"
    ADMINS = "adms"
    STATS = "st"
    DASHBOARD_DETAILS = "dshdet"
    SYSTEM = "sys"
    LOOKUP = "lk"
    SETTINGS = "set"
    LM_MENU = "lm"
    BACK = "back"
    APPROVALS = "aprv"
    PROMOTION = "prm"
    DEV_REPLY = "devr"
    HELP_SCREEN = "hlp"
    MANUAL = "man"
    YANDEX = "ydx"
    CALL = "call"

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
    CALL_EXPORT = "cxp"

    @classmethod
    def create(cls, action: str, *args) -> str:
        """
        Создает callback_data строку с валидацией длины.
        Пример: AdminCB.create(AdminCB.USERS, AdminCB.LIST, AdminCB.STATUS_PENDING)
        -> 'adm:usr:lst:p'

        Если длина > 64 байт, создаётся хеш‑фолбек adm:hd:<digest> и оригинал
        сохраняется в _hash_registry для последующего разрешения.
        """
        parts = [cls.PREFIX, action]
        parts.extend(map(str, args))
        data = cls.SEP.join(parts)

        if len(data.encode("utf-8")) > 64:
            # Длина callback_data превышает лимит Telegram (64 байта).
            logger.warning(
                "Callback data too long (%d bytes), returning hashed fallback", len(data)
            )
            digest = hashlib.sha256(data.encode("utf-8")).hexdigest()[:16]
            # 'hd' — short for 'hashed'
            fallback = cls.SEP.join([cls.PREFIX, cls.HD, digest])
            # Сохраняем сопоставление digest -> original для последующего разрешения.
            try:
                cls.register_hash(digest, data)
            except Exception:
                logger.exception("Failed to register hashed callback mapping")
            # Убедимся, что fallback укладывается в лимит (должно быть очень коротким)
            if len(fallback.encode("utf-8")) > 64:
                # На редкий случай — если и это вдруг большое, просто вернём префикс.
                logger.error(
                    "Hashed fallback for callback_data is still too long, returning prefix only"
                )
                return f"{cls.PREFIX}{cls.SEP}err"
            return fallback

        return data

    @classmethod
    def parse(cls, data: str) -> Tuple[Optional[str], List[str]]:
        """
        Парсит callback_data.
        Возвращает (action, [args]).
        Если префикс не совпадает, возвращает (None, []).
        """
        if not data or not data.startswith(cls.PREFIX + cls.SEP):
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

    # ---- Hash registry helpers ----
    @classmethod
    def register_hash(cls, digest: str, original: str) -> None:
        """
        Регистрирует сопоставление digest -> original.

        Поведение:
          - всегда сохраняет в in-memory registry (быстрый доступ), но это вспомогательный механизм;
          - если задан REDIS_URL, пытается сохранить в Redis (если event loop запущен — асинхронно в background task,
            иначе — синхронно используя redis-py sync client).
        """
        if not digest:
            return

        # Сохраняем в памяти сразу (быстрый доступ, dev резерв)
        try:
            cls._hash_registry[digest] = original
        except Exception:
            logger.exception("Failed to store hash mapping in memory")

        # Если REDIS не настроен — оставляем только in-memory (dev mode).
        redis_url = os.getenv("REDIS_URL")
        if not redis_url:
            logger.debug("REDIS_URL not set — skipping Redis persistence for admin callback hash")
            return

        # Попытка асинхронной записи в Redis, если есть запущенный loop.
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop:
            async def _save_to_redis():
                try:
                    from redis.asyncio import Redis
                    r = Redis.from_url(redis_url, decode_responses=True)
                    # Сохраняем с TTL 24 часа — это ограничит размер хранилища.
                    await r.set(f"adm_hd:{digest}", original, ex=24 * 3600)
                    await r.close()
                except Exception:
                    logger.exception("Failed to save hash mapping to Redis (async)")

            try:
                loop.create_task(_save_to_redis())
            except Exception:
                logger.exception("Failed to schedule Redis save task")
            return

        # Если event loop отсутствует (например, код вызывается при импорте/сборке клавиатур),
        # попытаться записать синхронно (best-effort).
        try:
            try:
                import redis as _redis_sync  # redis-py sync client
            except Exception:
                _redis_sync = None

            if _redis_sync:
                try:
                    r = _redis_sync.Redis.from_url(redis_url, decode_responses=True)
                    r.set(f"adm_hd:{digest}", original, ex=24 * 3600)
                    try:
                        r.close()
                    except Exception:
                        pass
                except Exception:
                    logger.exception("Failed to save hash mapping to Redis (sync)")
            else:
                logger.debug("redis package not available; cannot persist admin callback mapping synchronously")
        except Exception:
            logger.exception("Unexpected error while attempting sync Redis save")

    @classmethod
    def resolve_hash(cls, digest: str) -> Optional[str]:
        """
        Синхронный путь разрешения.

        Поведение:
         - Если настроен REDIS_URL и установлен sync-клиент redis, пытаемся сначала обратиться в Redis;
         - затем используем in-memory cache как резерв (dev).
        """
        if not digest:
            return None

        redis_url = os.getenv("REDIS_URL")
        if redis_url:
            try:
                import redis as _redis_sync
                if _redis_sync:
                    try:
                        r = _redis_sync.Redis.from_url(redis_url, decode_responses=True)
                        got = r.get(f"adm_hd:{digest}")
                        try:
                            r.close()
                        except Exception:
                            pass
                        if got:
                            # Обновляем in-memory cache для ускорения последующих запросов
                            cls._hash_registry[digest] = got
                            return got
                        # не найдено в Redis — логируем явное предупреждение
                        logger.warning("Hashed admin callback mapping not found in Redis for digest=%s", digest)
                    except Exception:
                        logger.exception("Failed to resolve hash from Redis (sync)")
            except Exception:
                # redis-py не установлен — переходим к in-memory
                logger.debug("redis sync client not available for resolving admin callback hash")
        # Fallback to in-memory
        original = cls._hash_registry.get(digest)
        if original:
            return original
        logger.warning("Hashed admin callback mapping not found in memory for digest=%s", digest)
        return None

    @classmethod
    async def resolve_hash_async(cls, digest: str) -> Optional[str]:
        """
        Асинхронно пытается разрешить digest -> original.

        Поведение:
         - Если настроен REDIS_URL, сначала пытаемся получить mapping из Redis (primary);
         - в случае неудачи — используем in-memory cache как резерв;
         - если ничего не найдено — логируем предупреждение.
        """
        if not digest:
            return None

        redis_url = os.getenv("REDIS_URL")
        if redis_url:
            try:
                from redis.asyncio import Redis
                r = Redis.from_url(redis_url, decode_responses=True)
                try:
                    got = await r.get(f"adm_hd:{digest}")
                finally:
                    try:
                        await r.close()
                    except Exception:
                        pass
                if got:
                    # Обновляем in-memory cache для ускорения последующих запросов
                    cls._hash_registry[digest] = got
                    return got
                # Не найдено в Redis — логируем явное предупреждение и пробуем in-memory
                logger.warning("Hashed admin callback mapping not found in Redis for digest=%s", digest)
            except Exception:
                logger.exception("Failed to resolve hash from Redis (async)")

        # Fallback to in-memory cache (dev reserve)
        original = cls._hash_registry.get(digest)
        if original:
            return original

        logger.warning("Hashed admin callback mapping not found (redis and memory) for digest=%s", digest)
        return None
