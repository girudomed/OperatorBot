"""
Менеджер подключения к базе данных.
"""

import asyncio
import inspect
import re
import time
import aiomysql
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Union, Tuple

from app.config import DB_CONFIG
from app.error_policy import get_retry_config, is_retryable
from app.errors import DatabaseIntegrationError
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)


class DatabaseManager:
    """
    Класс для управления пулом соединений с базой данных и выполнения запросов.
    """
    
    def __init__(self):
        self.pool: Optional[aiomysql.Pool] = None
        self._lock = asyncio.Lock()
    
    @staticmethod
    def _clip_for_log(value: Any, limit: int = 240) -> str:
        text = repr(value).replace("\r", " ").replace("\n", " ")
        if len(text) <= limit:
            return text
        return f"{text[:limit]}...<truncated:{len(text) - limit}>"

    @staticmethod
    def _is_admin_action_logs_query(query: Optional[str]) -> bool:
        if not isinstance(query, str):
            return False
        lowered = " ".join(query.split()).lower()
        return "admin_action_logs" in lowered and "insert into" in lowered

    @classmethod
    def _sanitize_params_for_log(
        cls,
        params: Optional[Union[Tuple, List, Dict]],
        *,
        query: Optional[str] = None,
    ) -> Optional[Union[Tuple, List, Dict, str]]:
        if params is None:
            return None
        try:
            if cls._is_admin_action_logs_query(query):
                if isinstance(params, tuple) and len(params) >= 4:
                    masked = list(params)
                    payload = params[3]
                    payload_len = len(payload) if isinstance(payload, str) else 0
                    masked[3] = f"<omitted payload_json len={payload_len}>"
                    return tuple(cls._clip_for_log(item) for item in masked)
                return "<omitted admin_action_logs params>"
            if isinstance(params, tuple):
                return tuple(cls._clip_for_log(item) for item in params)
            if isinstance(params, list):
                return [cls._clip_for_log(item) for item in params]
            if isinstance(params, dict):
                return {str(key): cls._clip_for_log(val) for key, val in params.items()}
            return cls._clip_for_log(params)
        except Exception:
            return "<unserializable params>"

    @staticmethod
    def _extract_db_error_details(error: Exception) -> Tuple[str, Optional[int], str]:
        error_type = type(error).__name__
        error_code = None
        message = str(error)
        if hasattr(error, "args") and error.args:
            first = error.args[0]
            if isinstance(first, int):
                error_code = first
        return error_type, error_code, message

    @staticmethod
    def _classify_db_error(error_code: Optional[int], message: str) -> str:
        if error_code in {1054, 1146, 1060, 1062}:
            return "schema_error"
        if error_code in {2002, 2003, 2006, 2013, 1205, 1213}:
            return "connection_error"
        if error_code in {3024}:
            return "timeout"
        return "unknown_error"

    def _resolve_query_name(self, query_name: Optional[str]) -> str:
        if query_name:
            return query_name
        try:
            frame = inspect.stack()[3]
            module = frame.frame.f_globals.get("__name__", "")
            return f"{module}.{frame.function}"
        except Exception as exc:
            logger.debug("Не удалось определить имя запроса: %s", exc, exc_info=True)
            return "unknown_query"

    @staticmethod
    def _sanitize_sql(query: Optional[str]) -> Optional[str]:
        """
        Горячий фикс: в старых запросах могли остаться обращения к cs.score.
        В MySQL такого столбца нет (есть call_score), поэтому мягко переписываем SQL.
        """
        if not isinstance(query, str) or "score" not in query:
            return query
        pattern = re.compile(r"(?i)\b(cs|call_scores)\.score\b")
        return pattern.sub(lambda match: f"{match.group(1)}.call_score", query)

    def _log_db_error(
        self,
        error: Exception,
        query: str,
        params: Optional[Union[Tuple, List, Dict]],
        query_name: Optional[str],
        category: Optional[str] = None,
    ) -> str:
        error_type, error_code, message = self._extract_db_error_details(error)
        resolved_category = category or self._classify_db_error(error_code, message)
        safe_params = self._sanitize_params_for_log(params, query=query)
        
        # Для критических ошибок схемы или специфичных кодов (1054 - Unknown column)
        # принудительно выводим контекст в текст сообщения для удобства.
        log_msg = f"Ошибка выполнения SQL-запроса ({resolved_category}): {message}"
        if error_code == 1054 or resolved_category == "schema_error":
            log_msg = f"КРИТИЧЕСКАЯ ОШИБКА СХЕМЫ (1054): {message}\nSQL: {query}\nParams: {safe_params}"
        
        logger.error(
            log_msg,
            extra={
                "error_type": error_type,
                "error_code": error_code,
                "error_message": message,
                "query_name": self._resolve_query_name(query_name),
                "sql": query,
                "params": safe_params,
                "category": resolved_category,
            },
        )
        return resolved_category

    async def create_pool(self) -> None:
        """Создание пула соединений с базой данных."""
        async with self._lock:
            if not self.pool:
                logger.info("Создание пула соединений с БД...")
                try:
                    self.pool = await aiomysql.create_pool(
                        host=DB_CONFIG["host"],
                        port=DB_CONFIG["port"],
                        user=DB_CONFIG["user"],
                        password=DB_CONFIG["password"],
                        db=DB_CONFIG["db"],
                        charset=DB_CONFIG.get("charset", "utf8mb4"),
                        use_unicode=True,
                        autocommit=DB_CONFIG.get("autocommit", True),
                        minsize=DB_CONFIG.get("minsize", 1),
                        maxsize=DB_CONFIG.get("maxsize", 50),
                        cursorclass=aiomysql.DictCursor
                    )
                    logger.info("Пул соединений с БД успешно создан.")
                except aiomysql.Error as e:
                    logger.warning("Ошибка при создании пула соединений: %s", e)
                    raise DatabaseIntegrationError(
                        "Failed to create DB pool",
                        user_visible=False,
                        retryable=True,
                        details={"error_type": type(e).__name__},
                    ) from e

    async def close_pool(self) -> None:
        """Закрытие пула соединений."""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            self.pool = None
            logger.info("Пул соединений с БД закрыт.")
    
    async def close(self) -> None:
        """Совместимость с прежним интерфейсом."""
        await self.close_pool()

    @asynccontextmanager
    async def acquire(self):
        """
        Контекстный менеджер для получения соединения из пула.
        """
        if not self.pool:
            await self.create_pool()
        
        conn = None
        try:
            conn = await self.pool.acquire()
            yield conn
        except aiomysql.Error as e:
            logger.warning("Ошибка при получении соединения: %s", e)
            raise DatabaseIntegrationError(
                "Failed to acquire DB connection",
                user_visible=False,
                retryable=True,
                details={"error_type": type(e).__name__},
            ) from e
        finally:
            if conn and self.pool:
                release_result = self.pool.release(conn)
                if inspect.isawaitable(release_result):
                    await release_result

    async def execute_query(
        self,
        query: str,
        params: Optional[Union[Tuple, List, Dict]] = None,
        fetchone: bool = False,
        fetchall: bool = False,
        *,
        commit: bool = False,
        query_name: Optional[str] = None,
        log_error: bool = True,
    ) -> Any:
        """
        Выполнение SQL-запроса.
        
        Args:
            query: SQL запрос
            params: Параметры запроса
            fetchone: Вернуть одну запись
            fetchall: Вернуть все записи
            
        Returns:
            Результат запроса (dict, list или True)
        """
        query = self._sanitize_sql(query)

        if not query or not query.strip():
            logger.error(
                "[DB] CRITICAL: Attempted to execute empty query!",
                extra={
                    "query_name": self._resolve_query_name(query_name),
                    "params": repr(params),
                    "original_query_type": type(query),
                },
            )
            # In dev/debug we might want to crash, but for now just raise exception
            raise ValueError(f"Empty SQL query passed to execute_query. QueryName: {query_name}")

        if not self.pool:
            await self.create_pool()
            
        async with self.acquire() as connection:
            async with connection.cursor() as cursor:
                try:
                    query_preview = " ".join(query.split()) if isinstance(query, str) else str(query)
                    safe_params = self._sanitize_params_for_log(params, query=query_preview)
                    logger.info(
                        "[DB] Executing query: %s | params=%s",
                        query_preview,
                        safe_params,
                    )
                    start_time = time.time()
                    await cursor.execute(query, params)
                    elapsed_time = time.time() - start_time
                    
                    # Логируем медленные запросы (> 1 сек)
                    if elapsed_time > 1.0:
                        logger.warning(f"Медленный запрос ({elapsed_time:.4f} сек): {query}")
                    else:
                        logger.debug(f"Запрос выполнен за {elapsed_time:.4f} сек.")
                
                    if fetchone:
                        result = await cursor.fetchone()
                        logger.info("[DB] fetchone result=%s", repr(result))
                        if commit and connection:
                            await connection.commit()
                        return result if isinstance(result, dict) else {}

                    if fetchall:
                        result = await cursor.fetchall()
                        logger.info(
                            "[DB] fetchall rows=%s",
                            len(result) if isinstance(result, list) else 0,
                        )
                        if commit and connection:
                            await connection.commit()
                        return result if isinstance(result, list) else []

                    if commit and connection:
                        await connection.commit()
                    logger.info(
                        "[DB] exec ok rowcount=%s lastrowid=%s",
                        cursor.rowcount,
                        getattr(cursor, "lastrowid", None),
                    )
                    return True
                except aiomysql.Error as e:
                    category = self._classify_db_error(*self._extract_db_error_details(e)[1:])
                    if log_error:
                        self._log_db_error(e, query, params, query_name, category)
                    else:
                        logger.debug("DB error без логирования (log_error=False): %s", e)
                    retryable = category in {"connection_error", "timeout"}
                    raise DatabaseIntegrationError(
                        f"DB query failed ({category})",
                        user_visible=False,
                        retryable=retryable,
                        details={
                            "query_name": self._resolve_query_name(query_name),
                            "category": category,
                            "error_type": type(e).__name__,
                        },
                    ) from e

    async def execute_with_retry(
        self,
        query: str,
        params: Optional[Union[Tuple, List, Dict]] = None,
        fetchone: bool = False,
        fetchall: bool = False,
        *,
        commit: bool = False,
        retries: int = 3,
        base_delay: float = 0.5,
        query_name: Optional[str] = None,
    ) -> Any:
        """
        Выполнение SQL-запроса с повторными попытками.
        """
        last_error = None
        for attempt in range(1, retries + 1):
            try:
                return await self.execute_query(
                    query,
                    params=params,
                    fetchone=fetchone,
                    fetchall=fetchall,
                    commit=commit,
                    query_name=query_name,
                    log_error=False,
                )
            except Exception as error:
                is_db_like = isinstance(error, aiomysql.Error) or error.__class__.__module__.startswith("pymysql")
                if not isinstance(error, DatabaseIntegrationError) and not is_db_like:
                    raise
                wrapped_error: DatabaseIntegrationError
                if isinstance(error, DatabaseIntegrationError):
                    wrapped_error = error
                else:
                    error_type, error_code, message = self._extract_db_error_details(error)
                    category = self._classify_db_error(error_code, message)
                    retryable = category in {"connection_error", "timeout"}
                    wrapped_error = DatabaseIntegrationError(
                        f"DB query failed ({category})",
                        user_visible=False,
                        retryable=retryable,
                        details={
                            "query_name": self._resolve_query_name(query_name),
                            "category": category,
                            "error_type": error_type,
                        },
                    )
                last_error = wrapped_error
                if not is_retryable(wrapped_error):
                    if isinstance(error, DatabaseIntegrationError):
                        raise
                    raise wrapped_error from error
                retry_cfg = get_retry_config(wrapped_error)
                logger.warning(
                    f"Ошибка выполнения запроса. Попытка {attempt}/{retries}",
                    extra={
                        "query_name": self._resolve_query_name(query_name),
                        "error_type": type(wrapped_error).__name__,
                        "category": wrapped_error.details.get("category"),
                        "attempt": attempt,
                        "max_attempts": min(retries, retry_cfg.max_retries),
                    },
                )
                if attempt >= min(retries, retry_cfg.max_retries):
                    raise wrapped_error from error
                delay = retry_cfg.base_delay if retry_cfg.base_delay else base_delay
                if retry_cfg.exponential_backoff:
                    delay = min(
                        retry_cfg.max_delay,
                        (retry_cfg.base_delay or base_delay) * (2 ** (attempt - 1)),
                    )
                await asyncio.sleep(delay)
        
        if last_error:
            raise last_error

    # Поддержка контекстного менеджера для самого класса
    async def __aenter__(self):
        await self.create_pool()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close_pool()
