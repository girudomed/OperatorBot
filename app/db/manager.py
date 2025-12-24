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
        msg = message.lower()
        if error_code in {1054, 1146, 1060, 1062} or "unknown column" in msg or "unknown table" in msg:
            return "schema_error"
        if error_code in {2003, 2006, 2013} or "connection" in msg:
            return "connection_error"
        if "timeout" in msg:
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
        
        # Для критических ошибок схемы или специфичных кодов (1054 - Unknown column)
        # принудительно выводим контекст в текст сообщения для удобства.
        log_msg = f"Ошибка выполнения SQL-запроса ({resolved_category}): {message}"
        if error_code == 1054 or resolved_category == "schema_error":
            log_msg = f"КРИТИЧЕСКАЯ ОШИБКА СХЕМЫ (1054): {message}\nSQL: {query}\nParams: {repr(params)}"
        
        logger.error(
            log_msg,
            extra={
                "error_type": error_type,
                "error_code": error_code,
                "error_message": message,
                "query_name": self._resolve_query_name(query_name),
                "sql": query,
                "params": repr(params),
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
                        autocommit=DB_CONFIG.get("autocommit", True),
                        minsize=DB_CONFIG.get("minsize", 1),
                        maxsize=DB_CONFIG.get("maxsize", 50),
                        cursorclass=aiomysql.DictCursor
                    )
                    logger.info("Пул соединений с БД успешно создан.")
                except Exception as e:
                    logger.error(f"Ошибка при создании пула соединений: {e}", exc_info=True)
                    raise

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
        except Exception as e:
            logger.error(f"Ошибка при получении соединения: {e}", exc_info=True)
            raise
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

        if not self.pool:
            await self.create_pool()
            
        async with self.acquire() as connection:
            async with connection.cursor() as cursor:
                try:
                    query_preview = " ".join(query.split()) if isinstance(query, str) else str(query)
                    logger.info(
                        "[DB] Executing query: %s | params=%s",
                        query_preview,
                        params,
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
                        logger.info("[DB] fetchone result=%r", result)
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
                except Exception as e:
                    if log_error:
                        self._log_db_error(e, query, params, query_name)
                    else:
                        logger.debug("DB error без логирования (log_error=False): %s", e, exc_info=True)
                    raise

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
            except (aiomysql.Error, RuntimeError) as error:
                last_error = error
                error_type, error_code, message = self._extract_db_error_details(error)
                category = self._classify_db_error(error_code, message)
                self._log_db_error(error, query, params, query_name, category)
                if category == "schema_error":
                    raise
                logger.warning(
                    f"Ошибка выполнения запроса. Попытка {attempt}/{retries}",
                    extra={
                        "query_name": self._resolve_query_name(query_name),
                        "error_type": error_type,
                        "error_code": error_code,
                        "category": category,
                        "attempt": attempt,
                        "max_attempts": retries,
                    },
                )
                if attempt == retries:
                    raise
                await asyncio.sleep(base_delay * attempt)
        
        if last_error:
            raise last_error

    # Поддержка контекстного менеджера для самого класса
    async def __aenter__(self):
        await self.create_pool()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close_pool()
