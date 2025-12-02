"""
Менеджер подключения к базе данных.
"""

import asyncio
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
                self.pool.release(conn)

    async def execute_query(
        self, 
        query: str, 
        params: Optional[Union[Tuple, List, Dict]] = None, 
        fetchone: bool = False, 
        fetchall: bool = False
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
        if not self.pool:
            await self.create_pool()
            
        async with self.acquire() as connection:
            async with connection.cursor() as cursor:
                try:
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
                        return result if isinstance(result, dict) else {}
                
                    if fetchall:
                        result = await cursor.fetchall()
                        return result if isinstance(result, list) else []

                    return True
                except Exception as e:
                    logger.error(f"Ошибка выполнения запроса: {query}, параметры: {params}, ошибка: {e}")
                    raise

    async def execute_with_retry(
        self,
        query: str,
        params: Optional[Union[Tuple, List, Dict]] = None,
        fetchone: bool = False,
        fetchall: bool = False,
        retries: int = 3,
        base_delay: float = 0.5,
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
                )
            except (aiomysql.Error, RuntimeError) as error:
                last_error = error
                logger.warning(
                    f"Ошибка выполнения запроса. Попытка {attempt}/{retries}: {error}"
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
