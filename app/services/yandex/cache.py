from __future__ import annotations

import asyncio
import logging
from typing import Optional, Tuple

try:
    import redis.asyncio as redis
except Exception:  # pragma: no cover
    redis = None  # type: ignore

from .disk import YandexDiskClient
from app.utils.best_effort import best_effort_async


logger = logging.getLogger(__name__)


class YandexDiskCache:
    PATH_KEY = "yd:mp3:path:{recording_id}"
    TG_FILE_KEY = "yd:tg:file_id:{recording_id}"

    def __init__(
        self,
        redis_url: Optional[str],
        *,
        file_ttl_seconds: Optional[int] = None,
    ):
        self.redis_url = redis_url
        self.file_ttl_seconds = file_ttl_seconds
        self._redis = (
            redis.from_url(
                redis_url,
                encoding="utf-8",
                decode_responses=True,
                socket_timeout=5,
            )
            if redis and redis_url
            else None
        )
        self._index_lock = asyncio.Lock()

    @property
    def enabled(self) -> bool:
        return self._redis is not None

    async def close(self) -> None:
        if self._redis:
            await self._redis.close()

    def _path_key(self, recording_id: str) -> str:
        return self.PATH_KEY.format(recording_id=recording_id)

    def _file_key(self, recording_id: str) -> str:
        return self.TG_FILE_KEY.format(recording_id=recording_id)

    async def get_path(self, recording_id: str) -> Optional[str]:
        if not self._redis:
            return None
        result = await best_effort_async(
            "best_effort_yandex_cache_get_path",
            self._redis.get(self._path_key(recording_id)),
            on_error_result=None,
            details={"recording_id": recording_id},
        )
        return result.value

    async def save_path(self, recording_id: str, path: str) -> None:
        if not self._redis:
            return
        await best_effort_async(
            "best_effort_yandex_cache_save_path",
            self._redis.set(self._path_key(recording_id), path),
            on_error_result=None,
            details={"recording_id": recording_id},
        )

    async def delete_path(self, recording_id: str) -> None:
        if not self._redis:
            return
        await best_effort_async(
            "best_effort_yandex_cache_delete_path",
            self._redis.delete(self._path_key(recording_id)),
            on_error_result=None,
            details={"recording_id": recording_id},
        )

    async def get_file_id(self, recording_id: str) -> Optional[str]:
        if not self._redis:
            return None
        result = await best_effort_async(
            "best_effort_yandex_cache_get_file_id",
            self._redis.get(self._file_key(recording_id)),
            on_error_result=None,
            details={"recording_id": recording_id},
        )
        return result.value

    async def save_file_id(self, recording_id: str, file_id: str) -> None:
        if not self._redis:
            return
        if self.file_ttl_seconds:
            await best_effort_async(
                "best_effort_yandex_cache_save_file_id_ttl",
                self._redis.setex(
                    self._file_key(recording_id),
                    self.file_ttl_seconds,
                    file_id,
                ),
                on_error_result=None,
                details={"recording_id": recording_id},
            )
            return
        await best_effort_async(
            "best_effort_yandex_cache_save_file_id",
            self._redis.set(self._file_key(recording_id), file_id),
            on_error_result=None,
            details={"recording_id": recording_id},
        )

    async def delete_file_id(self, recording_id: str) -> None:
        if not self._redis:
            return
        await best_effort_async(
            "best_effort_yandex_cache_delete_file_id",
            self._redis.delete(self._file_key(recording_id)),
            on_error_result=None,
            details={"recording_id": recording_id},
        )

    async def refresh_index(
        self,
        client: YandexDiskClient,
        *,
        limit: int = 500,
    ) -> int:
        if not self._redis:
            logger.warning("Redis не настроен, индексация пропущена.")
            return 0
        if not client:
            logger.warning("YandexDiskClient отсутствует, индексация невозможна.")
            return 0

        if self._index_lock.locked():
            logger.warning("[YDisk] Индексация уже выполняется, пропускаем повторный запуск.")
            return 0
        async with self._index_lock:
            offset = 0
            total = 0
            while True:
                page = await client._fetch_directory_page(offset, limit)
                if page is None:
                    break
                items, has_more = page
                if not items:
                    if not has_more:
                        break
                    offset += limit
                    continue
                pipe = self._redis.pipeline()
                added = 0
                for item in items:
                    if item.get("type") != "file":
                        continue
                    name = item.get("name") or ""
                    path = item.get("path")
                    recording_id = self._extract_recording_id(name)
                    if recording_id and path:
                        pipe.set(self._path_key(recording_id), path)
                        added += 1
                if added:
                    execute_result = await best_effort_async(
                        "best_effort_yandex_cache_refresh_index_execute_pipe",
                        pipe.execute(),
                        on_error_result=None,
                        details={"offset": offset, "limit": limit},
                    )
                    if execute_result.status == "error":
                        break
                total += added
                if not has_more:
                    break
                offset += limit
            logger.info("[YDisk] Индексация завершена: обновлено %s записей.", total)
            return total

    @staticmethod
    def _extract_recording_id(filename: str) -> Optional[str]:
        if not filename:
            return None
        base = filename
        if "." in base:
            base = base.rsplit(".", 1)[0]
        candidate = base.split("_")[-1]
        return candidate or None
