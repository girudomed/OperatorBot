"""
Клиент для загрузки записей с Яндекс.Диска через REST API (download-link).
"""

from __future__ import annotations

import base64
import binascii
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence, Tuple
from urllib.parse import unquote

import httpx

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

from app.logging_config import get_watchdog_logger
from app.errors import YandexDiskIntegrationError

logger = get_watchdog_logger(__name__)

YDISK_LOGIN = os.getenv("YDISK_LOGIN")
YDISK_PASSWORD = os.getenv("YDISK_PASSWORD")
YDISK_PATH = os.getenv("YDISK_PATH", "/mango_data/")
YDISK_OAUTH_TOKEN = (
    os.getenv("YDISK_OAUTH_TOKEN")
    or os.getenv("YDISK_OAUTH")
    or os.getenv("YDISK_TOKEN")
)
TIMEZONE_NAME = os.getenv("TIMEZONE", "Europe/Moscow")

try:
    LOCAL_TZ = ZoneInfo(TIMEZONE_NAME)
except Exception:  # pragma: no cover - в тестах может не быть tzdata
    LOCAL_TZ = timezone.utc


@dataclass
class YandexDiskRecording:
    """Описание записи, загруженной с Яндекс.Диска."""

    filename: str
    content: bytes
    content_type: Optional[str] = None
    path: Optional[str] = None


class YandexDiskClient:
    """Клиент Yandex.Диска, работающий через REST API (download-link)."""

    DOWNLOAD_URL = "https://cloud-api.yandex.net/v1/disk/resources/download"
    LIST_URL = "https://cloud-api.yandex.net/v1/disk/resources"

    def __init__(
        self,
        login: Optional[str],
        password: Optional[str],
        oauth_token: Optional[str] = None,
        base_path: str = "/mango_data/",
        *,
        timeout: float = 120.0,
    ):
        self.login = login
        self.password = password
        self.oauth_token = oauth_token
        self.base_path = self._normalize_path(base_path)
        self.timeout = timeout
        self._auth_header = self._build_auth_header()

    @classmethod
    def from_env(cls) -> "YandexDiskClient":
        return cls(YDISK_LOGIN, YDISK_PASSWORD, YDISK_OAUTH_TOKEN, YDISK_PATH)

    @staticmethod
    def _normalize_path(path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        if not path.endswith("/"):
            path += "/"
        return path

    @property
    def is_configured(self) -> bool:
        return bool(self.oauth_token or (self.login and self.password))

    def _build_auth_header(self) -> Optional[str]:
        if self.oauth_token:
            return f"OAuth {self.oauth_token}"
        if self.login and self.password:
            token = base64.b64encode(
                f"{self.login}:{self.password}".encode("utf-8")
            ).decode("ascii")
            return f"Basic {token}"
        return None

    async def download_recording(
        self,
        recording_id: str,
        *,
        call_time: Optional[datetime] = None,
        phone_candidates: Optional[Sequence[Optional[str]]] = None,
    ) -> Optional[YandexDiskRecording]:
        """
        Загружает запись по recording_id.
        Сначала пытается угадать имя файла по времени/номеру, затем ищет по подстроке.
        """
        if not recording_id:
            return None
        if not self.oauth_token:
            logger.warning("[YDisk] OAuth-токен не задан, загрузка невозможна.")
            return None

        candidates = self._build_filename_candidates(
            recording_id, call_time=call_time, phone_candidates=phone_candidates
        )
        logger.debug(
            "[YDisk] Попытка загрузки %s, кандидатов файлов: %s",
            recording_id,
            candidates,
        )
        for name in candidates:
            logger.debug("[YDisk] Пробую файл %s для %s", name, recording_id)
            recording = await self._download_file(name)
            if recording:
                logger.info(
                    "[YDisk] Запись %s найдена по имени %s", recording_id, name
                )
                return recording

        logger.debug(
            "[YDisk] Прямой подбор для %s не сработал, переключаемся на поиск.",
            recording_id,
        )
        resolved_path = await self._search_path(recording_id)
        if resolved_path:
            target_name = resolved_path.rsplit("/", 1)[-1]
            logger.info(
                "[YDisk] Поиск по %s нашёл файл %s, начинаю скачивание.",
                recording_id,
                target_name,
            )
            return await self._download_file(target_name, explicit_path=resolved_path)

        logger.warning(
            "[YDisk] Не удалось найти запись %s в %s", recording_id, self.base_path
        )
        return None

    def _build_filename_candidates(
        self,
        recording_id: str,
        *,
        call_time: Optional[datetime],
        phone_candidates: Optional[Sequence[Optional[str]]],
    ) -> List[str]:
        result: List[str] = []
        phone = self._resolve_phone(recording_id, phone_candidates or [])
        ts = self._normalize_datetime(call_time)
        if ts:
            base = self._build_filename(ts, phone, recording_id)
            result.append(base)
        # Простые fallback-варианты
        safe_id = re.sub(r"[^0-9A-Za-z_-]", "-", recording_id)
        for ext in (".mp3", ".wav", ".ogg"):
            result.append(f"{safe_id}{ext}")
        return list(dict.fromkeys(result))  # удаляем дубликаты, сохраняя порядок

    @staticmethod
    def _normalize_datetime(value: Optional[datetime]) -> Optional[datetime]:
        if not value:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=LOCAL_TZ)
        return value.astimezone(LOCAL_TZ)

    def _build_filename(self, ts: datetime, phone: str, recording_id: str) -> str:
        local_ts = ts.astimezone(LOCAL_TZ)
        date_part = local_ts.strftime("%Y-%m-%d")
        time_part = local_ts.strftime("%H-%M-%S")
        safe_phone = re.sub(r"[^0-9A-Za-z]", "", phone) or "unknown"
        safe_rid = re.sub(r"[^0-9A-Za-z_-]", "-", recording_id or "id")
        return f"{date_part}_{time_part}_{safe_phone}_{safe_rid}.mp3"

    def _resolve_phone(
        self,
        recording_id: str,
        phone_candidates: Sequence[Optional[str]],
    ) -> str:
        for candidate in phone_candidates:
            formatted = self._format_phone(candidate)
            if formatted:
                return formatted
        decoded = self._phone_from_recording_id(recording_id)
        if decoded:
            return decoded
        return "unknown"

    @staticmethod
    def _format_phone(number: Optional[str]) -> Optional[str]:
        if not number:
            return None
        digits = re.sub(r"\D", "", str(number))
        if not digits:
            return None
        if len(digits) > 11 and digits.startswith("007"):
            digits = digits[2:]
        if len(digits) == 11 and digits.startswith("8"):
            digits = "7" + digits[1:]
        if len(digits) == 10:
            digits = "7" + digits
        if len(digits) == 11 and digits.startswith("7"):
            return digits
        return None

    def _phone_from_recording_id(self, recording_id: str) -> Optional[str]:
        try:
            decoded = base64.b64decode(recording_id).decode("utf-8", errors="ignore")
        except (binascii.Error, ValueError):
            return None

        match = re.search(r"(\+7\d{10}|7\d{10}|8\d{10}|\d{10})", decoded)
        if match:
            formatted = self._format_phone(match.group(1))
            if formatted:
                return formatted

        for part in re.split(r"[^\+\d]+", decoded):
            if not part:
                continue
            formatted = self._format_phone(part)
            if formatted:
                return formatted

        for chunk in re.findall(r"\d{6,}", decoded):
            formatted = self._format_phone(chunk)
            if formatted:
                return formatted

        digits = re.findall(r"\d{6,}", decoded)
        if digits:
            candidate = digits[0]
            if len(candidate) == 10:
                formatted = self._format_phone("7" + candidate)
                if formatted:
                    return formatted
            if len(candidate) == 11 and candidate.startswith("8"):
                formatted = self._format_phone("7" + candidate[1:])
                if formatted:
                    return formatted
        return None

    async def download_by_path(self, path: str) -> Optional[YandexDiskRecording]:
        filename = path.rsplit("/", 1)[-1] if path else ""
        return await self._download_file(filename, explicit_path=path)

    async def _download_file(self, filename: str, *, explicit_path: Optional[str] = None) -> Optional[YandexDiskRecording]:
        path = explicit_path or self._build_full_path(filename)
        headers = {}
        if self._auth_header:
            headers["Authorization"] = self._auth_header
        href = await self._request_download_link(path, headers)
        if not href:
            logger.debug(
                "[YDisk] download-link не получен для %s, пропускаю скачивание.",
                path,
            )
            return None
        logger.debug("[YDisk] Начинаю скачивание %s по href=%s", filename, href)
        try:
            async with httpx.AsyncClient(
                timeout=self.timeout,
                follow_redirects=True,
                transport=httpx.AsyncHTTPTransport(retries=3),
            ) as client:
                resp = await client.get(href)
        except httpx.HTTPError as exc:
            logger.warning("[YDisk] Ошибка загрузки %s: %s", filename, exc)
            raise YandexDiskIntegrationError(
                "Failed to download file from Yandex Disk",
                retryable=True,
                user_visible=False,
                details={"filename": filename, "path": path},
            ) from exc

        if resp.status_code == httpx.codes.OK:
            return YandexDiskRecording(
                filename=filename,
                content=resp.content,
                content_type=resp.headers.get("Content-Type"),
                path=path,
            )

        logger.warning(
            "[YDisk] HTTP %s при скачивании %s через href: %s",
            resp.status_code,
            filename,
            resp.text[:200],
        )
        return None

    async def _request_download_link(self, path: str, headers: dict) -> Optional[str]:
        if not self._auth_header:
            return None
        headers = dict(headers)
        headers["Authorization"] = self._auth_header
        try:
            async with httpx.AsyncClient(
                timeout=self.timeout,
                transport=httpx.AsyncHTTPTransport(retries=3),
            ) as client:
                resp = await client.get(
                    self.DOWNLOAD_URL,
                    params={"path": path},
                    headers=headers,
                )
        except httpx.HTTPError as exc:
            logger.warning("[YDisk] Ошибка download-link для %s: %s", path, exc)
            raise YandexDiskIntegrationError(
                "Failed to request Yandex Disk download link",
                retryable=True,
                user_visible=False,
                details={"path": path},
            ) from exc

        if resp.status_code == httpx.codes.OK:
            try:
                data = resp.json()
            except ValueError as exc:
                raise YandexDiskIntegrationError(
                    "Invalid JSON in Yandex Disk download-link response",
                    retryable=False,
                    user_visible=False,
                    details={"path": path},
                ) from exc
            if not isinstance(data, dict):
                logger.warning("[YDisk] Неожиданный формат download-link для %s: %s", path, type(data))
                return None
            href = data.get("href")
            if not href:
                logger.warning("[YDisk] download-link без href для %s: %s", path, data)
            else:
                logger.debug("[YDisk] Получен download-link для %s", path)
            return href

        if resp.status_code == httpx.codes.NOT_FOUND:
            logger.debug("[YDisk] Файл %s не найден (404)", path)
            return None

        logger.warning(
            "[YDisk] download-link HTTP %s для %s: %s",
            resp.status_code,
            path,
            resp.text[:200],
        )
        return None

    async def _search_path(self, recording_id: str) -> Optional[str]:
        """Ищет файл в каталоге через /resources?path=..."""
        if not self._auth_header:
            logger.warning(
                "[YDisk] Невозможно выполнить поиск %s — нет OAuth-токена",
                recording_id,
            )
            return None

        limit = 500
        offset = 0

        while True:
            page = await self._fetch_directory_page(offset, limit, recording_id=recording_id)
            if page is None:
                return None
            items, has_more = page
            if not items:
                logger.debug("[YDisk] Каталог %s пуст (offset=%s).", self.base_path, offset)

            for item in items:
                if item.get("type") != "file":
                    continue
                name = item.get("name") or ""
                path = item.get("path") or ""
                decoded = unquote(path)
                if recording_id and (recording_id in name or recording_id in decoded):
                    logger.debug(
                        "[YDisk] Найден файл %s для %s (offset=%s)",
                        name or decoded,
                        recording_id,
                        offset,
                    )
                    full_path = decoded or path or self._build_full_path(name)
                    return full_path

            if not has_more:
                break
            offset += limit

        logger.debug("[YDisk] В каталоге %s нет файла для %s", self.base_path, recording_id)
        return None

    async def _fetch_directory_page(
        self,
        offset: int,
        limit: int,
        *,
        recording_id: Optional[str] = None,
    ) -> Optional[Tuple[List[Dict], bool]]:
        if not self._auth_header:
            return None
        base_path = self.base_path.rstrip("/")
        headers = {"Authorization": self._auth_header}
        params = {
            "path": base_path,
            "limit": limit,
            "offset": offset,
        }
        logger.debug(
            "[YDisk] Загружаю список файлов (offset=%s, limit=%s) для %s",
            offset,
            limit,
            recording_id or "*",
        )
        try:
            async with httpx.AsyncClient(
                timeout=self.timeout,
                transport=httpx.AsyncHTTPTransport(retries=3),
            ) as client:
                resp = await client.get(self.LIST_URL, params=params, headers=headers)
        except httpx.HTTPError as exc:
            logger.warning("[YDisk] Ошибка получения списка %s: %s", recording_id or "*", exc)
            raise YandexDiskIntegrationError(
                "Failed to fetch Yandex Disk directory page",
                retryable=True,
                user_visible=False,
                details={"offset": offset, "limit": limit, "recording_id": recording_id},
            ) from exc

        if resp.status_code != httpx.codes.OK:
            logger.warning(
                "[YDisk] Список файлов %s вернул HTTP %s: %s",
                recording_id or "*",
                resp.status_code,
                resp.text[:200],
            )
            return None

        try:
            data = resp.json()
        except ValueError as exc:
            logger.warning("[YDisk] Некорректный JSON каталога %s: %s", recording_id or "*", exc)
            return None
        if not isinstance(data, dict):
            logger.warning(
                "[YDisk] Каталог %s вернул неожиданный тип %s",
                recording_id or "*",
                type(data),
            )
            return None

        embedded = data.get("_embedded")
        if not isinstance(embedded, dict):
            logger.warning("[YDisk] Ответ каталога %s без _embedded", recording_id or "*")
            return None
        items = embedded.get("items") or []
        if not isinstance(items, list):
            logger.warning("[YDisk] Неверный формат items для %s: %s", recording_id or "*", type(items))
            return None

        has_more = len(items) >= limit
        return items, has_more

    def _build_full_path(self, filename: str) -> str:
        return f"{self.base_path.rstrip('/')}/{filename}"
