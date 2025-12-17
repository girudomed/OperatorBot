from __future__ import annotations

from dataclasses import dataclass
from typing import List

from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)


CALLBACK_PREFIX = "cb"
CALLBACK_ERROR = "cb:error"
MAX_LENGTH_BYTES = 64


def pack(prefix: str, *parts: object) -> str:
    payload_parts = [CALLBACK_PREFIX, prefix, *[str(part) for part in parts]]
    data = ":".join(payload_parts)
    if len(data.encode("utf-8")) > MAX_LENGTH_BYTES:
        logger.warning(
            "callback_data overflow (%s bytes) for %s, returning error token",
            len(data.encode("utf-8")),
            data,
        )
        return CALLBACK_ERROR
    return data


@dataclass
class CallbackData:
    prefix: str
    parts: List[str]


def unpack(data: str | None) -> CallbackData:
    if not data:
        return CallbackData(prefix="", parts=[])
    parts = data.split(":")
    if not parts or parts[0] != CALLBACK_PREFIX:
        return CallbackData(prefix="", parts=parts)
    if len(parts) < 2:
        return CallbackData(prefix="", parts=[])
    return CallbackData(prefix=parts[1], parts=parts[2:])
