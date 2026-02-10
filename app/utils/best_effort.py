from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional, TypeVar

from app.logging_config import get_trace_id, get_watchdog_logger


logger = get_watchdog_logger(__name__)
T = TypeVar("T")


@dataclass
class BestEffortResult:
    status: str
    value: Any = None
    error: Optional[Exception] = None


async def best_effort_async(
    op_name: str,
    coro: Awaitable[T],
    *,
    on_error_result: Any = None,
    details: Optional[dict[str, Any]] = None,
) -> BestEffortResult:
    extra = {
        "event": "best_effort",
        "operation": op_name,
        "trace_id": get_trace_id(),
        **(details or {}),
    }
    try:
        value = await coro
    except Exception as exc:
        logger.warning(
            "best_effort operation failed",
            extra={
                **extra,
                "status": "error",
                "error_type": type(exc).__name__,
                "error_message": str(exc),
            },
            exc_info=True,
        )
        return BestEffortResult(status="error", value=on_error_result, error=exc)

    logger.info(
        "best_effort operation completed",
        extra={**extra, "status": "success"},
    )
    return BestEffortResult(status="success", value=value, error=None)


def best_effort_sync(
    op_name: str,
    fn: Callable[..., T],
    *args: Any,
    on_error_result: Any = None,
    details: Optional[dict[str, Any]] = None,
    **kwargs: Any,
) -> BestEffortResult:
    if inspect.iscoroutinefunction(fn):
        raise TypeError("best_effort_sync expected a sync callable")

    extra = {
        "event": "best_effort",
        "operation": op_name,
        "trace_id": get_trace_id(),
        **(details or {}),
    }
    try:
        value = fn(*args, **kwargs)
    except Exception as exc:
        logger.warning(
            "best_effort operation failed",
            extra={
                **extra,
                "status": "error",
                "error_type": type(exc).__name__,
                "error_message": str(exc),
            },
            exc_info=True,
        )
        return BestEffortResult(status="error", value=on_error_result, error=exc)

    logger.info(
        "best_effort operation completed",
        extra={**extra, "status": "success"},
    )
    return BestEffortResult(status="success", value=value, error=None)
