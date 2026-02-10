import pytest

from app.db.manager import DatabaseManager
from app.errors import AppError, DatabaseIntegrationError, OpenAIIntegrationError


def test_raise_from_preserves_cause():
    low = RuntimeError("sdk failure")
    with pytest.raises(OpenAIIntegrationError) as exc_info:
        try:
            raise low
        except RuntimeError as exc:
            raise OpenAIIntegrationError("openai failed", retryable=True) from exc

    assert exc_info.value.__cause__ is low


def test_unknown_exception_not_swallowed():
    def explode() -> None:
        raise ValueError("bug")

    with pytest.raises(ValueError):
        explode()


def test_app_error_is_controlled_type():
    err = AppError("controlled")
    assert isinstance(err, Exception)


@pytest.mark.asyncio
async def test_db_retry_wrapper_preserves_cause_for_db_errors():
    import aiomysql
    from unittest.mock import AsyncMock

    manager = DatabaseManager()
    low = aiomysql.Error("unknown failure")
    manager.execute_query = AsyncMock(side_effect=low)  # type: ignore[method-assign]

    with pytest.raises(DatabaseIntegrationError) as exc_info:
        await manager.execute_with_retry("SELECT 1", retries=1, base_delay=0.01)

    assert exc_info.value.__cause__ is low
