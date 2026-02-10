import pytest

from app.errors import AppError, OpenAIIntegrationError


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
