from app.error_policy import get_retry_config, is_retryable
from app.errors import OpenAIIntegrationError


def test_retry_only_for_retryable():
    retryable = OpenAIIntegrationError("tmp", retryable=True)
    non_retryable = OpenAIIntegrationError("perm", retryable=False)

    assert is_retryable(retryable) is True
    assert get_retry_config(retryable).enabled is True

    assert is_retryable(non_retryable) is False
    assert get_retry_config(non_retryable).enabled is False


def test_unknown_exception_not_retryable():
    assert is_retryable(RuntimeError("bug")) is False
    assert get_retry_config(RuntimeError("bug")).enabled is False
