import os
import sys
import types
import asyncio
import pytest
from types import SimpleNamespace

from openai import OpenAIError

from app.telegram.utils.callback_data import AdminCB
from app.services.openai_service import OpenAIService
import app.config as app_config


@pytest.mark.asyncio
async def test_resolve_hash_async_handles_redis_exceptions(monkeypatch):
    """
    Если при попытке доступа к Redis происходит исключение, resolve_hash_async
    должен поймать его, залогировать и вернуть None (не выбрасывать).
    Для теста подставляем фейковый модуль redis.asyncio.Redis, который бросает.
    """
    # Установим REDIS_URL, чтобы код пытался обратиться к Redis
    monkeypatch.setenv("REDIS_URL", "redis://localhost/0")

    # Подготовим фейковый модуль redis.asyncio с классом Redis, возвращающим клиент,
    # у которого get() бросает исключение при await.
    redis_mod = types.ModuleType("redis")
    redis_async = types.ModuleType("redis.asyncio")

    class FakeRedis:
        @classmethod
        def from_url(cls, *args, **kwargs):
            class Client:
                async def get(self, key):
                    raise Exception("redis failure")
                async def close(self):
                    return None
            return Client()

    redis_async.Redis = FakeRedis
    sys.modules["redis"] = redis_mod
    sys.modules["redis.asyncio"] = redis_async

    # Вызов должен вернуть None и не выбросить исключение
    got = await AdminCB.resolve_hash_async("nonexistent")
    assert got is None


@pytest.mark.asyncio
async def test_openai_service_retries_and_fallback(monkeypatch):
    """
    Генератор рекомендаций должен ловить OpenAIError и после попыток вернуть fallback-строку.
    Подменяем клиент на объект, у которого chat.completions.create всегда бросает OpenAIError.
    """
    # Убедимся, что есть ключ API в конфиге, требуемый конструктором
    monkeypatch.setattr(app_config, "OPENAI_API_KEY", "test-key", raising=False)

    svc = OpenAIService(model="test-model")

    async def raise_openai(*args, **kwargs):
        raise OpenAIError("simulated openai failure")

    # Подменяем client.chat.completions.create
    svc.client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=raise_openai)))

    result = await svc.generate_recommendations("hello", max_retries=2, max_tokens=10)
    assert isinstance(result, str)
    assert result.startswith("Ошибка:")
