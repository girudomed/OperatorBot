import asyncio
import pytest

import httpx

from app.services.yandex.disk import YandexDiskClient, logger as ydisk_logger
import logging


class DummyResponse:
    def __init__(
        self,
        *,
        status_code=200,
        text="",
        content=b"",
        headers=None,
        json_data=None,
        json_exc=None,
    ):
        self.status_code = status_code
        self.text = text
        self.content = content
        self.headers = headers or {}
        self._json_data = json_data
        self._json_exc = json_exc

    def json(self):
        if self._json_exc:
            raise self._json_exc
        if self._json_data is None:
            raise ValueError("No JSON")
        return self._json_data


class DummyAsyncClient:
    def __init__(self, response):
        self._response = response

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, *args, **kwargs):
        return self._response

    async def request(self, *args, **kwargs):
        return await self.get(*args, **kwargs)


@pytest.mark.asyncio
async def test_download_recording_handles_download_link_401(monkeypatch):
    responses = iter(
        [
            DummyResponse(status_code=401, text="Unauthorized"),
            DummyResponse(status_code=500, text="error"),
        ]
    )

    def fake_client(*args, **kwargs):
        try:
            response = next(responses)
        except StopIteration:
            raise AssertionError("Unexpected AsyncClient creation")
        return DummyAsyncClient(response)

    monkeypatch.setattr(httpx, "AsyncClient", fake_client)

    client = YandexDiskClient(login=None, password=None, oauth_token="token", base_path="/mango")
    monkeypatch.setattr(
        client,
        "_build_filename_candidates",
        lambda *a, **kw: ["test.mp3"],
    )

    result = await client.download_recording("rec-id")
    assert result is None


@pytest.mark.asyncio
async def test_request_download_link_handles_invalid_json(monkeypatch):
    responses = iter(
        [
            DummyResponse(
                status_code=200,
                text="broken",
                json_exc=ValueError("oops"),
            )
        ]
    )

    def fake_client(*args, **kwargs):
        try:
            response = next(responses)
        except StopIteration:
            raise AssertionError("Unexpected AsyncClient creation")
        return DummyAsyncClient(response)

    monkeypatch.setattr(httpx, "AsyncClient", fake_client)

    client = YandexDiskClient(login=None, password=None, oauth_token="token", base_path="/mango")
    href = await client._request_download_link("/mango/test.mp3", {})
    assert href is None


@pytest.mark.asyncio
async def test_download_recording_reraises_unexpected_exception(monkeypatch, caplog):
    async def boom(*args, **kwargs):
        raise RuntimeError("boom")

    client = YandexDiskClient(login=None, password=None, oauth_token="token", base_path="/mango")
    monkeypatch.setattr(client, "_build_filename_candidates", lambda *a, **k: ["file.mp3"])
    monkeypatch.setattr(client, "_download_file", boom)

    caplog.set_level(logging.ERROR, ydisk_logger.name)
    result = await client.download_recording("rid")
    assert result is None
    assert any("Непредвиденная ошибка при загрузке записи" in rec.message for rec in caplog.records)


@pytest.mark.asyncio
async def test_request_download_link_reraises_unexpected(monkeypatch):
    class RaisingClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, *args, **kwargs):
            raise RuntimeError("boom")

    monkeypatch.setattr(httpx, "AsyncClient", lambda *a, **k: RaisingClient())
    client = YandexDiskClient(login=None, password=None, oauth_token="token", base_path="/mango")

    href = await client._request_download_link("/mango/file", {})
    assert href is None


@pytest.mark.asyncio
async def test_search_filename_finds_match(monkeypatch):
    responses = iter(
        [
            DummyResponse(
                status_code=200,
                json_data={
                    "_embedded": {
                        "items": [
                            {
                                "type": "file",
                                "name": "2024_rec_MTox",
                                "path": "/mango_data/2024_rec_MTox",
                            }
                        ]
                    }
                },
            )
        ]
    )

    def fake_client(*args, **kwargs):
        try:
            response = next(responses)
        except StopIteration:
            raise AssertionError("Unexpected AsyncClient creation")
        return DummyAsyncClient(response)

    monkeypatch.setattr(httpx, "AsyncClient", fake_client)

    client = YandexDiskClient(login=None, password=None, oauth_token="token", base_path="/mango_data")
    path = await client._search_path("MTox")
    assert path == "/mango_data/2024_rec_MTox"


@pytest.mark.asyncio
async def test_search_filename_handles_http_error(monkeypatch):
    responses = iter(
        [
            DummyResponse(status_code=403, text="forbidden"),
        ]
    )

    def fake_client(*args, **kwargs):
        try:
            response = next(responses)
        except StopIteration:
            raise AssertionError("Unexpected AsyncClient creation")
        return DummyAsyncClient(response)

    monkeypatch.setattr(httpx, "AsyncClient", fake_client)
    client = YandexDiskClient(login=None, password=None, oauth_token="token", base_path="/mango")
    result = await client._search_path("MTox")
    assert result is None
