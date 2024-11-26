import asyncio
import pytest
from aiohttp.test_utils import TestClient, TestServer
from openai_telebot import app, trigger_error
from aiohttp import web


@pytest.mark.asyncio
async def test_hello_route():
    """Тест для корневого маршрута."""
    async with TestClient(TestServer(app)) as client:
        response = await client.get("/")
        assert response.status == 200
        assert await response.text() == "Hello, world"

@pytest.mark.asyncio
async def test_trigger_error(unused_tcp_port_factory):
    """Тест для маршрута с искусственной ошибкой."""
    port = unused_tcp_port_factory()
    loop = asyncio.get_event_loop()
    app = web.Application()
    app.add_routes([web.get("/error", trigger_error)])

    async with TestClient(TestServer(app, port=port), loop=loop) as client:
        response = await client.get("/error")
        assert response.status == 500