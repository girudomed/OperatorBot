from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.telegram.handlers.system_menu import SystemMenuHandler


def _make_handler() -> SystemMenuHandler:
    permissions = SimpleNamespace(
        is_supreme_admin=lambda *_args, **_kwargs: True,
        is_dev_admin=lambda *_args, **_kwargs: True,
        get_effective_role=AsyncMock(return_value="founder"),
    )
    return SystemMenuHandler(db_manager=SimpleNamespace(), permissions=permissions)


def test_grep_logs_skips_polling_noise(tmp_path):
    log_path = tmp_path / "errors.log"
    log_path.write_text(
        "\n".join(
            [
                "2026-02-10 09:07:33 - telegram.ext.Updater - ERROR - Exception happened while polling for updates.",
                "2026-02-10 09:07:33 | Traceback (most recent call last):",
                "2026-02-10 09:07:33 |     self.gen.throw(typ, value, traceback)",
                "2026-02-10 09:19:08 - app.core - ERROR - real incident",
            ]
        ),
        encoding="utf-8",
    )
    handler = _make_handler()
    rows = handler._grep_logs([log_path], limit=10)
    rendered = "\n".join(rows)
    assert "real incident" in rendered
    assert "Exception happened while polling for updates" not in rendered
    assert "Traceback (most recent call last):" not in rendered
    assert "self.gen.throw(typ, value, traceback)" not in rendered


def test_grep_logs_adds_timestamp_for_continuation_lines(tmp_path):
    log_path = tmp_path / "errors.log"
    log_path.write_text(
        "\n".join(
            [
                "2026-02-10 09:19:08 - app.core - ERROR - first incident",
                "Traceback (most recent call last):",
            ]
        ),
        encoding="utf-8",
    )
    handler = _make_handler()
    rows = handler._grep_logs([log_path], limit=10)
    rendered = "\n".join(rows)
    assert "2026-02-10 09:19:08 - app.core - ERROR - first incident" in rendered
    assert "2026-02-10 09:19:08 | Traceback (most recent call last):" in rendered


def test_grep_logs_skips_sql_noise_with_result_preview(tmp_path):
    log_path = tmp_path / "operabot.log"
    log_path.write_text(
        "\n".join(
            [
                "2026-02-10 12:11:01 - app.telegram.handlers.admin_panel - ERROR - Не удалось отправить выгрузку: Timed out",
                "2026-02-10 12:11:01 | Traceback (most recent call last):",
                "2026-02-10 12:14:11 - app.db.manager - INFO - [DB] Executing query: INSERT INTO admin_action_logs (...) | params=('system_action', '{\"result_preview\": \"Traceback (most recent call last): ...\"}')",
            ]
        ),
        encoding="utf-8",
    )
    handler = _make_handler()
    rows = handler._grep_logs([log_path], limit=10)
    rendered = "\n".join(rows)
    assert "Не удалось отправить выгрузку: Timed out" in rendered
    assert "Traceback (most recent call last):" in rendered
    assert "result_preview" not in rendered
    assert "[DB] Executing query" not in rendered


@pytest.mark.asyncio
async def test_log_system_action_for_errors_uses_summary_payload():
    handler = _make_handler()
    handler.action_logger.log_action = AsyncMock(return_value=True)  # type: ignore[method-assign]

    await handler._log_system_action(
        user_id=11,
        action="errors",
        text="Traceback (most recent call last):\nboom",
    )

    assert handler.action_logger.log_action.await_count == 1
    kwargs = handler.action_logger.log_action.await_args.kwargs
    payload = kwargs["payload"]
    assert payload["action"] == "errors"
    assert payload["has_traceback"] is True
    assert "sample_hash" in payload
    assert "result_preview" not in payload


@pytest.mark.asyncio
async def test_collect_recent_errors_escapes_html_chars(monkeypatch):
    handler = _make_handler()
    monkeypatch.setattr(
        handler,
        "_grep_logs",
        lambda *args, **kwargs: [
            "[errors.log] 2026-02-10 - ERROR - broken <locals> & invalid",
        ],
    )

    text = await handler._collect_recent_errors()

    assert "<b>Последние ошибки</b>" in text
    assert "<code>" in text
    assert "&lt;locals&gt;" in text
    assert "&amp;" in text


@pytest.mark.asyncio
async def test_can_use_system_allows_superadmin_role():
    permissions = SimpleNamespace(
        is_supreme_admin=lambda *_args, **_kwargs: False,
        is_dev_admin=lambda *_args, **_kwargs: False,
        get_effective_role=AsyncMock(return_value="superadmin"),
    )
    handler = SystemMenuHandler(db_manager=SimpleNamespace(), permissions=permissions)

    allowed = await handler._can_use_system(11, "superuser")

    assert allowed is True
