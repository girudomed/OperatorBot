from __future__ import annotations

from typing import Awaitable, Callable, Dict, Optional

from telegram import Update
from telegram.ext import Application, ContextTypes

AdminCallback = Callable[[Update, ContextTypes.DEFAULT_TYPE], Awaitable[None]]


def register_admin_callback_handler(
    application: Application,
    action: str,
    handler: AdminCallback,
) -> None:
    registry: Dict[str, AdminCallback] = application.bot_data.setdefault("admin_callback_handlers", {})
    registry[action] = handler


def get_admin_callback_handler(
    context: ContextTypes.DEFAULT_TYPE,
    action: str,
) -> Optional[AdminCallback]:
    registry: Dict[str, AdminCallback] = context.application.bot_data.get("admin_callback_handlers", {})
    return registry.get(action)
