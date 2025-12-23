"""Экранные рендеры админки."""

from dataclasses import dataclass
from typing import List

from telegram import InlineKeyboardButton


@dataclass
class Screen:
    text: str
    keyboard: List[List[InlineKeyboardButton]]
    parse_mode: str = "HTML"

    @property
    def markup(self) -> "InlineKeyboardMarkup":
        from telegram import InlineKeyboardMarkup
        return InlineKeyboardMarkup(self.keyboard)

