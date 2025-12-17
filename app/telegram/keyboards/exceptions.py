"""Специальные исключения для построения клавиатур."""

from telegram import ReplyKeyboardMarkup


class KeyboardPermissionsError(RuntimeError):
    """Ошибка получения прав для клавиатуры.

    Содержит fallback-клавиатуру, которую можно показать пользователю.
    """

    def __init__(self, fallback_keyboard: ReplyKeyboardMarkup, message: str = "Временная ошибка доступа"):
        super().__init__(message)
        self.fallback_keyboard = fallback_keyboard
