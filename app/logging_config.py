"""
Модуль настройки логирования приложения.

Использует watch_dog для централизованного логирования с маскировкой секретов.
"""

import logging
from watch_dog import setup_watchdog, get_watchdog_logger


def setup_app_logging() -> logging.Logger:
    """
    Настраивает централизованное логирование приложения через watch_dog.
    
    Returns:
        logging.Logger: Настроенный корневой логгер
    """
    # Инициализируем watch_dog (настраивает root logger)
    setup_watchdog()
    
    # Возвращаем логгер для основного приложения
    return get_watchdog_logger("app")


# Глобальный логгер для использования в модуле
logger = setup_app_logging()
