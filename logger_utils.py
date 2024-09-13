import logging
import os
from logging.handlers import RotatingFileHandler

def setup_logging(log_file="logs/logs.log", log_level=logging.INFO, max_bytes=5 * 1024 * 1024, backup_count=5):
    """
    Настраивает логирование для проекта.
    Логи записываются как в файл с ротацией, так и выводятся в консоль.

    :param log_file: Имя файла для сохранения логов.
    :param log_level: Уровень логирования (по умолчанию INFO).
    :param max_bytes: Максимальный размер лог-файла до ротации (по умолчанию 5 MB).
    :param backup_count: Количество резервных копий лог-файлов (по умолчанию 5).
    :return: Конфигурированный объект логгера.
    """
    # Проверяем, существует ли папка для логов, и создаем её при необходимости
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir)
            print(f"Создана директория для логов: {log_dir}")
        except Exception as e:
            print(f"Ошибка при создании директории для логов: {e}")
            raise

    # Настройка ротации логов
    rotating_handler = RotatingFileHandler(
        log_file, maxBytes=max_bytes, backupCount=backup_count
    )

    # Настройка форматирования логов
    formatter = logging.Formatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    rotating_handler.setFormatter(formatter)

    # Настройка основного логгера
    logger = logging.getLogger()
    logger.setLevel(log_level)

    # Удаление предыдущих хендлеров, если они уже есть
    if logger.hasHandlers():
        logger.handlers.clear()

    # Добавляем хэндлеры для файла и консоли
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    logger.addHandler(rotating_handler)
    logger.addHandler(console_handler)

    # Логируем информацию о запуске
    logger.info(f"Логирование настроено. Логи сохраняются в файл: {log_file}")

    return logger

# Пример использования
if __name__ == "__main__":
    # Инициализация логгера с уровнем DEBUG
    logger = setup_logging(log_level=logging.DEBUG)

    # Пример логирования
    logger.info("Пример информационного сообщения.")
    logger.warning("Пример предупреждения.")
    logger.error("Пример ошибки.")
    logger.debug("Пример отладки.")
