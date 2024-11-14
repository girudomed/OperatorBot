import logging
import os
from logging.handlers import RotatingFileHandler
import json

def setup_logging(
    log_file="logs/logs.log",
    log_level=logging.INFO,
    max_bytes=5 * 1024 * 1024,
    backup_count=5,
    json_format=False
):
    """
    Настраивает логирование для проекта "КРОТ".
    Логи записываются в файл с ротацией, выводятся в консоль и поддерживают формат JSON.

    :param log_file: Имя файла для сохранения логов.
    :param log_level: Уровень логирования (по умолчанию INFO).
    :param max_bytes: Максимальный размер лог-файла до ротации (по умолчанию 5 MB).
    :param backup_count: Количество резервных копий лог-файлов (по умолчанию 5).
    :param json_format: Если True, логи записываются в формате JSON.
    :return: Конфигурированный объект логгера "КРОТ".
    """
    # Добавляем фишку "КРОТ": проверка создания папки для логов
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir)
            print(f"[КРОТ]: Создана директория для логов: {log_dir}")
        except Exception as e:
            print(f"[КРОТ]: Ошибка при создании директории для логов: {e}")
            raise

    # Настройка ротации логов с фишкой "КРОТ"
    rotating_handler = RotatingFileHandler(
        log_file, maxBytes=max_bytes, backupCount=backup_count
    )

    # Логирование в формате JSON или стандартный формат "КРОТ"
    if json_format:
        formatter = JsonFormatter()
    else:
        formatter = logging.Formatter(
            fmt="%(asctime)s - [КРОТ] - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

    rotating_handler.setFormatter(formatter)

    # Основной логгер с меткой "КРОТ"
    logger = logging.getLogger('KROT')
    logger.setLevel(log_level)

    # Удаление предыдущих хендлеров, если они уже есть
    if logger.hasHandlers():
        logger.handlers.clear()

    # Добавляем хендлеры для файла и консоли
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(rotating_handler)
    logger.addHandler(console_handler)

    # Логируем информацию о запуске
    logger.info(f"[КРОТ]: Логирование настроено. Логи сохраняются в файл: {log_file}")

    return logger

# Класс для форматирования логов в JSON формате с "КРОТ"
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            'time': self.formatTime(record, self.datefmt),
            'level': record.levelname,
            'message': record.getMessage(),
            'logger': record.name
        }
        return json.dumps(log_record)

# Пример использования "КРОТ"
if __name__ == "__main__":
    # Включаем JSON форматирование для "КРОТ"
    logger = setup_logging(log_level=logging.DEBUG, json_format=True)

    # Примеры логов
    logger.info("[КРОТ] Пример информационного сообщения.")
    logger.warning("[КРОТ] Пример предупреждения.")
    logger.error("[КРОТ] Пример ошибки.")
    logger.debug("[КРОТ] Пример отладки.")
