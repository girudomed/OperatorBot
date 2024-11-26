import logging
import os
from logging.handlers import RotatingFileHandler, QueueHandler, QueueListener
import json
import queue
import httpx

class TelegramErrorHandler(logging.Handler):
    """
    Логгинг ошибок в Telegram.
    """
    def __init__(self, bot_token, chat_id, level=logging.ERROR):
        super().__init__(level)
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    def emit(self, record):
        try:
            log_entry = self.format(record)
            message = f"🚨 Ошибка в боте:\n\n{log_entry}"
            payload = {
                "chat_id": self.chat_id,
                "text": message,
                "parse_mode": "HTML"
            }
            with httpx.Client() as client:
                response = client.post(self.api_url, json=payload)
                if response.status_code != 200:
                    print(f"[КРОТ]: Ошибка отправки в Telegram: {response.text}")
        except Exception as e:
            print(f"[КРОТ]: Ошибка в TelegramErrorHandler: {e}")


def setup_logging(
    log_file="logs.log",
    log_level=logging.INFO,
    max_bytes=5 * 1024 * 1024,
    backup_count=5,
    json_format=False,
    use_queue=False,
    telegram_bot_token=None,
    telegram_chat_id=None
):
    """
    Настраивает логирование для проекта "КРОТ".
    Логи записываются в файл с ротацией, выводятся в консоль, поддерживают формат JSON и
    обеспечивают поддержку многопоточного логирования через QueueHandler.

    :param log_file: Имя файла для сохранения логов.
    :param log_level: Уровень логирования (по умолчанию INFO).
    :param max_bytes: Максимальный размер лог-файла до ротации (по умолчанию 5 MB).
    :param backup_count: Количество резервных копий лог-файлов (по умолчанию 5).
    :param json_format: Если True, логи записываются в формате JSON.
    :param use_queue: Если True, используется QueueHandler для многопоточного логирования.
    :param telegram_bot_token: Токен Telegram бота для отправки ошибок.
    :param telegram_chat_id: ID чата для отправки ошибок.
    :return: Конфигурированный объект логгера "КРОТ".
    """
    # Проверка существования директории для логов
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir)
            print(f"[КРОТ]: Создана директория для логов: {log_dir}")
        except Exception as e:
            print(f"[КРОТ]: Ошибка при создании директории для логов: {e}")
            raise

    # Установка базового форматтера
    if json_format:
        formatter = JsonFormatter()
    else:
        formatter = logging.Formatter(
            fmt="%(asctime)s - [КРОТ] - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

    # Настройка ротации логов
    rotating_handler = RotatingFileHandler(
        log_file, maxBytes=max_bytes, backupCount=backup_count
    )
    rotating_handler.setFormatter(formatter)

    # Очередь для QueueHandler (если включено)
    log_queue = queue.Queue(-1) if use_queue else None

    # Конфигурация основного логгера
    logger = logging.getLogger('KROT')
    logger.setLevel(log_level)

    # Удаление предыдущих хендлеров
    if logger.hasHandlers():
        logger.handlers.clear()

    # Добавление обработчиков
    if use_queue:
        queue_handler = QueueHandler(log_queue)
        logger.addHandler(queue_handler)

        # Запуск QueueListener
        listener = QueueListener(log_queue, rotating_handler)
        listener.start()
    else:
        logger.addHandler(rotating_handler)

    # Настройка консольного логирования
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Добавление TelegramErrorHandler (если токен и чат указаны)
    if telegram_bot_token and telegram_chat_id:
        telegram_handler = TelegramErrorHandler(
            bot_token=telegram_bot_token,
            chat_id=telegram_chat_id
        )
        telegram_handler.setFormatter(formatter)
        logger.addHandler(telegram_handler)

    # Логирование успешной настройки
    logger.info(f"[КРОТ]: Логирование настроено. Логи сохраняются в файл: {log_file}")

    return logger


# Класс для форматирования логов в JSON формате
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            'time': self.formatTime(record, self.datefmt),
            'level': record.levelname,
            'message': record.getMessage(),
            'logger': record.name,
        }
        if record.exc_info:
            log_record['exception'] = self.formatException(record.exc_info)
        return json.dumps(log_record)


# Пример использования "КРОТ"
if __name__ == "__main__":
    # Включаем JSON форматирование, многопоточность и Telegram уведомления для "КРОТ"
    TELEGRAM_BOT_TOKEN = "ваш_telegram_bot_token"
    TELEGRAM_CHAT_ID = "ваш_telegram_chat_id"

    logger = setup_logging(
        log_level=logging.DEBUG,
        json_format=True,
        use_queue=True,
        telegram_bot_token=TELEGRAM_BOT_TOKEN,
        telegram_chat_id=TELEGRAM_CHAT_ID
    )

    # Примеры логов
    try:
        logger.info("[КРОТ] Пример информационного сообщения.")
        logger.warning("[КРОТ] Пример предупреждения.")
        logger.error("[КРОТ] Пример ошибки.")
        raise ValueError("Тестовое исключение для логов")
    except Exception as e:
        logger.exception("[КРОТ] Исключение обработано.")