import logging
import os
import json
import queue
from logging.handlers import RotatingFileHandler, QueueHandler, QueueListener
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


class JsonFormatter(logging.Formatter):
    """
    Форматтер для записи логов в формате JSON.
    """
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


def setup_logging(
    log_file="logs.log",
    log_level=logging.INFO,
    max_log_lines=200000,
    average_line_length=100,
    backup_count=0,
    json_format=False,
    use_queue=True,
    telegram_bot_token=None,
    telegram_chat_id=None
):
    """
    Настраивает логирование с учётом:
      • ротации лог-файла на основе количества строк (max_log_lines) и усреднённой длины строки (average_line_length);
      • очереди (QueueHandler/QueueListener) при многопоточном использовании (use_queue);
      • отправки ошибок в Telegram (при наличии telegram_bot_token и telegram_chat_id);
      • возможности JSON-форматирования (json_format).

    Параметры:
      log_file          : Имя файла, куда пишутся логи.
      log_level         : Уровень логирования (по умолчанию INFO).
      max_log_lines     : Приблизительное количество строк, после которого произойдёт ротация.
      average_line_length: Средняя длина одной строки лога, используется для вычисления maxBytes.
      backup_count      : Количество резервных копий лог-файлов.
      json_format       : Если True, логи записываются в формате JSON.
      use_queue         : Если True, включается QueueHandler/QueueListener для многопоточного логирования.
      telegram_bot_token: Токен Telegram-бота для отправки ошибок в чат.
      telegram_chat_id  : ID чата в Telegram, куда отправлять ошибки.

    Возвращает:
      Объект корневого логгера (root logger) с настроенными хендлерами.
    """

    # Если указан токен, проверяем, что он строка
    if telegram_bot_token is not None and not isinstance(telegram_bot_token, str):
        raise TypeError("Значение токена должно быть строкой")

    # Создаём директорию для логов, если её нет
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir)
        except Exception as e:
            print(f"[КРОТ]: Ошибка при создании директории для логов: {e}")
            raise

    # Вычисляем размер файла в байтах для ротации
    max_bytes = max_log_lines * average_line_length

    # Форматтер
    if json_format:
        formatter = JsonFormatter()
    else:
        formatter = logging.Formatter(
            fmt="%(asctime)s - [КРОТ] - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

    # Создаём ротационный файловый хендлер
    rotating_handler = RotatingFileHandler(
        filename=log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8"
    )
    rotating_handler.setFormatter(formatter)
    rotating_handler.setLevel(log_level)

    # Настраиваем корневой логгер (root logger)
    logger = logging.getLogger()  # используем корневой логгер
    logger.setLevel(log_level)

    # Очищаем предыдущие хендлеры, чтобы избежать дублирования
    if logger.hasHandlers():
        logger.handlers.clear()

    # Хендлер для консоли
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(log_level)
    logger.addHandler(console_handler)
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # Если нужно логировать в Telegram
    if telegram_bot_token and telegram_chat_id:
        telegram_handler = TelegramErrorHandler(
            bot_token=telegram_bot_token,
            chat_id=telegram_chat_id,
            level=logging.ERROR  # Лишь ошибки отправляем в Telegram
        )
        telegram_handler.setFormatter(formatter)
        logger.addHandler(telegram_handler)

    # Если включён режим использования очереди (для многопоточности)
    if use_queue:
        log_queue = queue.Queue(-1)
        queue_handler = QueueHandler(log_queue)
        # QueueListener будет фактически писать в rotating_handler
        listener = QueueListener(log_queue, rotating_handler)
        listener.start()
        logger.addHandler(queue_handler)
    else:
        # Иначе напрямую используем rotating_handler
        logger.addHandler(rotating_handler)

    logger.info(
        "[КРОТ]: Логирование настроено. "
        f"Файл: {log_file}, maxBytes≈{max_bytes}, backupCount={backup_count}"
    )

    return logger


# Тестовый пример
if __name__ == "__main__":
    logger = setup_logging(
        log_file="logs.log",
        log_level=logging.DEBUG,
        max_log_lines=200000,
        average_line_length=100,
        backup_count=3,
        json_format=False,
        use_queue=True,
        telegram_bot_token="your_bot_token",
        telegram_chat_id="your_chat_id"
    )
    logger.info("Тестовая запись в лог.")
    logger.error("Тестовая ошибка для проверки Telegram-уведомления.")