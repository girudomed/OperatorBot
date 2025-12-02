from celery import Celery
from telegram import Bot
import logging

from app.config import TELEGRAM_TOKEN

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Настройка Celery с Redis
celery_app = Celery('tasks', broker='redis://localhost:6379/0')

# Инициализация Telegram Bot
if not isinstance(TELEGRAM_TOKEN, str):
    raise RuntimeError("TELEGRAM_BOT_TOKEN отсутствует в конфигурации.")
bot = Bot(TELEGRAM_TOKEN)

@celery_app.task(bind=True, max_retries=3)
def send_message_task(self, chat_id, text):
    """
    Задача для отправки сообщения через Celery. 
    Отправляет сообщение в синхронном режиме, так как Celery плохо работает с asyncio.
    """
    try:
        # Отправка сообщения в синхронном режиме
        bot.send_message(chat_id=chat_id, text=text)
        logger.info(f"Сообщение успешно отправлено в чат {chat_id}")
    except Exception as exc:
        logger.error(f"Ошибка при отправке сообщения: {exc}")
        raise self.retry(exc=exc, countdown=5)  # Повтор через 5 секунд при ошибке
