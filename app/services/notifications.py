# Файл: app/services/notifications.py

import logging
import time
import asyncio
from typing import Optional
from app.db.connection import acquire_connection
from datetime import datetime
from app.logging_config import get_watchdog_logger
from telegram import Bot
from app.config import TELEGRAM_TOKEN

# Настройка логирования
logger = get_watchdog_logger(__name__)

class NotificationsManager:
    def __init__(self):
        """
        Инициализация менеджера уведомлений.
        Бот инициализируется через Telegram API.
        """
        self.bot = Bot(token=TELEGRAM_TOKEN)  # Инициализация Telegram бота

    async def execute_query(self, query, params=None, fetchone=False, fetchall=False):
        """
        Унифицированная функция для выполнения запросов к базе данных с обработкой ошибок.
        """
        try:
            async with acquire_connection() as connection:
                async with connection.cursor() as cursor:
                    start_time = time.time()
                    await cursor.execute(query, params)
                    elapsed_time = time.time() - start_time
                    logger.info(f"[КРОТ]: Запрос выполнен за {elapsed_time:.4f} сек. (Запрос: {query}, Параметры: {params})")

                    if fetchone:
                        result = await cursor.fetchone()
                        return result
                    if fetchall:
                        result = await cursor.fetchall()
                        return result

                    await connection.commit()
                    return True
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка выполнения запроса: {e}", exc_info=True)
            return None if fetchone or fetchall else False

    async def send_notification(self, user_id, message, chat_id, retries=3):
        """
        Асинхронная отправка уведомления через Telegram с логированием и повторными попытками при ошибке.
        :param user_id: Идентификатор пользователя.
        :param message: Сообщение для отправки.
        :param chat_id: Идентификатор чата для отправки сообщения.
        :param retries: Количество попыток повторной отправки при ошибке.
        """
        try:
            # Сохранение уведомления в базе данных
            query = "INSERT INTO notifications (user_id, message, created_at) VALUES (%s, %s, %s)"
            success = await self.execute_query(query, (user_id, message, datetime.utcnow()))
            if not success:
                logger.error(f"[КРОТ]: Не удалось сохранить уведомление в базу данных для пользователя {user_id}")
                return

            # Попытки отправки сообщения через Telegram API
            for attempt in range(retries):
                try:
                    await self.bot.send_message(chat_id=chat_id, text=message)
                    logger.info(f"[КРОТ]: Уведомление успешно отправлено пользователю с ID {user_id} через Telegram.")
                    break
                except Exception as e:
                    logger.error(f"[КРОТ]: Ошибка отправки уведомления через Telegram (Попытка {attempt+1}/{retries}): {e}", exc_info=True)
                    if attempt + 1 == retries:
                        logger.error(f"[КРОТ]: Не удалось отправить уведомление пользователю с ID {user_id} после {retries} попыток.")
                    await asyncio.sleep(2 ** attempt)  # Экспоненциальное увеличение задержки перед следующей попыткой
        except Exception as e:
            logger.error(f"[КРОТ]: Общая ошибка при отправке уведомления: {e}", exc_info=True)

    async def notify_approval(self, telegram_id: int, approver_name: Optional[str]) -> None:
        """
        Уведомляет пользователя о том, что его аккаунт одобрен.
        """
        if not telegram_id:
            logger.warning("[КРОТ]: Не удалось отправить уведомление об одобрении — нет telegram_id")
            return
        approver = approver_name or "администратор"
        text = (
            f"✅ Ваш доступ одобрен. Администратор {approver} предоставил вам доступ к боту.\n"
            "Можно пользоваться командами прямо сейчас."
        )
        try:
            await self.bot.send_message(chat_id=telegram_id, text=text)
            logger.info("[КРОТ]: Уведомление об одобрении отправлено пользователю %s", telegram_id)
        except Exception as exc:
            logger.error("[КРОТ]: Не удалось отправить уведомление об одобрении: %s", exc, exc_info=True)

    async def get_notifications(self, user_id):
        """
        Асинхронное получение всех уведомлений для конкретного пользователя с логированием.
        :param user_id: Идентификатор пользователя.
        :return: Список уведомлений.
        """
        query = "SELECT * FROM notifications WHERE user_id = %s ORDER BY created_at DESC"
        notifications = await self.execute_query(query, (user_id,), fetchall=True)
        if notifications:
            logger.info(f"[КРОТ]: Уведомления для пользователя с ID '{user_id}' успешно получены.")
        else:
            logger.info(f"[КРОТ]: Уведомления для пользователя с ID '{user_id}' не найдены.")
        return notifications or []

    def log_critical_event(self, event_description):
        """
        Логирование критических событий.
        :param event_description: Описание критического события.
        """
        logger.critical(f"[КРОТ]: Критическое событие: {event_description}")

    async def send_daily_reports(self):
        """
        Асинхронная отправка отчетов операторам в конце рабочего дня через Telegram с логированием.
        """
        try:
            # Получаем все отчеты за текущий день
            # report_date в БД это VARCHAR, не DATE! Формат: 'YYYY-MM-DD'
            today_str = datetime.now().strftime('%Y-%m-%d')
            query = "SELECT user_id, report_text FROM reports WHERE report_date = %s"
            reports = await self.execute_query(query, (today_str,), fetchall=True)

            if not reports:
                logger.info("[КРОТ]: Нет отчетов для отправки за текущий день.")
                return

            tasks = []
            for report in reports:
                user_id = report['user_id']
                report_text = report['report_text']

                # Получаем chat_id из UsersTelegaBot (не users!)
                # user_id в reports связан с UsersTelegaBot.user_id (Telegram ID)
                query_user = "SELECT chat_id FROM UsersTelegaBot WHERE user_id = %s"
                user = await self.execute_query(query_user, (user_id,), fetchone=True)

                if user and user.get('chat_id'):
                    chat_id = user['chat_id']
                    # Параллельная отправка отчетов
                    tasks.append(self.send_notification(user_id, report_text, chat_id))
                    logger.info(f"[КРОТ]: Отчет для пользователя с ID '{user_id}' добавлен в очередь на отправку.")
                else:
                    logger.warning(f"[КРОТ]: Не удалось найти chat_id для пользователя с ID '{user_id}'.")

            # Асинхронная отправка всех отчетов
            if tasks:
                await asyncio.gather(*tasks)
            logger.info(f"[КРОТ]: Все отчеты за день успешно отправлены.")
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при отправке отчетов: {e}", exc_info=True)


# Backward compatibility
class NotificationService(NotificationsManager):
    """
    Legacy alias for NotificationsManager.
    """
    pass
