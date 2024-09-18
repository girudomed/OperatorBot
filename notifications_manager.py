import logging
import time
import asyncio
from db_module import create_async_connection
from datetime import datetime
from logger_utils import setup_logging
from telegram import Bot
import config

# Настройка логирования
logger = setup_logging()

class NotificationsManager:
    def __init__(self):
        """
        Инициализация менеджера уведомлений.
        Можно передать параметры для Telegram Bot для отправки сообщений через Telegram.
        """
        self.bot = Bot(token=config.telegram_token)  # Инициализация Telegram бота

    async def execute_query(self, query, params=None, fetchone=False, fetchall=False):
        """
        Унифицированная функция для выполнения запросов к базе данных.
        """
        connection = await create_async_connection()
        if not connection:
            logger.error("[КРОТ]: Ошибка подключения к базе данных.")
            return None if fetchone or fetchall else False

        try:
            async with connection.cursor() as cursor:
                start_time = time.time()
                await cursor.execute(query, params)
                elapsed_time = time.time() - start_time
                logger.info(f"[КРОТ]: Запрос выполнен за {elapsed_time:.4f} сек.")

                if fetchone:
                    result = await cursor.fetchone()
                    return result
                if fetchall:
                    result = await cursor.fetchall()
                    return result

                await connection.commit()
                return True
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка выполнения запроса: {e}")
            await connection.rollback()
            return None if fetchone or fetchall else False
        finally:
            await connection.ensure_closed()

    async def send_notification(self, user_id, message, chat_id):
        """
        Асинхронная отправка уведомления пользователю через Telegram с сохранением в базу данных.
        """
        try:
            # Сохранение уведомления в базе данных
            query = "INSERT INTO notifications (user_id, message, created_at) VALUES (%s, %s, %s)"
            success = await self.execute_query(query, (user_id, message, datetime.utcnow()))
            if not success:
                return

            # Отправляем сообщение через Telegram API
            await self.bot.send_message(chat_id=chat_id, text=message)
            logger.info(f"[КРОТ]: Уведомление отправлено пользователю с ID '{user_id}' через Telegram.")
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при отправке уведомления: {e}")

    async def get_notifications(self, user_id):
        """
        Асинхронное получение всех уведомлений для конкретного пользователя.
        """
        query = "SELECT * FROM notifications WHERE user_id = %s ORDER BY created_at DESC"
        notifications = await self.execute_query(query, (user_id,), fetchall=True)
        if notifications is not None:
            logger.info(f"[КРОТ]: Уведомления для пользователя с ID '{user_id}' получены.")
        return notifications or []

    def log_critical_event(self, event_description):
        """
        Логирование критических событий.
        """
        logger.critical(f"[КРОТ]: Критическое событие: {event_description}")

    async def send_daily_reports(self):
        """
        Отправка отчетов операторам в конце рабочего дня.
        Использует данные из таблицы `reports` и отправляет отчеты через Telegram.
        """
        try:
            # Получаем все отчеты за текущий день
            query = "SELECT user_id, report_text FROM reports WHERE report_date = CURRENT_DATE"
            reports = await self.execute_query(query, fetchall=True)

            if not reports:
                return

            tasks = []
            for report in reports:
                user_id = report['user_id']
                report_text = report['report_text']

                # Получаем chat_id из таблицы UsersTelegaBot по user_id
                query_user = "SELECT chat_id FROM UsersTelegaBot WHERE user_id = %s"
                user = await self.execute_query(query_user, (user_id,), fetchone=True)

                if user:
                    chat_id = user['chat_id']
                    # Параллельная отправка отчетов
                    tasks.append(self.bot.send_message(chat_id=chat_id, text=report_text))
                    logger.info(f"[КРОТ]: Отчет отправлен пользователю с ID '{user_id}' через Telegram.")
                else:
                    logger.warning(f"[КРОТ]: Не удалось найти chat_id для пользователя с ID '{user_id}'.")

            # Асинхронная отправка всех отчетов
            if tasks:
                await asyncio.gather(*tasks)
            logger.info(f"[КРОТ]: Все отчеты успешно отправлены.")
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при отправке отчетов: {e}")
