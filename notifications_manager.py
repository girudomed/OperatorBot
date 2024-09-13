import logging
import time  # Для замера времени
from db_helpers import create_async_connection
from datetime import datetime
from logger_utils import setup_logging

# Настройка логирования
logger = setup_logging()

class NotificationsManager:
    def __init__(self):
        # Убираем синхронное подключение и создаем асинхронное подключение для таблиц
        pass

    async def _create_tables(self):
        """
        Проверяет и создает таблицу `notifications`, если её нет (асинхронно).
        """
        connection = await create_async_connection()
        if not connection:
            logger.error("[КРОТ]: Ошибка подключения к базе данных при создании таблицы.")
            return
        try:
            async with connection.cursor() as cursor:
                start_time = time.time()
                await cursor.execute("""
                    CREATE TABLE IF NOT EXISTS notifications (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        user_id INT NOT NULL,
                        message TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                await connection.commit()
                elapsed_time = time.time() - start_time
                logger.info(f"[КРОТ]: Таблица 'notifications' успешно создана (Время выполнения: {elapsed_time:.4f} сек).")
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при создании таблицы 'notifications': {e}")
            await connection.rollback()
        finally:
            await connection.ensure_closed()

    async def send_notification(self, user_id, message):
        """
        Асинхронная отправка уведомления пользователю с сохранением в базу данных.
        """
        connection = await create_async_connection()
        if not connection:
            logger.error("[КРОТ]: Ошибка подключения к базе данных при отправке уведомления.")
            return
        try:
            async with connection.cursor() as cursor:
                start_time = time.time()
                sql = "INSERT INTO notifications (user_id, message, created_at) VALUES (%s, %s, %s)"
                await cursor.execute(sql, (user_id, message, datetime.utcnow()))
                await connection.commit()
                elapsed_time = time.time() - start_time
                logger.info(f"[КРОТ]: Уведомление отправлено пользователю с ID '{user_id}' (Время выполнения: {elapsed_time:.4f} сек).")
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при отправке уведомления: {e}")
            await connection.rollback()
        finally:
            await connection.ensure_closed()

    async def get_notifications(self, user_id):
        """
        Асинхронное получение всех уведомлений для конкретного пользователя.
        """
        connection = await create_async_connection()
        if not connection:
            logger.error("[КРОТ]: Ошибка подключения к базе данных при получении уведомлений.")
            return []
        try:
            async with connection.cursor() as cursor:
                start_time = time.time()
                sql = "SELECT * FROM notifications WHERE user_id = %s ORDER BY created_at DESC"
                await cursor.execute(sql, (user_id,))
                notifications = await cursor.fetchall()
                elapsed_time = time.time() - start_time
                logger.info(f"[КРОТ]: Уведомления для пользователя с ID '{user_id}' получены (Время выполнения: {elapsed_time:.4f} сек).")
                return notifications
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при получении уведомлений для пользователя с ID '{user_id}': {e}")
            return []
        finally:
            await connection.ensure_closed()

    def log_critical_event(self, event_description):
        """
        Логирование критических событий.
        """
        logger.critical(f"[КРОТ]: Критическое событие: {event_description}")

# Пример использования:
# notifications_manager = NotificationsManager()
# await notifications_manager.send_notification(1, "Your report is ready")
# user_notifications = await notifications_manager.get_notifications(1)
# print(user_notifications)
# notifications_manager.log_critical_event("Database connection lost")
