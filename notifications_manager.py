import pymysql
from db_utils import get_db_connection
from datetime import datetime
import logging

# Настройка логирования
logging.basicConfig(filename='logs.log', level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(message)s')

from logger_utils import setup_logging

logger = setup_logging()

def some_function():
    logger.info("Функция some_function начала работу.")
    # Логика функции
    try:
        # Некоторый код
        logger.info("Успешное выполнение.")
    except Exception as e:
        logger.error(f"Произошла ошибка: {e}")


class NotificationsManager:
    def __init__(self):
        self.connection = get_db_connection()
        self._create_tables()  # Автоматически создаем таблицы при инициализации

    def _create_tables(self):
        """
        Проверяет и создает таблицу `notifications`, если её нет.
        """
        try:
            with self.connection.cursor() as cursor:
                # Создание таблицы уведомлений
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS notifications (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        user_id INT NOT NULL,
                        message TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                self.connection.commit()
                print("Table 'notifications' created or verified successfully.")
        except Exception as e:
            logging.error(f"Error creating table 'notifications': {e}")
            self.connection.rollback()

    def send_notification(self, user_id, message):
        """
        Отправка уведомления пользователю с сохранением в базу данных.
        """
        try:
            with self.connection.cursor() as cursor:
                sql = "INSERT INTO notifications (user_id, message, created_at) VALUES (%s, %s, %s)"
                cursor.execute(sql, (user_id, message, datetime.utcnow()))
                self.connection.commit()
                print(f"Notification sent to user ID '{user_id}' successfully.")
        except Exception as e:
            logging.error(f"Error sending notification: {e}")
            self.connection.rollback()

    def get_notifications(self, user_id):
        """
        Получение всех уведомлений для конкретного пользователя.
        """
        try:
            with self.connection.cursor() as cursor:
                sql = "SELECT * FROM notifications WHERE user_id = %s ORDER BY created_at DESC"
                cursor.execute(sql, (user_id,))
                notifications = cursor.fetchall()
                return notifications
        except Exception as e:
            logging.error(f"Error fetching notifications: {e}")
            return []

    def log_critical_event(self, event_description):
        """
        Логирование критических событий в файл logs.log.
        """
        logging.error(f"Critical event: {event_description}")

# Example usage:
# notifications_manager = NotificationsManager()
# notifications_manager.send_notification(1, "Your report is ready")
# user_notifications = notifications_manager.get_notifications(1)
# print(user_notifications)
# notifications_manager.log_critical_event("Database connection lost")
