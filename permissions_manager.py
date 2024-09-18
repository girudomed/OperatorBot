import logging
import time  # Для замера времени
import asyncio
from logger_utils import setup_logging

# Настройка логирования
logger = setup_logging()

class PermissionsManager:
    """
    Класс для управления разрешениями пользователей на основе их ролей.
    """

    def __init__(self, db_manager):
        self.db_manager = db_manager

    async def check_permission(self, role_name, required_role):
        """
        Проверяет, имеет ли роль необходимый уровень доступа (required_role).
        Возвращает True, если роль имеет достаточно прав, и False в противном случае.
        """
        try:
            start_time = time.time()
            query = "SELECT role_name FROM RolesTelegaBot WHERE role_name = %s"
            result = await self.db_manager.execute_query(query, (role_name,), fetchone=True)
            elapsed_time = time.time() - start_time

            # Если роль найдена, проверим ее уровень
            if result:
                logger.info(f"[КРОТ]: Роль '{role_name}' найдена (Время выполнения: {elapsed_time:.4f} сек).")
                # Проверка на соответствие требуемой роли
                return result['role_name'] == required_role
            else:
                logger.warning(f"[КРОТ]: Роль '{role_name}' не найдена (Время выполнения: {elapsed_time:.4f} сек).")
                return False
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при проверке роли '{role_name}': {e}")
            return False

    async def add_role(self, role_name, role_password):
        """
        Добавляет новую роль в таблицу RolesTelegaBot.
        """
        try:
            start_time = time.time()
            query_insert = """
            INSERT INTO RolesTelegaBot (role_name, role_password)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE role_password = VALUES(role_password)
            """
            await self.db_manager.execute_query(query_insert, (role_name, role_password))
            elapsed_time = time.time() - start_time
            logger.info(f"[КРОТ]: Роль '{role_name}' добавлена или обновлена (Время выполнения: {elapsed_time:.4f} сек).")
            return True
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при добавлении роли '{role_name}': {e}")
            return False

    async def remove_role(self, role_name):
        """
        Удаляет роль из таблицы RolesTelegaBot.
        """
        try:
            start_time = time.time()
            query_delete = "DELETE FROM RolesTelegaBot WHERE role_name = %s"
            await self.db_manager.execute_query(query_delete, (role_name,))
            elapsed_time = time.time() - start_time
            logger.info(f"[КРОТ]: Роль '{role_name}' удалена (Время выполнения: {elapsed_time:.4f} сек).")
            return True
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при удалении роли '{role_name}': {e}")
            return False
