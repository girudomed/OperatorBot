import logging
import time  # Для замера времени
import asyncio
from logger_utils import setup_logging

# Настройка логирования
logger = setup_logging()

# Иерархия ролей с уровнями
ROLE_HIERARCHY = {
    "Admin": 3,
    "Supervisor": 2,
    "Operator": 1,
    "Guest": 0
}

class PermissionsManager:
    """
    Класс для управления разрешениями пользователей на основе их ролей и иерархии ролей.
    """

    def __init__(self, db_manager):
        self.db_manager = db_manager

    async def get_user_role(self, user_id):
        """
        Получает имя роли пользователя по его user_id.
        """
        try:
            start_time = time.time()

            # Получаем role_id из таблицы UsersTelegaBot
            query = "SELECT role_id FROM UsersTelegaBot WHERE user_id = %s"
            async with self.db_manager.acquire() as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(query, (user_id,))
                    result = await cursor.fetchone()

            if result and result.get('role_id'):
                role_id = result['role_id']
                role_name = await self.get_role_name(role_id)
                elapsed_time = time.time() - start_time
                logger.info(f"[КРОТ]: Роль пользователя с ID '{user_id}' успешно получена: {role_name} (Время выполнения: {elapsed_time:.4f} сек).")
                return role_name
            else:
                logger.warning(f"[КРОТ]: Роль для пользователя с ID '{user_id}' не найдена.")
                return None

        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при получении роли для пользователя с ID '{user_id}': {e}")
            return None

    async def get_role_name(self, role_id):
        """
        Получает имя роли по ее role_id.
        """
        try:
            query = "SELECT role_name FROM RolesTelegaBot WHERE id = %s"
            async with self.db_manager.acquire() as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(query, (role_id,))
                    result = await cursor.fetchone()
            if result and result.get('role_name'):
                return result['role_name']
            else:
                logger.warning(f"[КРОТ]: Роль с ID '{role_id}' не найдена.")
                return None
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при получении имени роли с ID '{role_id}': {e}")
            return None

    async def check_permission(self, role_name, required_permission):
        """
        Проверяет, имеет ли роль пользователя необходимое разрешение.
        """
        try:
            # Получаем role_id по role_name
            query_role_id = "SELECT id FROM RolesTelegaBot WHERE role_name = %s"
            async with self.db_manager.acquire() as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(query_role_id, (role_name,))
                    role_result = await cursor.fetchone()

            if not role_result or not role_result.get('id'):
                logger.warning(f"[КРОТ]: Роль '{role_name}' не найдена.")
                return False

            role_id = role_result['id']

            # Получаем список разрешений для role_id
            query_permissions = "SELECT permission FROM PermissionsTelegaBot WHERE role_id = %s"
            async with self.db_manager.acquire() as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(query_permissions, (role_id,))
                    permissions = await cursor.fetchall()

            user_permissions = [perm['permission'] for perm in permissions]

            logger.info(f"[КРОТ]: Разрешения для роли '{role_name}' получены.")

            return required_permission in user_permissions or 'full_access' in user_permissions

        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при проверке разрешений для роли '{role_name}': {e}")
            return False

    async def add_role(self, role_name, role_password):
        """
        Добавляет новую роль в таблицу RolesTelegaBot.
        Если роль уже существует, обновляет ее.
        """
        try:
            start_time = time.time()

            query_insert = """
            INSERT INTO RolesTelegaBot (role_name, role_password)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE role_password = VALUES(role_password)
            """
            async with self.db_manager.acquire() as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(query_insert, (role_name, role_password))
                    await connection.commit()

            elapsed_time = time.time() - start_time
            logger.info(f"[КРОТ]: Роль '{role_name}' добавлена или обновлена (Время выполнения: {elapsed_time:.4f} сек).")
            return True

        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при добавлении или обновлении роли '{role_name}': {e}")
            return False

    async def remove_role(self, role_name):
        """
        Удаляет роль из таблицы RolesTelegaBot.
        """
        try:
            start_time = time.time()

            query_delete = "DELETE FROM RolesTelegaBot WHERE role_name = %s"
            async with self.db_manager.acquire() as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(query_delete, (role_name,))
                    await connection.commit()

            elapsed_time = time.time() - start_time
            logger.info(f"[КРОТ]: Роль '{role_name}' удалена (Время выполнения: {elapsed_time:.4f} сек).")
            return True

        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при удалении роли '{role_name}': {e}")
            return False

    async def update_user_role(self, user_id, new_role_name):
        """
        Обновляет роль пользователя в базе данных.
        """
        try:
            start_time = time.time()

            # Получаем role_id по new_role_name
            query_role_id = "SELECT id FROM RolesTelegaBot WHERE role_name = %s"
            async with self.db_manager.acquire() as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(query_role_id, (new_role_name,))
                    role_result = await cursor.fetchone()

            if not role_result or not role_result.get('id'):
                logger.warning(f"[КРОТ]: Роль '{new_role_name}' не найдена при обновлении пользователя с ID '{user_id}'.")
                return False

            new_role_id = role_result['id']

            # Обновляем role_id пользователя
            query_update = "UPDATE UsersTelegaBot SET role_id = %s WHERE user_id = %s"
            async with self.db_manager.acquire() as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(query_update, (new_role_id, user_id))
                    await connection.commit()

            elapsed_time = time.time() - start_time
            logger.info(f"[КРОТ]: Роль пользователя с ID '{user_id}' обновлена на '{new_role_name}' (Время выполнения: {elapsed_time:.4f} сек).")
            return True

        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при обновлении роли для пользователя с ID '{user_id}': {e}")
            return False

    async def list_all_roles(self):
        """
        Возвращает список всех ролей из базы данных.
        """
        try:
            start_time = time.time()

            query = "SELECT role_name FROM RolesTelegaBot"
            async with self.db_manager.acquire() as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(query)
                    roles = await cursor.fetchall()

            elapsed_time = time.time() - start_time
            if roles:
                logger.info(f"[КРОТ]: Получен список всех ролей (Время выполнения: {elapsed_time:.4f} сек).")
                return [role['role_name'] for role in roles]
            else:
                logger.warning("[КРОТ]: В базе данных не найдено ни одной роли.")
                return []

        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при получении списка ролей: {e}")
            return []

    async def list_users_by_role(self, role_name):
        """
        Возвращает список всех пользователей с заданной ролью.
        """
        try:
            start_time = time.time()

            # Получаем role_id по role_name
            query_role_id = "SELECT id FROM RolesTelegaBot WHERE role_name = %s"
            async with self.db_manager.acquire() as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(query_role_id, (role_name,))
                    role_result = await cursor.fetchone()

            if not role_result or not role_result.get('id'):
                logger.warning(f"[КРОТ]: Роль '{role_name}' не найдена.")
                return []

            role_id = role_result['id']

            # Получаем пользователей с заданным role_id
            query = "SELECT user_id, full_name FROM UsersTelegaBot WHERE role_id = %s"
            async with self.db_manager.acquire() as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(query, (role_id,))
                    users = await cursor.fetchall()

            elapsed_time = time.time() - start_time
            if users:
                logger.info(f"[КРОТ]: Получен список пользователей с ролью '{role_name}' (Время выполнения: {elapsed_time:.4f} сек).")
                return users
            else:
                logger.warning(f"[КРОТ]: Пользователи с ролью '{role_name}' не найдены.")
                return []

        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при получении списка пользователей для роли '{role_name}': {e}")
            return []