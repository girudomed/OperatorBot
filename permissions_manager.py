import logging
import time  # Для замера времени
from db_setup import create_async_connection
from logger_utils import setup_logging

# Настройка логирования
logger = setup_logging()

class PermissionsManager:
    """
    Класс для управления разрешениями пользователей на основе их ролей.
    """
    
    async def check_permission(self, role_name, permission_name):
        """
        Проверяет, имеет ли роль определенное разрешение.
        Возвращает True, если разрешение есть, и False в противном случае.
        """
        connection = await create_async_connection()
        if not connection:
            logger.error("[КРОТ]: Ошибка подключения к базе данных для проверки разрешений.")
            return False

        try:
            start_time = time.time()
            async with connection.cursor() as cursor:
                query = """
                SELECT 1 FROM PermissionsTelegaBot P
                JOIN RolesTelegaBot R ON P.role_id = R.id
                WHERE R.role_name = %s AND P.permission = %s
                """
                await cursor.execute(query, (role_name, permission_name))
                result = await cursor.fetchone()
                elapsed_time = time.time() - start_time
                
                # Проверка результата запроса
                if result:
                    logger.info(f"[КРОТ]: Роль '{role_name}' имеет разрешение '{permission_name}' (Время выполнения: {elapsed_time:.4f} сек).")
                    return True
                else:
                    logger.warning(f"[КРОТ]: Роль '{role_name}' не имеет разрешения '{permission_name}' (Время выполнения: {elapsed_time:.4f} сек).")
                    return False
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при проверке разрешений для роли '{role_name}': {e}")
            return False
        finally:
            await connection.ensure_closed()

