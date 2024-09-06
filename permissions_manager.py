import logging
from db_setup import create_async_connection
from logger_utils import setup_logging

logger = setup_logging()

class PermissionsManager:
    """
    Класс для управления разрешениями пользователей на основе их ролей.
    """
    
    async def check_permission(self, role_name, permission_name):
        """
        Проверяет, имеет ли роль определенное разрешение.
        """
        connection = await create_async_connection()
        if not connection:
            logger.error("Ошибка подключения к базе данных для проверки разрешений.")
            return False

        try:
            async with connection.cursor() as cursor:
                query = """
                SELECT 1 FROM PermissionsTelegaBot P
                JOIN RolesTelegaBot R ON P.role_id = R.id
                WHERE R.role_name = %s AND P.permission = %s
                """
                await cursor.execute(query, (role_name, permission_name))
                result = await cursor.fetchone()
                if result:
                    logger.info(f"Роль {role_name} имеет разрешение {permission_name}.")
                    return True
                else:
                    logger.warning(f"Роль {role_name} не имеет разрешения {permission_name}.")
                    return False
        except Exception as e:
            logger.error(f"Ошибка при проверке разрешений для роли {role_name}: {e}")
            return False
        finally:
            await connection.ensure_closed()
