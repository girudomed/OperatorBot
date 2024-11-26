import pymysql
from db_module import get_db_connection
from logger_utils import setup_logging
import time  # Для замера времени

# Настройка логирования
logger = setup_logging()

class RoleManager:
    def __init__(self):
        self.connection = get_db_connection()

    def create_role(self, role_name, permissions):
        """
        Создание новой роли и назначение ей разрешений.
        Перед созданием проверяем валидность имени роли и списка разрешений.
        """
        if not self._validate_role_data(role_name, permissions):
            return
        
        try:
            start_time = time.time()
            with self.connection.cursor() as cursor:
                sql = "INSERT INTO roles (role_name) VALUES (%s)"
                cursor.execute(sql, (role_name,))
                role_id = cursor.lastrowid
                self._assign_permissions(role_id, permissions)
                self.connection.commit()
                elapsed_time = time.time() - start_time
                logger.info(f"[КРОТ]: Роль '{role_name}' успешно создана (Время выполнения: {elapsed_time:.4f} сек).")
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при создании роли '{role_name}': {e}")
            self.connection.rollback()

    def delete_role(self, role_id):
        """
        Удаление роли и связанных с ней разрешений.
        """
        if not self._validate_role_id(role_id):
            return

        try:
            start_time = time.time()
            with self.connection.cursor() as cursor:
                sql = "DELETE FROM roles WHERE role_id = %s"
                cursor.execute(sql, (role_id,))
                sql_perm = "DELETE FROM permissions WHERE role_id = %s"
                cursor.execute(sql_perm, (role_id,))
                self.connection.commit()
                elapsed_time = time.time() - start_time
                logger.info(f"[КРОТ]: Роль с ID '{role_id}' успешно удалена (Время выполнения: {elapsed_time:.4f} сек).")
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при удалении роли с ID '{role_id}': {e}")
            self.connection.rollback()

    def update_role(self, role_id, new_name=None, new_permissions=None):
        """
        Обновление роли и её разрешений.
        """
        if not self._validate_role_id(role_id) or not self._validate_role_data(new_name, new_permissions, update=True):
            return

        try:
            start_time = time.time()
            with self.connection.cursor() as cursor:
                if new_name:
                    sql = "UPDATE roles SET role_name = %s WHERE role_id = %s"
                    cursor.execute(sql, (new_name, role_id))
                if new_permissions:
                    self._assign_permissions(role_id, new_permissions, update=True)
                self.connection.commit()
                elapsed_time = time.time() - start_time
                logger.info(f"[КРОТ]: Роль с ID '{role_id}' успешно обновлена (Время выполнения: {elapsed_time:.4f} сек).")
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при обновлении роли с ID '{role_id}': {e}")
            self.connection.rollback()

    def _assign_permissions(self, role_id, permissions, update=False):
        """
        Назначение разрешений для роли. Если update=True, то сначала удаляются старые разрешения.
        """
        try:
            with self.connection.cursor() as cursor:
                if update:
                    sql_del = "DELETE FROM permissions WHERE role_id = %s"
                    cursor.execute(sql_del, (role_id,))
                for permission in permissions:
                    sql_perm = "INSERT INTO permissions (role_id, permission_name) VALUES (%s, %s)"
                    cursor.execute(sql_perm, (role_id, permission))
            logger.info(f"[КРОТ]: Разрешения для роли с ID '{role_id}' успешно назначены.")
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при назначении разрешений для роли с ID '{role_id}': {e}")

    def get_role(self, role_id):
        """
        Получение информации о роли по её ID.
        """
        if not self._validate_role_id(role_id):
            return None
        
        try:
            start_time = time.time()
            with self.connection.cursor() as cursor:
                sql = "SELECT * FROM roles WHERE role_id = %s"
                cursor.execute(sql, (role_id,))
                role = cursor.fetchone()
                elapsed_time = time.time() - start_time
                logger.info(f"[КРОТ]: Роль с ID '{role_id}' успешно получена (Время выполнения: {elapsed_time:.4f} сек).")
                return role
        except Exception as e:
            logger.error(f"[КРОТ]: Ошибка при получении роли с ID '{role_id}': {e}")
            return None

    def _validate_role_data(self, role_name, permissions, update=False):
        """
        Валидация данных роли.
        Проверка на то, что имя роли не пустое, а разрешения представлены списком.
        """
        if not role_name and not update:
            logger.error(f"[КРОТ]: Имя роли не может быть пустым.")
            return False
        if permissions is not None and not isinstance(permissions, list):
            logger.error(f"[КРОТ]: Разрешения должны быть представлены в виде списка.")
            return False
        return True

    def _validate_role_id(self, role_id):
        """
        Проверка на валидность role_id.
        """
        if not role_id or not isinstance(role_id, int):
            logger.error(f"[КРОТ]: Недопустимый ID роли: {role_id}")
            return False
        return True

# Пример использования
if __name__ == "__main__":
    role_manager = RoleManager()
    # Пример создания роли
    role_manager.create_role('Manager', ['view_reports', 'manage_users'])
    # Пример удаления роли
    role_manager.delete_role(1)
    # Пример обновления роли
    role_manager.update_role(1, 'Super Manager', ['view_reports', 'edit_reports'])
    # Пример получения роли
    role = role_manager.get_role(1)
    print(f"Полученная роль: {role}")
