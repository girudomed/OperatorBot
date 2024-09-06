
import pymysql
from db_utils import get_db_connection

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


class RoleManager:
    def __init__(self):
        self.connection = get_db_connection()

    def create_role(self, role_name, permissions):
        try:
            with self.connection.cursor() as cursor:
                sql = "INSERT INTO roles (role_name) VALUES (%s)"
                cursor.execute(sql, (role_name,))
                role_id = cursor.lastrowid
                self._assign_permissions(role_id, permissions)
                self.connection.commit()
                print(f"Role '{role_name}' created successfully.")
        except Exception as e:
            print(f"Error creating role: {e}")
            self.connection.rollback()

    def delete_role(self, role_id):
        try:
            with self.connection.cursor() as cursor:
                sql = "DELETE FROM roles WHERE role_id = %s"
                cursor.execute(sql, (role_id,))
                sql_perm = "DELETE FROM permissions WHERE role_id = %s"
                cursor.execute(sql_perm, (role_id,))
                self.connection.commit()
                print(f"Role ID '{role_id}' deleted successfully.")
        except Exception as e:
            print(f"Error deleting role: {e}")
            self.connection.rollback()

    def update_role(self, role_id, new_name=None, new_permissions=None):
        try:
            with self.connection.cursor() as cursor:
                if new_name:
                    sql = "UPDATE roles SET role_name = %s WHERE role_id = %s"
                    cursor.execute(sql, (new_name, role_id))
                if new_permissions:
                    self._assign_permissions(role_id, new_permissions, update=True)
                self.connection.commit()
                print(f"Role ID '{role_id}' updated successfully.")
        except Exception as e:
            print(f"Error updating role: {e}")
            self.connection.rollback()

    def _assign_permissions(self, role_id, permissions, update=False):
        try:
            with self.connection.cursor() as cursor:
                if update:
                    sql_del = "DELETE FROM permissions WHERE role_id = %s"
                    cursor.execute(sql_del, (role_id,))
                for permission in permissions:
                    sql_perm = "INSERT INTO permissions (role_id, permission_name) VALUES (%s, %s)"
                    cursor.execute(sql_perm, (role_id, permission))
        except Exception as e:
            print(f"Error assigning permissions: {e}")

    def get_role(self, role_id):
        try:
            with self.connection.cursor() as cursor:
                sql = "SELECT * FROM roles WHERE role_id = %s"
                cursor.execute(sql, (role_id,))
                role = cursor.fetchone()
                print(f"Role found: {role}")
                return role
        except Exception as e:
            print(f"Error fetching role: {e}")
            return None

# Example usage
# role_manager = RoleManager()
# role_manager.create_role('Manager', ['view_reports', 'manage_users'])
# role_manager.delete_role(1)
# role_manager.update_role(1, 'Super Manager', ['view_reports', 'edit_reports'])
