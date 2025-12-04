"""
Скрипт для проверки схемы UsersTelegaBot
"""
import asyncio
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db.manager import DatabaseManager
from app.logging_config import get_watchdog_logger

logger = get_watchdog_logger(__name__)


async def check_schema():
    """Проверить схему таблицы UsersTelegaBot"""
    db = DatabaseManager()
    
    try:
        await db.connect()
        
        print("=" * 80)
        print("СХЕМА ТАБЛИЦЫ UsersTelegaBot:")
        print("=" * 80)
        
        # Получить схему таблицы
        rows = await db.execute_query("SHOW COLUMNS FROM UsersTelegaBot", fetchall=True)
        
        if rows:
            for row in rows:
                print(f"  {row.get('Field'):<20} {row.get('Type'):<30} {row.get('Null'):<5} {row.get('Key'):<5} {row.get('Default')}")
        
        print("=" * 80)
        print("\nПРОВЕРКА НАЛИЧИЯ КРИТИЧЕСКИХ КОЛОНОК:")
        print("=" * 80)
        
        columns = [row.get('Field') for row in rows]
        
        required_columns = {
            'user_id': 'Telegram user ID',
            'full_name': 'Полное имя',
            'role_id': 'ID роли',
            'status': 'Статус (pending/approved/blocked)',
            'operator_name': 'Имя оператора',
            'extension': 'Extension номер',
            'approved_by': 'Кто одобрил',
            'blocked_at': 'Когда заблокирован'
        }
        
        optional_columns = {
            'username': 'Telegram username',
            'registered_at': 'Дата регистрации',
            'id': 'Автоинкремент ID'
        }
        
        print("\nОбязательные колонки:")
        for col, desc in required_columns.items():
            exists = col in columns
            status = "✅" if exists else "❌"
            print(f"  {status} {col:<20} - {desc}")
        
        print("\nОпциональные колонки:")
        for col, desc in optional_columns.items():
            exists = col in columns
            status = "✅" if exists else "⚠️"
            print(f"  {status} {col:<20} - {desc}")
        
        print("\n" + "=" * 80)
        print("ПРИМЕР ЗАПИСИ:")
        print("=" * 80)
        
        sample = await db.execute_query("SELECT * FROM UsersTelegaBot LIMIT 1", fetchone=True)
        if sample:
            for key, value in sample.items():
                print(f"  {key:<20}: {value}")
        else:
            print("  Таблица пуста")
        
    except Exception as e:
        logger.error(f"Ошибка при проверке схемы: {e}", exc_info=True)
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(check_schema())
