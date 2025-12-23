
import asyncio
from app.db.manager import DatabaseManager
from app.config import DB_CONFIG

async def check():
    db = DatabaseManager()
    await db.create_pool()
    try:
        for table in ["call_history", "call_analytics", "lm_value"]:
            query = f"DESCRIBE {table}"
            rows = await db.execute_query(query, fetchall=True)
            print(f"COLUMNS for {table}:")
            for row in rows:
                print(f" - {row['Field']}")
    finally:
        await db.close()

if __name__ == "__main__":
    asyncio.run(check())
