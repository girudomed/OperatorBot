
import asyncio
from app.db.manager import DatabaseManager
import os

async def migrate():
    db = DatabaseManager()
    await db.create_pool()
    try:
        migration_file = "/Users/vitalyefimov/Projects/operabot/scripts/migrations/002_lm_metrics_v2.sql"
        if not os.path.exists(migration_file):
            print(f"Migration file not found: {migration_file}")
            return
            
        with open(migration_file, 'r') as f:
            sql = f.read()
            
        # Split by semicolon but watch out for triggers/procedures (not here)
        commands = [cmd.strip() for cmd in sql.split(';') if cmd.strip()]
        
        print(f"Applying {len(commands)} migration commands...")
        for cmd in commands:
            try:
                print(f"Executing: {cmd[:50]}...")
                await db.execute_with_retry(cmd, commit=True)
            except Exception as e:
                # 1060 (Duplicate column name), 1061 (Duplicate key name) are common in migrations
                if "1060" in str(e) or "1061" in str(e):
                    print(f"Skipping (already exists): {e}")
                else:
                    print(f"Error executing command: {e}")
                    raise e
        
        print("Migration completed successfully.")
    finally:
        await db.close()

if __name__ == "__main__":
    asyncio.run(migrate())
