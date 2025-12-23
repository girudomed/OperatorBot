
import asyncio
import os
from app.db.manager import DatabaseManager

async def inspect_db():
    db_manager = DatabaseManager()
    await db_manager.create_pool()
    
    # Describe call_scores
    print("--- Describe call_scores ---")
    rows = await db_manager.execute_query("DESCRIBE call_scores;", fetchall=True)
    for row in rows:
        print(row)
        
    # Sample result field
    print("\n--- Sample result from call_scores ---")
    rows = await db_manager.execute_query("SELECT result FROM call_scores WHERE result IS NOT NULL AND result != '' LIMIT 5;", fetchall=True)
    for row in rows:
        print("-" * 20)
        print(row['result'])
        
    await db_manager.close_pool()

if __name__ == "__main__":
    asyncio.run(inspect_db())
