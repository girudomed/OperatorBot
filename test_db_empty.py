import asyncio
from app.db.manager import DatabaseManager
from app.logging_config import setup_app_logging

async def test_empty_query():
    setup_app_logging()
    db = DatabaseManager()
    
    print("Testing empty query execution...")
    try:
        await db.execute_query("")
        print("FAIL: check did not trigger")
    except ValueError as e:
        print(f"SUCCESS: Caught expected error: {e}")
    except Exception as e:
        print(f"FAIL: Caught unexpected error: {type(e)} {e}")
        
    print("Testing whitespace query execution...")
    try:
        await db.execute_query("   ")
        print("FAIL: check did not trigger")
    except ValueError as e:
        print(f"SUCCESS: Caught expected error: {e}")
        
    await db.close_pool()

if __name__ == "__main__":
    asyncio.run(test_empty_query())
