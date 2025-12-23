
import asyncio
from app.db.manager import DatabaseManager
from app.db.repositories.lm_repository import LMRepository
from app.db.repositories.lm_dictionary_repository import LMDictionaryRepository
from app.services.lm_service import LMService
import json

async def verify():
    db = DatabaseManager()
    await db.create_pool()
    try:
        repo = LMRepository(db)
        dictionary_repo = LMDictionaryRepository(db)
        service = LMService(repo, dictionary_repository=dictionary_repo)
        
        print(f"Testing sync_new_metrics (v1912)...")
        result = await service.sync_new_metrics(limit=5)
        print(f"Sync result: {json.dumps(result, indent=2, ensure_ascii=False)}")
        
        # Check watermark
        watermark = await repo.get_calc_watermark(service.lm_version, "default_v1")
        print(f"New watermark: {watermark}")
        
    finally:
        await db.close()

if __name__ == "__main__":
    asyncio.run(verify())
