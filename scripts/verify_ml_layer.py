
import asyncio
import sys
import os
from datetime import datetime
from unittest.mock import MagicMock, AsyncMock

# Add project root to path
sys.path.append(os.getcwd())

from app.services.lm_service import LMService
from app.db.repositories.lm_repository import LMRepository
from app.ml.models import CallScorer, ChurnPredictor, UpsellRecommender

async def verify_ml_layer():
    print("Verifying ML Layer...")
    
    # 1. Verify Imports
    try:
        scorer = CallScorer()
        churn = ChurnPredictor()
        upsell = UpsellRecommender()
        print("✅ ML Models imported successfully")
    except Exception as e:
        print(f"❌ Failed to import ML Models: {e}")
        return

    # 2. Verify LMService Integration
    try:
        # Mock Repository
        mock_repo = MagicMock(spec=LMRepository)
        mock_repo.save_lm_values_batch = AsyncMock(return_value=10)
        
        service = LMService(mock_repo)
        print("✅ LMService initialized successfully")
        
        # Test Data
        call_history = {
            'history_id': 123,
            'call_date': datetime.now(),
            'talk_duration': 120,
            'call_type': 'incoming'
        }
        
        call_score = {
            'id': 456,
            'call_score': 8.5,
            'outcome': 'record',
            'call_category': 'Запись на услугу (успешная)',
            'number_checklist': 10,
            'is_target': 1
        }
        
        # Run Calculation
        count = await service.calculate_all_metrics(
            history_id=123,
            call_history=call_history,
            call_score=call_score
        )
        
        print(f"✅ Calculated {count} metrics")
        
        # Verify specific metrics were calculated (via internal logic check)
        ops = service.scorer.calculate_operational_metrics(call_history)
        print(f"   Operational Metrics: {ops}")
        
        quality = service.scorer.calculate_quality_metrics(call_history, call_score)
        print(f"   Quality Metrics: {quality}")
        
        risk = service.churn_predictor.predict_risk_metrics(call_history, call_score)
        print(f"   Risk Metrics: {risk}")
        
    except Exception as e:
        print(f"❌ LMService verification failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(verify_ml_layer())
