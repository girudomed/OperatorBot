import asyncio
import logging
import datetime
from app.db.manager import DatabaseManager
from app.services.reports import ReportService
from app.logging_config import setup_app_logging

async def diag():
    setup_app_logging()
    db_manager = DatabaseManager()
    report_service = ReportService(db_manager)
    
    user_id = 2  # Example user_id from logs
    period = "daily"
    
    print(f"Starting diagnostic for user_id={user_id}, period={period}")
    try:
        # We wrap it in a timeout to catch hangs
        report = await asyncio.wait_for(
            report_service.generate_report(user_id=user_id, period=period),
            timeout=30.0
        )
        print("Report generation finished!")
        print(f"Report length: {len(report) if report else 0}")
        if report:
            print("Report preview:")
            print(report[:500])
    except asyncio.TimeoutError:
        print("DIAGNOSTIC FAILED: Hang detected during report generation!")
    except Exception as e:
        print(f"DIAGNOSTIC FAILED with exception: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await db_manager.close_pool()

if __name__ == "__main__":
    asyncio.run(diag())
