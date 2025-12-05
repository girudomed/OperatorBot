import asyncio
import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db.manager import DatabaseManager

async def apply_migration():
    print("Initializing DatabaseManager...")
    db = DatabaseManager()
    await db.create_pool()
    
    migration_file = "migrations/003_ml_analytics.sql"
    print(f"Reading migration file: {migration_file}")
    
    try:
        with open(migration_file, 'r') as f:
            sql_content = f.read()
            
        print("Executing migration...")
        
        # 1. Create operator_dashboards
        print("Creating operator_dashboards...")
        await db.execute_query("""
            CREATE TABLE IF NOT EXISTS operator_dashboards (
                operator_name VARCHAR(255) NOT NULL,
                period_type VARCHAR(50) NOT NULL,
                period_start DATE NOT NULL,
                period_end DATE NOT NULL,
                total_calls INT DEFAULT 0,
                accepted_calls INT DEFAULT 0,
                missed_calls INT DEFAULT 0,
                records_count INT DEFAULT 0,
                leads_no_record INT DEFAULT 0,
                wish_to_record INT DEFAULT 0,
                conversion_rate DECIMAL(5, 2) DEFAULT 0.00,
                avg_score_all DECIMAL(4, 2) DEFAULT 0.00,
                avg_score_leads DECIMAL(4, 2) DEFAULT 0.00,
                avg_score_cancel DECIMAL(4, 2) DEFAULT 0.00,
                cancel_calls INT DEFAULT 0,
                reschedule_calls INT DEFAULT 0,
                cancel_share DECIMAL(5, 2) DEFAULT 0.00,
                avg_talk_all INT DEFAULT 0,
                total_talk_time INT DEFAULT 0,
                avg_talk_record INT DEFAULT 0,
                avg_talk_navigation INT DEFAULT 0,
                avg_talk_spam INT DEFAULT 0,
                complaint_calls INT DEFAULT 0,
                avg_score_complaint DECIMAL(4, 2) DEFAULT 0.00,
                cached_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (operator_name, period_type, period_start),
                INDEX idx_dashboard_cache_lookup (operator_name, period_type, period_start, cached_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)
        
        # 2. Create operator_recommendations
        print("Creating operator_recommendations...")
        await db.execute_query("""
            CREATE TABLE IF NOT EXISTS operator_recommendations (
                operator_name VARCHAR(255) NOT NULL,
                report_date DATE NOT NULL,
                recommendations TEXT,
                call_samples_analyzed INT DEFAULT 0,
                generated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (operator_name, report_date),
                INDEX idx_recommendations_lookup (operator_name, report_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)
        
        # 3. Add columns
        columns_to_add = [
            "ADD COLUMN ml_p_record DECIMAL(5, 4) NULL COMMENT 'Probability of record (0-1)'",
            "ADD COLUMN ml_score_pred DECIMAL(4, 2) NULL COMMENT 'Predicted quality score (0-10)'",
            "ADD COLUMN ml_p_complaint DECIMAL(5, 4) NULL COMMENT 'Probability of complaint (0-1)'",
            "ADD COLUMN ml_updated_at DATETIME NULL COMMENT 'When ML metrics were last updated'"
        ]
        
        for col_def in columns_to_add:
            try:
                col_name = col_def.split()[2]
                print(f"Adding column {col_name}...")
                await db.execute_query(f"ALTER TABLE call_scores {col_def}")
                print(f"Column {col_name} added.")
            except Exception as e:
                # Check for duplicate column error (MySQL error 1060)
                if "Duplicate column name" in str(e) or "1060" in str(e):
                    print(f"Column {col_name} already exists.")
                else:
                    print(f"Error adding column {col_name}: {e}")
        
        print("Migration completed successfully.")
        
    except Exception as e:
        print(f"Migration failed: {e}")
    finally:
        await db.close()

if __name__ == "__main__":
    asyncio.run(apply_migration())
