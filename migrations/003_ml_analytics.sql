-- Migration 003: ML Analytics & Caching
-- Created based on code requirements in dashboard_cache.py and analytics.py

-- 1. Create operator_dashboards table
CREATE TABLE IF NOT EXISTS operator_dashboards (
    operator_name VARCHAR(255) NOT NULL,
    period_type VARCHAR(50) NOT NULL,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    
    -- Call Stats
    total_calls INT DEFAULT 0,
    accepted_calls INT DEFAULT 0,
    missed_calls INT DEFAULT 0,
    
    -- Conversion Stats
    records_count INT DEFAULT 0,
    leads_no_record INT DEFAULT 0,
    wish_to_record INT DEFAULT 0,
    conversion_rate DECIMAL(5, 2) DEFAULT 0.00,
    
    -- Quality Stats
    avg_score_all DECIMAL(4, 2) DEFAULT 0.00,
    avg_score_leads DECIMAL(4, 2) DEFAULT 0.00,
    avg_score_cancel DECIMAL(4, 2) DEFAULT 0.00,
    
    -- Cancellation Stats
    cancel_calls INT DEFAULT 0,
    reschedule_calls INT DEFAULT 0,
    cancel_share DECIMAL(5, 2) DEFAULT 0.00,
    
    -- Time Stats
    avg_talk_all INT DEFAULT 0,
    total_talk_time INT DEFAULT 0,
    avg_talk_record INT DEFAULT 0,
    avg_talk_navigation INT DEFAULT 0,
    avg_talk_spam INT DEFAULT 0,
    
    -- Complaint Stats
    complaint_calls INT DEFAULT 0,
    avg_score_complaint DECIMAL(4, 2) DEFAULT 0.00,
    
    -- Metadata
    cached_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (operator_name, period_type, period_start),
    INDEX idx_dashboard_cache_lookup (operator_name, period_type, period_start, cached_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 2. Create operator_recommendations table
CREATE TABLE IF NOT EXISTS operator_recommendations (
    operator_name VARCHAR(255) NOT NULL,
    report_date DATE NOT NULL,
    recommendations TEXT,
    call_samples_analyzed INT DEFAULT 0,
    generated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (operator_name, report_date),
    INDEX idx_recommendations_lookup (operator_name, report_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 3. Add ML columns to call_scores if they don't exist
-- Using a stored procedure to check for column existence to avoid errors
DROP PROCEDURE IF EXISTS UpgradeCallScores;

DELIMITER $$
CREATE PROCEDURE UpgradeCallScores()
BEGIN
    -- ml_p_record
    IF NOT EXISTS (
        SELECT * FROM information_schema.COLUMNS 
        WHERE TABLE_SCHEMA = DATABASE() 
        AND TABLE_NAME = 'call_scores' 
        AND COLUMN_NAME = 'ml_p_record'
    ) THEN
        ALTER TABLE call_scores ADD COLUMN ml_p_record DECIMAL(5, 4) NULL COMMENT 'Probability of record (0-1)';
    END IF;

    -- ml_score_pred
    IF NOT EXISTS (
        SELECT * FROM information_schema.COLUMNS 
        WHERE TABLE_SCHEMA = DATABASE() 
        AND TABLE_NAME = 'call_scores' 
        AND COLUMN_NAME = 'ml_score_pred'
    ) THEN
        ALTER TABLE call_scores ADD COLUMN ml_score_pred DECIMAL(4, 2) NULL COMMENT 'Predicted quality score (0-10)';
    END IF;

    -- ml_p_complaint
    IF NOT EXISTS (
        SELECT * FROM information_schema.COLUMNS 
        WHERE TABLE_SCHEMA = DATABASE() 
        AND TABLE_NAME = 'call_scores' 
        AND COLUMN_NAME = 'ml_p_complaint'
    ) THEN
        ALTER TABLE call_scores ADD COLUMN ml_p_complaint DECIMAL(5, 4) NULL COMMENT 'Probability of complaint (0-1)';
    END IF;

    -- ml_updated_at
    IF NOT EXISTS (
        SELECT * FROM information_schema.COLUMNS 
        WHERE TABLE_SCHEMA = DATABASE() 
        AND TABLE_NAME = 'call_scores' 
        AND COLUMN_NAME = 'ml_updated_at'
    ) THEN
        ALTER TABLE call_scores ADD COLUMN ml_updated_at DATETIME NULL COMMENT 'When ML metrics were last updated';
    END IF;
END$$
DELIMITER ;

CALL UpgradeCallScores();
DROP PROCEDURE IF EXISTS UpgradeCallScores;
