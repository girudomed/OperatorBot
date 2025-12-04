-- Migration 003: ML Analytics and Dashboard Infrastructure
-- Adds ML prediction fields, caching tables, and optimization indexes

-- ============================================================================
-- Part 1: ML Prediction Fields in call_scores
-- ============================================================================

-- Add ML prediction columns to call_scores
ALTER TABLE call_scores
ADD COLUMN ml_p_record DECIMAL(5,4) NULL 
    COMMENT 'ML прогноз вероятности записи (0-1)',
ADD COLUMN ml_score_pred DECIMAL(4,2) NULL 
    COMMENT 'ML прогноз ожидаемой оценки (0-10)',
ADD COLUMN ml_p_complaint DECIMAL(5,4) NULL 
    COMMENT 'ML прогноз риска жалобы (0-1)',
ADD COLUMN ml_updated_at TIMESTAMP NULL 
    COMMENT 'Время последнего ML-прогноза';

-- ============================================================================
-- Part 2: Indexes for Analytics Performance
-- ============================================================================

-- Index for operator-based queries (most common dashboard filter)
CREATE INDEX idx_call_scores_operator_date 
    ON call_scores(call_date, called_info, caller_info);

-- Index for target/outcome analysis (conversion metrics)
CREATE INDEX idx_call_scores_target_outcome 
    ON call_scores(is_target, outcome, call_date);

-- Index for category-based analysis (time metrics by category)
CREATE INDEX idx_call_scores_category 
    ON call_scores(call_category, is_target, call_date);

-- Index for quality analysis
CREATE INDEX idx_call_scores_quality 
    ON call_scores(call_score, call_date);

-- ============================================================================
-- Part 3: Dashboard Caching Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS operator_dashboards (
    id INT AUTO_INCREMENT PRIMARY KEY,
    operator_name VARCHAR(255) NOT NULL,
    period_type ENUM('day', 'week', 'month') NOT NULL,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    
    -- Общая статистика по звонкам
    total_calls INT DEFAULT 0 COMMENT 'Всего звонков за период',
    accepted_calls INT DEFAULT 0 COMMENT 'Принято звонков',
    missed_calls INT DEFAULT 0 COMMENT 'Пропущено звонков',
    
    -- Конверсионные метрики
    records_count INT DEFAULT 0 COMMENT 'Записей на услугу',
    leads_no_record INT DEFAULT 0 COMMENT 'Лидов без записи',
    wish_to_record INT DEFAULT 0 COMMENT 'Желающих записаться (records + leads_no_record)',
    conversion_rate DECIMAL(5,2) DEFAULT 0 COMMENT 'Конверсия в запись (%)',
    
    -- Метрики качества
    avg_score_all DECIMAL(4,2) DEFAULT 0 COMMENT 'Средняя оценка всех звонков',
    avg_score_leads DECIMAL(4,2) DEFAULT 0 COMMENT 'Средняя оценка звонков желающих записаться',
    avg_score_cancel DECIMAL(4,2) DEFAULT 0 COMMENT 'Средняя оценка при отменах',
    
    -- Метрики отмен
    cancel_calls INT DEFAULT 0 COMMENT 'Количество отмен',
    reschedule_calls INT DEFAULT 0 COMMENT 'Количество переносов',
    cancel_share DECIMAL(5,2) DEFAULT 0 COMMENT 'Доля отмен (%)',
    
    -- Метрики времени
    avg_talk_all INT DEFAULT 0 COMMENT 'Среднее время разговора (сек)',
    total_talk_time INT DEFAULT 0 COMMENT 'Общее время разговоров (сек)',
    avg_talk_record INT DEFAULT 0 COMMENT 'Среднее время при записи (сек)',
    avg_talk_navigation INT DEFAULT 0 COMMENT 'Среднее время навигации (сек)',
    avg_talk_spam INT DEFAULT 0 COMMENT 'Среднее время со спамом (сек)',
    
    -- Метрики жалоб
    complaint_calls INT DEFAULT 0 COMMENT 'Звонков с жалобами',
    avg_score_complaint DECIMAL(4,2) DEFAULT 0 COMMENT 'Средняя оценка жалоб',
    
    -- ML метрики (опционально, заполняются при наличии ML-моделей)
    expected_records DECIMAL(8,2) DEFAULT 0 COMMENT 'Ожидаемое число записей (ML)',
    record_uplift DECIMAL(8,2) DEFAULT 0 COMMENT 'Переизполнение плана (ML)',
    hot_missed_leads INT DEFAULT 0 COMMENT 'Упущенные горячие лиды (ML)',
    difficulty_index DECIMAL(5,4) DEFAULT 0 COMMENT 'Индекс сложности потока (ML)',
    
    -- Мета-данные
    cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Время создания кеша',
    
    -- Уникальный ключ для предотвращения дубликатов
    UNIQUE KEY uk_operator_period (operator_name, period_type, period_start),
    
    -- Индексы для быстрого поиска
    INDEX idx_cached_at (cached_at),
    INDEX idx_period_type (period_type, period_start)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 
COMMENT='Кеш метрик дашборда для операторов';

-- ============================================================================
-- Part 4: Recommendations Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS operator_recommendations (
    id INT AUTO_INCREMENT PRIMARY KEY,
    operator_name VARCHAR(255) NOT NULL,
    report_date DATE NOT NULL,
    recommendations TEXT COMMENT 'LLM-генерированные рекомендации',
    call_samples_analyzed INT DEFAULT 0 COMMENT 'Количество звонков в анализе',
    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Уникальность по оператору и дате
    UNIQUE KEY uk_operator_date (operator_name, report_date),
    
    -- Индекс для поиска актуальных рекомендаций
    INDEX idx_report_date (report_date),
    INDEX idx_generated_at (generated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT='Хранение LLM-рекомендаций для операторов';

-- ============================================================================
-- Part 5: Verification Queries
-- ============================================================================

-- Проверка добавленных колонок в call_scores
SELECT 
    COLUMN_NAME, 
    DATA_TYPE, 
    COLUMN_COMMENT 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = DATABASE() 
  AND TABLE_NAME = 'call_scores' 
  AND COLUMN_NAME LIKE 'ml_%';

-- Проверка созданных индексов
SELECT 
    INDEX_NAME, 
    COLUMN_NAME 
FROM INFORMATION_SCHEMA.STATISTICS 
WHERE TABLE_SCHEMA = DATABASE() 
  AND TABLE_NAME = 'call_scores' 
  AND INDEX_NAME LIKE 'idx_call_scores_%'
ORDER BY INDEX_NAME, SEQ_IN_INDEX;
