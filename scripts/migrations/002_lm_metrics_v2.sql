
-- Миграция: Phase 2 - LM Metrics Backend Updates
-- Обновление схемы для инкрементальных расчетов и SST v1912

-- 1. Добавление полей в lm_value
-- Note: IF NOT EXISTS in ALTER TABLE works in MariaDB 10.2.19+, but not standard MySQL < 8.0.19
-- We improve the python runner to handle errors or use safer syntax.
ALTER TABLE lm_value 
ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;

ALTER TABLE lm_value
ADD COLUMN calc_profile VARCHAR(64) DEFAULT 'default_v1' AFTER lm_version;

-- 2. Создание таблицы для watermark (состояние расчетов)
CREATE TABLE IF NOT EXISTS lm_calc_state (
    lm_version VARCHAR(64) NOT NULL,
    calc_profile VARCHAR(64) NOT NULL,
    last_score_date DATETIME,
    last_id BIGINT DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (lm_version, calc_profile)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- 3. Индексы
-- We don't use IF NOT EXISTS here as it's not widely supported. 
-- The python runner should handle "Duplicate key name" error (1061).
CREATE INDEX idx_lm_value_updated_at ON lm_value(updated_at);
CREATE INDEX idx_lm_value_history_metrics ON lm_value(history_id, metric_code);
