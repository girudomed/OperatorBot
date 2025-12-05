-- Migration 001: LM Value Table
-- Таблица для хранения LM-метрик (lm_value)
-- Дата: 2025-12-05

CREATE TABLE IF NOT EXISTS `lm_value` (
  `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `history_id` INT NOT NULL COMMENT 'Связь с call_history',
  `call_score_id` INT NULL COMMENT 'Связь с call_scores',
  
  -- Метрика
  `metric_code` VARCHAR(50) NOT NULL COMMENT 'Код метрики: response_speed_score, churn_risk_level, etc',
  `metric_group` VARCHAR(30) NOT NULL COMMENT 'operational/conversion/quality/risk/forecast/aux',
  
  -- Значения
  `value_numeric` DECIMAL(10,4) NULL COMMENT 'Числовое значение',
  `value_label` VARCHAR(100) NULL COMMENT 'Текстовое значение (high/medium/low)',
  `value_json` JSON NULL COMMENT 'JSON для сложных данных',
  
  -- Метаданные расчёта
  `lm_version` VARCHAR(20) NOT NULL DEFAULT 'lm_v2' COMMENT 'Версия модели',
  `calc_method` VARCHAR(20) NOT NULL DEFAULT 'rule' COMMENT 'rule/ml/hybrid',
  `calc_source` VARCHAR(50) NULL COMMENT 'worker_batch/realtime/backfill',
  
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  
  UNIQUE KEY `uk_history_metric` (`history_id`, `metric_code`, `lm_version`),
  KEY `idx_history_id` (`history_id`),
  KEY `idx_call_score_id` (`call_score_id`),
  KEY `idx_metric_code` (`metric_code`),
  KEY `idx_metric_group` (`metric_group`),
  KEY `idx_lm_version` (`lm_version`),
  
  CONSTRAINT `fk_lm_value_history` 
    FOREIGN KEY (`history_id`) REFERENCES `call_history` (`history_id`) ON DELETE CASCADE,
  CONSTRAINT `fk_lm_value_score` 
    FOREIGN KEY (`call_score_id`) REFERENCES `call_scores` (`id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
