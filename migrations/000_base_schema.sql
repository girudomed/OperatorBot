-- Migration 000: Base Schema
-- Актуальная схема продакшн БД mangoapi_db
-- Дата: 2025-12-05
-- Источник: mangoapi_db (2).sql

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";

-- =============================================================================
-- 1. ROLES_REFERENCE (справочник ролей)
-- PK: role_id
-- =============================================================================
CREATE TABLE IF NOT EXISTS `roles_reference` (
  `role_id` TINYINT UNSIGNED NOT NULL PRIMARY KEY,
  `role_name` VARCHAR(50) NOT NULL,
  `can_view_own_stats` TINYINT(1) DEFAULT 1 COMMENT 'Может видеть свою статистику',
  `can_view_all_stats` TINYINT(1) DEFAULT 0 COMMENT 'Может видеть статистику всех',
  `can_view_dashboard` TINYINT(1) DEFAULT 1 COMMENT 'Доступ к dashboard',
  `can_generate_reports` TINYINT(1) DEFAULT 1 COMMENT 'Может генерировать отчеты',
  `can_view_transcripts` TINYINT(1) DEFAULT 1 COMMENT 'Доступ к расшифровкам',
  `can_manage_users` TINYINT(1) DEFAULT 0 COMMENT 'Управление пользователями',
  `can_debug` TINYINT(1) DEFAULT 0 COMMENT 'Команды отладки',
  `description` TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- =============================================================================
-- 2. RolesTelegaBot (legacy роли)
-- =============================================================================
CREATE TABLE IF NOT EXISTS `RolesTelegaBot` (
  `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `role_name` VARCHAR(255) DEFAULT NULL UNIQUE,
  `role_password` VARCHAR(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- =============================================================================
-- 3. PermissionsTelegaBot (legacy права)
-- =============================================================================
CREATE TABLE IF NOT EXISTS `PermissionsTelegaBot` (
  `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `role_id` INT DEFAULT NULL,
  `permission` VARCHAR(255) DEFAULT NULL,
  KEY `role_id` (`role_id`),
  CONSTRAINT `PermissionsTelegaBot_ibfk_1` FOREIGN KEY (`role_id`) REFERENCES `RolesTelegaBot` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- =============================================================================
-- 4. UsersTelegaBot (пользователи Telegram бота)
-- ВАЖНО: Есть ОБА поля telegram_id и user_id (дублирование)
-- Код использует user_id
-- =============================================================================
CREATE TABLE IF NOT EXISTS `UsersTelegaBot` (
  `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `telegram_id` BIGINT DEFAULT NULL UNIQUE,
  `user_id` BIGINT DEFAULT NULL UNIQUE,
  `extension` VARCHAR(50) DEFAULT NULL COMMENT 'Extension оператора',
  `username` VARCHAR(255) DEFAULT NULL,
  `full_name` VARCHAR(255) DEFAULT NULL,
  `operator_name` VARCHAR(255) DEFAULT NULL COMMENT 'Имя оператора из call_scores',
  `role_id` TINYINT UNSIGNED DEFAULT NULL COMMENT '1=Оператор,2=Администратор,...',
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `password` VARCHAR(255) DEFAULT NULL,
  `operator_id` VARCHAR(255) DEFAULT NULL,
  `chat_id` BIGINT DEFAULT NULL,
  `status` ENUM('pending','approved','blocked') NOT NULL DEFAULT 'pending',
  `approved_by` INT DEFAULT NULL COMMENT 'UsersTelegaBot.id админа',
  `blocked_at` DATETIME DEFAULT NULL,
  `last_active_at` DATETIME DEFAULT NULL,
  KEY `idx_telega_status` (`status`),
  KEY `idx_telega_approved_by` (`approved_by`),
  KEY `idx_users_operator_name` (`operator_name`),
  KEY `idx_users_extension` (`extension`),
  CONSTRAINT `fk_telega_approved_by` FOREIGN KEY (`approved_by`) REFERENCES `UsersTelegaBot` (`id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- =============================================================================
-- 5. users (операторы Mango ВАТС)
-- =============================================================================
CREATE TABLE IF NOT EXISTS `users` (
  `user_id` INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'pk',
  `name` VARCHAR(255) DEFAULT NULL COMMENT 'Имя',
  `mobile` VARCHAR(255) DEFAULT NULL,
  `login` VARCHAR(255) DEFAULT NULL,
  `email` VARCHAR(255) DEFAULT NULL,
  `department` VARCHAR(255) DEFAULT NULL,
  `position` VARCHAR(255) DEFAULT NULL,
  `extension` VARCHAR(255) DEFAULT NULL COMMENT 'Идентификатор сотрудника ВАТС',
  `outgoingline` VARCHAR(255) DEFAULT NULL,
  `number` VARCHAR(255) DEFAULT NULL UNIQUE COMMENT 'mangosip',
  `protocol` VARCHAR(255) DEFAULT NULL,
  `order` SMALLINT UNSIGNED DEFAULT 1,
  `wait_sec` SMALLINT UNSIGNED DEFAULT 60,
  `status` VARCHAR(255) DEFAULT 'on',
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `full_name` VARCHAR(255) DEFAULT NULL,
  KEY `idx_users_outgoingline` (`outgoingline`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- =============================================================================
-- 6. call_history (история звонков)
-- =============================================================================
CREATE TABLE IF NOT EXISTS `call_history` (
  `history_id` INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'pk',
  `context_type` TINYINT(1) DEFAULT NULL,
  `caller_number` VARCHAR(255) NOT NULL,
  `caller_info` VARCHAR(255) DEFAULT NULL,
  `called_number` VARCHAR(255) NOT NULL,
  `called_info` VARCHAR(255) DEFAULT NULL,
  `talk_duration` SMALLINT UNSIGNED DEFAULT NULL,
  `await_sec` SMALLINT UNSIGNED DEFAULT NULL,
  `context_start_time` INT UNSIGNED DEFAULT NULL,
  `context_status` TINYINT(1) DEFAULT NULL,
  `recall_status` INT UNSIGNED DEFAULT NULL,
  `utm_source_by_number` MEDIUMTEXT,
  `categories` MEDIUMTEXT,
  `transcript` LONGTEXT,
  `entry_id` VARCHAR(255) NOT NULL UNIQUE,
  `caller_id` INT UNSIGNED DEFAULT NULL,
  `recording_id` VARCHAR(255) DEFAULT NULL UNIQUE,
  `phrases` JSON DEFAULT NULL,
  `uploaded_at` DATETIME DEFAULT NULL,
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `downloaded_at` DATETIME DEFAULT NULL,
  `processed` TINYINT DEFAULT NULL,
  `context_start_time_dt` DATETIME DEFAULT NULL,
  `transcription_status` ENUM('pending','running','done','failed') NOT NULL DEFAULT 'pending',
  `transcription_worker` VARCHAR(255) DEFAULT NULL,
  `transcription_error` MEDIUMTEXT,
  `error_class` VARCHAR(255) DEFAULT NULL,
  `error_message_short` VARCHAR(500) DEFAULT NULL,
  `error_http_status` INT DEFAULT NULL,
  `transcribed_at` DATETIME DEFAULT NULL,
  `audio_path` MEDIUMTEXT,
  `duration` SMALLINT UNSIGNED DEFAULT NULL,
  `context_start_timestamp` INT DEFAULT NULL,
  `needs_meta` TINYINT DEFAULT NULL,
  `meta_source` ENUM('queries','stats','file-only','unknown') DEFAULT 'unknown',
  `meta_last_checked_at` DATETIME DEFAULT NULL,
  `answered_extension` VARCHAR(50) DEFAULT NULL,
  `answered_user_id` INT DEFAULT NULL,
  `needs_enrichment` TINYINT DEFAULT NULL,
  `download_attempts` INT DEFAULT NULL,
  `download_error` MEDIUMTEXT,
  `file_size` BIGINT DEFAULT NULL,
  `download_started_at` DATETIME DEFAULT NULL,
  `download_finished_at` DATETIME DEFAULT NULL,
  `download_retry_at` DATETIME DEFAULT NULL,
  `transcription_started_at` DATETIME DEFAULT NULL,
  `transcription_finished_at` DATETIME DEFAULT NULL,
  `transcription_attempts` INT DEFAULT NULL,
  `context_ts` DATETIME DEFAULT NULL,
  `calc_cts` DATETIME DEFAULT NULL,
  `audio_size` BIGINT DEFAULT NULL,
  `downloaded` TINYINT(1) NOT NULL DEFAULT 0,
  `file_size_mango` BIGINT DEFAULT NULL,
  `stt_status` VARCHAR(20) NOT NULL DEFAULT 'pending',
  `stt_attempts` INT NOT NULL DEFAULT 0,
  `stt_updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `has_audio` TINYINT(1) NOT NULL DEFAULT 0,
  KEY `idx_context_start_time` (`context_start_time`),
  KEY `idx_call_history_processed` (`processed`),
  KEY `idx_needs_enrichment` (`needs_enrichment`),
  KEY `ix_ch_ans_user` (`answered_user_id`),
  KEY `ix_ch_answ_ext` (`answered_extension`),
  KEY `idx_ch_duration` (`duration`),
  KEY `idx_ch_created` (`created_at`),
  KEY `idx_stt_pending_audio` (`stt_status`, `has_audio`, `recording_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =============================================================================
-- 7. call_scores (оценки звонков)
-- =============================================================================
CREATE TABLE IF NOT EXISTS `call_scores` (
  `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `history_id` INT UNSIGNED NOT NULL,
  `call_score` FLOAT NOT NULL,
  `score_date` DATETIME NOT NULL,
  `called_info` VARCHAR(255) DEFAULT NULL,
  `call_date` DATETIME DEFAULT NULL,
  `call_type` VARCHAR(50) DEFAULT NULL,
  `context_type` VARCHAR(255) DEFAULT NULL,
  `talk_duration` SMALLINT UNSIGNED DEFAULT NULL,
  `call_success` VARCHAR(50) DEFAULT NULL,
  `transcript` MEDIUMTEXT,
  `result` MEDIUMTEXT,
  `caller_info` VARCHAR(255) NOT NULL,
  `caller_number` VARCHAR(255) DEFAULT NULL,
  `called_number` VARCHAR(255) DEFAULT NULL,
  `utm_source_by_number` MEDIUMTEXT,
  `call_category` MEDIUMTEXT NOT NULL,
  `number_category` INT NOT NULL,
  `number_checklist` INT DEFAULT NULL,
  `category_checklist` MEDIUMTEXT,
  `is_target` TINYINT(1) NOT NULL DEFAULT 0,
  `outcome` VARCHAR(32) DEFAULT NULL,
  `requested_service_id` INT UNSIGNED DEFAULT NULL,
  `requested_service_name` VARCHAR(255) DEFAULT NULL,
  `requested_doctor_id` INT UNSIGNED DEFAULT NULL,
  `requested_doctor_name` VARCHAR(255) DEFAULT NULL,
  `requested_doctor_speciality` VARCHAR(255) DEFAULT NULL,
  `refusal_reason` VARCHAR(255) DEFAULT NULL,
  `ml_p_record` DECIMAL(5,4) DEFAULT NULL,
  `ml_score_pred` DECIMAL(4,2) DEFAULT NULL,
  `ml_p_complaint` DECIMAL(5,4) DEFAULT NULL,
  `ml_updated_at` DATETIME DEFAULT NULL,
  KEY `idx_call_date` (`call_date`),
  KEY `idx_history_id` (`history_id`),
  KEY `idx_call_scores_target` (`is_target`, `outcome`),
  KEY `idx_call_scores_service` (`requested_service_id`),
  KEY `idx_call_scores_doctor` (`requested_doctor_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- =============================================================================
-- 8. call_analytics (денормализованная аналитика)
-- ВАЖНО: Поля response_speed_score, talk_time_efficiency etc СУЩЕСТВУЮТ!
-- =============================================================================
CREATE TABLE IF NOT EXISTS `call_analytics` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `call_scores_id` INT NOT NULL COMMENT 'PK из call_scores.id',
  `history_id` INT UNSIGNED NOT NULL,
  `call_date` DATETIME NOT NULL,
  `call_type` VARCHAR(50) DEFAULT NULL,
  `operator_name` VARCHAR(255) DEFAULT NULL,
  `operator_extension` VARCHAR(50) DEFAULT NULL,
  `is_target` TINYINT(1) NOT NULL DEFAULT 0,
  `response_speed_score` DECIMAL(5,2) DEFAULT NULL,
  `talk_time_efficiency` DECIMAL(5,2) DEFAULT NULL,
  `conversion_score` DECIMAL(5,2) DEFAULT NULL,
  `churn_risk_score` DECIMAL(5,2) DEFAULT NULL,
  `churn_risk_level` VARCHAR(20) DEFAULT NULL,
  `synced_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  `outcome` VARCHAR(32) DEFAULT NULL,
  `call_category` MEDIUMTEXT,
  `call_score` FLOAT DEFAULT NULL,
  `talk_duration` SMALLINT UNSIGNED DEFAULT NULL,
  `ml_p_record` DECIMAL(5,4) DEFAULT NULL,
  `ml_score_pred` DECIMAL(4,2) DEFAULT NULL,
  `ml_p_complaint` DECIMAL(5,4) DEFAULT NULL,
  `ml_updated_at` TIMESTAMP DEFAULT NULL,
  UNIQUE KEY `uk_history` (`history_id`),
  UNIQUE KEY `uk_call_scores_id` (`call_scores_id`),
  KEY `idx_ca_operator_date` (`operator_name`, `call_date`),
  KEY `idx_ca_target_outcome` (`is_target`, `outcome`, `call_date`),
  KEY `idx_ca_quality` (`call_score`, `call_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- =============================================================================
-- 9. admin_action_logs (аудит админ-действий)
-- actor_id/target_id → UsersTelegaBot.id (PK!)
-- =============================================================================
CREATE TABLE IF NOT EXISTS `admin_action_logs` (
  `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `actor_id` INT NOT NULL COMMENT 'UsersTelegaBot.id',
  `target_id` INT DEFAULT NULL COMMENT 'UsersTelegaBot.id',
  `action` VARCHAR(50) NOT NULL COMMENT 'approve, block, unblock, promote, demote, etc.',
  `payload_json` TEXT COMMENT 'Additional JSON data',
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  KEY `idx_actor` (`actor_id`),
  KEY `idx_target` (`target_id`),
  KEY `idx_action` (`action`),
  KEY `idx_created` (`created_at`),
  KEY `idx_action_created` (`action`, `created_at`),
  CONSTRAINT `fk_admin_logs_actor` FOREIGN KEY (`actor_id`) REFERENCES `UsersTelegaBot` (`id`) ON DELETE CASCADE,
  CONSTRAINT `fk_admin_logs_target` FOREIGN KEY (`target_id`) REFERENCES `UsersTelegaBot` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =============================================================================
-- 10. operator_dashboards (кеш дашбордов)
-- =============================================================================
CREATE TABLE IF NOT EXISTS `operator_dashboards` (
  `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `operator_name` VARCHAR(255) NOT NULL,
  `period_type` ENUM('day','week','month') NOT NULL,
  `period_start` DATE NOT NULL,
  `period_end` DATE NOT NULL,
  `total_calls` INT DEFAULT 0,
  `accepted_calls` INT DEFAULT 0,
  `missed_calls` INT DEFAULT 0,
  `records_count` INT DEFAULT 0,
  `leads_no_record` INT DEFAULT 0,
  `wish_to_record` INT DEFAULT 0,
  `conversion_rate` DECIMAL(5,2) DEFAULT 0.00,
  `avg_score_all` DECIMAL(4,2) DEFAULT 0.00,
  `avg_score_leads` DECIMAL(4,2) DEFAULT 0.00,
  `avg_score_cancel` DECIMAL(4,2) DEFAULT 0.00,
  `cancel_calls` INT DEFAULT 0,
  `reschedule_calls` INT DEFAULT 0,
  `cancel_share` DECIMAL(5,2) DEFAULT 0.00,
  `avg_talk_all` INT DEFAULT 0,
  `total_talk_time` INT DEFAULT 0,
  `avg_talk_record` INT DEFAULT 0,
  `avg_talk_navigation` INT DEFAULT 0,
  `avg_talk_spam` INT DEFAULT 0,
  `complaint_calls` INT DEFAULT 0,
  `avg_score_complaint` DECIMAL(4,2) DEFAULT 0.00,
  `expected_records` DECIMAL(8,2) DEFAULT 0.00,
  `record_uplift` DECIMAL(8,2) DEFAULT 0.00,
  `hot_missed_leads` INT DEFAULT 0,
  `difficulty_index` DECIMAL(5,4) DEFAULT 0.0000,
  `cached_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `uk_operator_period` (`operator_name`, `period_type`, `period_start`),
  KEY `idx_cached_at` (`cached_at`),
  KEY `idx_period_type` (`period_type`, `period_start`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- =============================================================================
-- 11. operator_recommendations (LLM рекомендации)
-- =============================================================================
CREATE TABLE IF NOT EXISTS `operator_recommendations` (
  `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `operator_name` VARCHAR(255) NOT NULL,
  `report_date` DATE NOT NULL,
  `recommendations` TEXT COMMENT 'Рекомендации по улучшению',
  `call_samples_analyzed` INT DEFAULT 0,
  `generated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `uk_operator_date` (`operator_name`, `report_date`),
  KEY `idx_report_date` (`report_date`),
  KEY `idx_generated_at` (`generated_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- =============================================================================
-- 12. reports (отчёты)
-- PK: report_id
-- period, report_date — VARCHAR!
-- =============================================================================
CREATE TABLE IF NOT EXISTS `reports` (
  `report_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `user_id` INT DEFAULT NULL,
  `name` VARCHAR(255) DEFAULT NULL,
  `report_text` TEXT,
  `period` VARCHAR(20) NOT NULL,
  `report_date` VARCHAR(50) NOT NULL,
  `total_calls` INT DEFAULT 0,
  `accepted_calls` INT DEFAULT 0,
  `booked_services` INT DEFAULT 0,
  `conversion_rate` FLOAT DEFAULT 0,
  `avg_call_rating` FLOAT DEFAULT 0,
  `total_cancellations` INT DEFAULT 0,
  `cancellation_rate` FLOAT DEFAULT 0,
  `total_conversation_time` FLOAT DEFAULT 0,
  `avg_conversation_time` FLOAT DEFAULT 0,
  `avg_spam_time` FLOAT DEFAULT 0,
  `total_spam_time` FLOAT DEFAULT 0,
  `avg_navigation_time` FLOAT DEFAULT 0,
  `complaint_calls` INT DEFAULT 0,
  `complaint_rating` FLOAT DEFAULT 0,
  `recommendations` TEXT,
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `missed_calls` INT DEFAULT NULL,
  `missed_rate` FLOAT DEFAULT NULL,
  `total_leads` INT DEFAULT NULL,
  `conversion_rate_leads` FLOAT DEFAULT NULL,
  `avg_lead_call_rating` FLOAT DEFAULT NULL,
  `avg_cancel_score` FLOAT DEFAULT NULL,
  `avg_service_time` FLOAT DEFAULT 0,
  `avg_time_spam` FLOAT DEFAULT 0,
  `avg_time_reminder` FLOAT DEFAULT 0,
  `avg_time_cancellation` FLOAT DEFAULT 0,
  `avg_time_complaints` FLOAT DEFAULT 0,
  `avg_time_reservations` FLOAT DEFAULT 0,
  `avg_time_reschedule` FLOAT DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- =============================================================================
-- 13. call_analysis (детальный анализ звонков)
-- =============================================================================
CREATE TABLE IF NOT EXISTS `call_analysis` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `history_id` INT NOT NULL UNIQUE,
  `call_date` DATETIME DEFAULT NULL,
  `call_type` VARCHAR(50) DEFAULT NULL,
  `talk_duration` SMALLINT UNSIGNED DEFAULT NULL,
  `call_success` VARCHAR(50) DEFAULT NULL,
  `caller_info` VARCHAR(255) NOT NULL,
  `called_info` VARCHAR(255) DEFAULT NULL,
  `call_category` TEXT NOT NULL,
  `number_category` INT NOT NULL,
  `category_checklist` TEXT,
  `number_checklist` INT DEFAULT NULL,
  `transcript` LONGTEXT,
  `result` LONGTEXT,
  `greeting_score` TINYINT UNSIGNED NOT NULL,
  `name_usage_score` TINYINT UNSIGNED NOT NULL,
  `active_listening_score` TINYINT UNSIGNED NOT NULL,
  `speech_clarity_score` TINYINT UNSIGNED NOT NULL,
  `need_identification_score` TINYINT UNSIGNED NOT NULL,
  `knowledge_services_score` TINYINT UNSIGNED NOT NULL,
  `price_address_score` TINYINT UNSIGNED NOT NULL,
  `presentation_score` TINYINT UNSIGNED NOT NULL,
  `conversation_mgmt_score` TINYINT UNSIGNED NOT NULL,
  `alternatives_score` TINYINT UNSIGNED NOT NULL,
  `hidden_objection_score` TINYINT UNSIGNED NOT NULL,
  `objection_handling_score` TINYINT UNSIGNED NOT NULL,
  `contact_followup_score` TINYINT UNSIGNED NOT NULL,
  `summary_score` TINYINT UNSIGNED NOT NULL,
  `motivation_score` TINYINT UNSIGNED NOT NULL,
  `next_step_score` TINYINT UNSIGNED NOT NULL,
  `overall_score` DECIMAL(5,2) GENERATED ALWAYS AS (
    ROUND((greeting_score + name_usage_score + active_listening_score + 
           speech_clarity_score + need_identification_score + knowledge_services_score +
           price_address_score + presentation_score + conversation_mgmt_score +
           alternatives_score + hidden_objection_score + objection_handling_score +
           contact_followup_score + summary_score + motivation_score + next_step_score) / 16, 2)
  ) STORED,
  `call_score` JSON DEFAULT NULL,
  `prompt_tokens` INT UNSIGNED DEFAULT NULL,
  `completion_tokens` INT UNSIGNED DEFAULT NULL,
  `latency_ms` INT UNSIGNED DEFAULT NULL,
  `cost_usd` DECIMAL(7,4) DEFAULT NULL,
  `score_date` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `processed_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY `idx_history` (`history_id`),
  KEY `idx_call_date` (`call_date`),
  KEY `idx_processed` (`processed_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- =============================================================================
-- 14. company_employee_login (legacy)
-- =============================================================================
CREATE TABLE IF NOT EXISTS `company_employee_login` (
  `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `company_name` VARCHAR(100) NOT NULL,
  `employee_name` VARCHAR(100) NOT NULL,
  `login` VARCHAR(50) NOT NULL UNIQUE,
  `password` VARCHAR(50) NOT NULL,
  `license_expiry_date` DATE NOT NULL,
  `license_key` VARCHAR(100) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
