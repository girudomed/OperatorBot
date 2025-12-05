-- Migration 003: Call Access Logs Table
-- Лог доступа к звонкам через call_lookup
-- Дата: 2025-12-05

CREATE TABLE IF NOT EXISTS `call_access_logs` (
  `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `user_id` INT NOT NULL COMMENT 'Telegram user_id',
  `phone_normalized` VARCHAR(20) NOT NULL COMMENT 'Нормализованный номер телефона',
  `result_count` INT DEFAULT 0 COMMENT 'Количество найденных записей',
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  
  KEY `idx_user_id` (`user_id`),
  KEY `idx_phone` (`phone_normalized`),
  KEY `idx_created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
