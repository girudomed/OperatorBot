-- Migration 002: Notifications Table
-- Таблица уведомлений для пользователей
-- Дата: 2025-12-05

CREATE TABLE IF NOT EXISTS `notifications` (
  `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `user_id` INT NOT NULL COMMENT 'Telegram user_id или UsersTelegaBot.user_id',
  `message` TEXT NOT NULL COMMENT 'Текст уведомления',
  `is_read` TINYINT(1) DEFAULT 0 COMMENT 'Прочитано ли',
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  
  KEY `idx_user_id` (`user_id`),
  KEY `idx_created_at` (`created_at`),
  KEY `idx_unread` (`user_id`, `is_read`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
