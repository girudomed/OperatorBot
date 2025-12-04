-- Migration 004: Extended Roles and Operator Linking
-- Expands role system to 8 levels and adds operator linking fields

-- ============================================================================
-- Part 1: Update Role System
-- ============================================================================

-- Extend role_id to support 8 role types
ALTER TABLE UsersTelegaBot
MODIFY COLUMN role_id TINYINT UNSIGNED NOT NULL DEFAULT 1
    COMMENT '1=Оператор,2=Администратор,3=Маркетолог,4=ЗавРег,5=СТАдмин,6=Руководство,7=SuperAdmin,8=Dev';

-- ============================================================================
-- Part 2: Operator Linking Fields
-- ============================================================================

-- Add fields to link Telegram user with operator from call_scores
ALTER TABLE UsersTelegaBot
ADD COLUMN operator_name VARCHAR(255) NULL 
    COMMENT 'Имя оператора из call_scores (called_info/caller_info)',
ADD COLUMN extension VARCHAR(50) NULL 
    COMMENT 'Extension оператора для поиска звонков';

-- Create indexes for operator lookups
CREATE INDEX idx_users_operator_name ON UsersTelegaBot(operator_name);
CREATE INDEX idx_users_extension ON UsersTelegaBot(extension);

-- ============================================================================
-- Part 3: Role Reference Table (for documentation)
-- ============================================================================

CREATE TABLE IF NOT EXISTS roles_reference (
    role_id TINYINT UNSIGNED PRIMARY KEY,
    role_name VARCHAR(50) NOT NULL,
    can_view_own_stats BOOLEAN DEFAULT TRUE COMMENT 'Может видеть свою статистику',
    can_view_all_stats BOOLEAN DEFAULT FALSE COMMENT 'Может видеть статистику всех',
    can_view_dashboard BOOLEAN DEFAULT TRUE COMMENT 'Доступ к dashboard',
    can_generate_reports BOOLEAN DEFAULT TRUE COMMENT 'Может генерировать отчеты',
    can_view_transcripts BOOLEAN DEFAULT TRUE COMMENT 'Доступ к расшифровкам',
    can_manage_users BOOLEAN DEFAULT FALSE COMMENT 'Управление пользователями',
    can_debug BOOLEAN DEFAULT FALSE COMMENT 'Команды отладки',
    description TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT='Справочник ролей и их возможностей';

-- Insert role definitions
INSERT INTO roles_reference (role_id, role_name, can_view_own_stats, can_view_all_stats, can_view_dashboard, can_generate_reports, can_view_transcripts, can_manage_users, can_debug, description) VALUES
(1, 'Оператор', TRUE, FALSE, TRUE, TRUE, TRUE, FALSE, FALSE, 'Видит только свою статистику и дашборд'),
(2, 'Администратор', TRUE, FALSE, TRUE, TRUE, TRUE, TRUE, FALSE, 'Видит свою статистику + управление пользователями'),
(3, 'Маркетолог', FALSE, TRUE, TRUE, TRUE, TRUE, FALSE, FALSE, 'Видит всю статистику, без команд отладки'),
(4, 'ЗавРег', FALSE, TRUE, TRUE, TRUE, TRUE, FALSE, FALSE, 'Видит всю статистику, без команд отладки'),
(5, 'СТ Админ', FALSE, TRUE, TRUE, TRUE, TRUE, TRUE, FALSE, 'Видит всю статистику кроме команд отладки'),
(6, 'Руководство', FALSE, TRUE, TRUE, TRUE, TRUE, TRUE, FALSE, 'Все привилегии кроме отладки'),
(7, 'SuperAdmin', FALSE, TRUE, TRUE, TRUE, TRUE, TRUE, FALSE, 'Все привилегии (через SUPREME_ADMIN_IDS)'),
(8, 'Dev', FALSE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, 'Все привилегии + команды отладки (через DEV_ADMIN_IDS)');

-- ============================================================================
-- Part 4: Update existing users (if needed)
-- ============================================================================

-- Set default approved status for existing users
UPDATE UsersTelegaBot 
SET status = 'approved' 
WHERE status IS NULL OR status = 'pending';

-- ============================================================================
-- Part 5: Verification Queries
-- ============================================================================

-- Show updated role_id column
SELECT 
    COLUMN_NAME, 
    COLUMN_TYPE, 
    COLUMN_COMMENT 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = DATABASE() 
  AND TABLE_NAME = 'UsersTelegaBot' 
  AND COLUMN_NAME IN ('role_id', 'operator_name', 'extension');

-- Show role definitions
SELECT * FROM roles_reference ORDER BY role_id;
