-- Migration Runner Script
-- Execute all migrations in order

-- Usage:
-- mysql -u username -p database_name < migrations/run_migrations.sql

-- Or run individually:
-- mysql -u username -p database_name < migrations/001_admin_roles.sql
-- mysql -u username -p database_name < migrations/002_admin_audit.sql
-- mysql -u username -p database_name < migrations/003_call_lookup_fields.sql

SOURCE 001_admin_roles.sql;
SOURCE 002_admin_audit.sql;

-- Verify migrations
SELECT 'Checking users table...' AS migration_check;
SHOW COLUMNS FROM users LIKE 'role_id';
SHOW COLUMNS FROM users LIKE 'status';

SELECT 'Checking admin_action_logs table...' AS migration_check;
SHOW TABLES LIKE 'admin_action_logs';

SELECT 'All migrations completed!' AS status;
