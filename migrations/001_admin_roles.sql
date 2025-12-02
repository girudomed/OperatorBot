-- Migration 001: Add role_id/status workflow for users table
-- Implements strict role_id mapping (1=operator,2=admin,3=superadmin)

-- Add numeric role_id column (default operator)
ALTER TABLE users 
ADD COLUMN role_id TINYINT UNSIGNED NOT NULL DEFAULT 1
COMMENT 'Role identifier: 1=operator, 2=admin, 3=superadmin';

-- Add status column for approval workflow
ALTER TABLE users 
ADD COLUMN status ENUM('pending', 'approved', 'blocked') 
NOT NULL DEFAULT 'pending'
COMMENT 'User approval status';

-- Add approved_by to track approver
ALTER TABLE users 
ADD COLUMN approved_by INT NULL
COMMENT 'users.id of admin who approved this user';

-- Add blocked_at timestamp
ALTER TABLE users 
ADD COLUMN blocked_at TIMESTAMP NULL
COMMENT 'Timestamp when user was blocked';

-- Add operator_id to link with call scores/history
ALTER TABLE users 
ADD COLUMN operator_id INT NULL
COMMENT 'Link to operator data (call_scores/call_history)';

-- Backfill role_id using legacy role column if it exists
UPDATE users
SET role_id = CASE 
        WHEN role = 'admin' THEN 2
        WHEN role = 'superadmin' THEN 3
        ELSE 1
    END
WHERE role IS NOT NULL;

-- Drop legacy enum role column if present
ALTER TABLE users DROP COLUMN IF EXISTS role;

-- Add foreign key for approved_by
ALTER TABLE users 
ADD CONSTRAINT fk_users_approved_by 
FOREIGN KEY (approved_by) REFERENCES users(id) 
ON DELETE SET NULL;

-- Indexes for fast filtering
CREATE INDEX idx_users_role_id ON users(role_id);
CREATE INDEX idx_users_status ON users(status);
CREATE INDEX idx_users_approved_by ON users(approved_by);

-- Approve existing users by default
UPDATE users SET status = 'approved' WHERE status = 'pending' OR status IS NULL;
