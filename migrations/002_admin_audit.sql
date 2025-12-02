-- Migration 002: Admin action logs for audit trail
-- Tracks all admin actions (approve, promote, demote, etc.)

CREATE TABLE admin_action_logs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    actor_id INT NOT NULL COMMENT 'User ID who performed the action',
    target_id INT NULL COMMENT 'User ID affected by the action (null for system actions)',
    action VARCHAR(50) NOT NULL COMMENT 'Action type: approve, decline, promote, demote, block, unblock, lookup',
    payload_json TEXT NULL COMMENT 'Additional data in JSON format',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'When action was performed',
    
    -- Foreign keys
    CONSTRAINT fk_admin_logs_actor 
        FOREIGN KEY (actor_id) REFERENCES users(id) 
        ON DELETE CASCADE,
    
    CONSTRAINT fk_admin_logs_target 
        FOREIGN KEY (target_id) REFERENCES users(id) 
        ON DELETE CASCADE,
    
    -- Indexes for fast queries
    INDEX idx_actor (actor_id),
    INDEX idx_target (target_id),
    INDEX idx_action (action),
    INDEX idx_created (created_at),
    INDEX idx_action_created (action, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Audit trail for all admin actions';
