-- Migration 000: Base Schema
-- Базовая схема всех таблиц проекта OperaBot
-- ВАЖНО: Пользователи бота хранятся в UsersTelegaBot, операторы Mango в users

-- =============================================================================
-- 1. ТАБЛИЦА РОЛЕЙ (справочник)
-- =============================================================================
CREATE TABLE IF NOT EXISTS roles_reference (
    id TINYINT UNSIGNED PRIMARY KEY,
    role_name VARCHAR(50) NOT NULL UNIQUE,
    description VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT IGNORE INTO roles_reference (id, role_name, description) VALUES
    (1, 'operator', 'Оператор - базовый пользователь'),
    (2, 'admin', 'Администратор - расширенные права'),
    (3, 'superadmin', 'Супер-администратор - полные права'),
    (4, 'dev', 'Разработчик - отладка');

-- =============================================================================
-- 2. ПОЛЬЗОВАТЕЛИ TELEGRAM БОТА (UsersTelegaBot)
-- =============================================================================
CREATE TABLE IF NOT EXISTS UsersTelegaBot (
    id INT AUTO_INCREMENT PRIMARY KEY,
    telegram_id BIGINT NOT NULL UNIQUE COMMENT 'Telegram user ID',
    username VARCHAR(255) NULL COMMENT 'Telegram @username',
    first_name VARCHAR(255) NULL,
    last_name VARCHAR(255) NULL,
    
    -- Роль и статус
    role_id TINYINT UNSIGNED NOT NULL DEFAULT 1 COMMENT '1=operator,2=admin,3=superadmin,4=dev',
    status ENUM('pending', 'approved', 'blocked') NOT NULL DEFAULT 'pending',
    
    -- Связи
    approved_by INT NULL COMMENT 'ID админа, одобрившего пользователя',
    operator_id INT NULL COMMENT 'Связь с users.id (оператор Mango)',
    
    -- Метаданные
    blocked_at TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- Индексы
    INDEX idx_telegram_id (telegram_id),
    INDEX idx_role_id (role_id),
    INDEX idx_status (status),
    INDEX idx_approved_by (approved_by),
    
    CONSTRAINT fk_users_telega_role 
        FOREIGN KEY (role_id) REFERENCES roles_reference(id),
    CONSTRAINT fk_users_telega_approved_by 
        FOREIGN KEY (approved_by) REFERENCES UsersTelegaBot(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =============================================================================
-- 3. ОПЕРАТОРЫ MANGO (users) - данные из ВАТС
-- =============================================================================
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL COMMENT 'ФИО оператора',
    extension VARCHAR(20) NULL COMMENT 'Внутренний номер',
    sip_id VARCHAR(50) NULL COMMENT 'SIP ID',
    position_id INT NULL,
    department VARCHAR(100) NULL,
    is_active TINYINT(1) DEFAULT 1,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_extension (extension),
    INDEX idx_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =============================================================================
-- 4. ИСТОРИЯ ЗВОНКОВ (call_history)
-- =============================================================================
CREATE TABLE IF NOT EXISTS call_history (
    history_id INT AUTO_INCREMENT PRIMARY KEY,
    call_id VARCHAR(100) NULL COMMENT 'Внешний ID звонка',
    context_start_time INT NULL COMMENT 'Timestamp начала',
    call_date DATETIME NULL,
    
    -- Участники
    caller_number VARCHAR(50) NULL,
    called_number VARCHAR(50) NULL,
    caller_info VARCHAR(255) NULL,
    called_info VARCHAR(255) NULL,
    
    -- Метрики
    call_type VARCHAR(50) NULL COMMENT 'incoming/outgoing/internal',
    talk_duration INT DEFAULT 0 COMMENT 'Длительность разговора (сек)',
    await_sec INT DEFAULT 0 COMMENT 'Время ожидания (сек)',
    
    -- Аудио
    records_url TEXT NULL,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_context_start_time (context_start_time),
    INDEX idx_call_date (call_date),
    INDEX idx_caller_number (caller_number),
    INDEX idx_called_number (called_number)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =============================================================================
-- 5. ОЦЕНКИ ЗВОНКОВ (call_scores)
-- =============================================================================
CREATE TABLE IF NOT EXISTS call_scores (
    id INT AUTO_INCREMENT PRIMARY KEY,
    history_id INT NOT NULL,
    operator_name VARCHAR(255) NULL,
    
    -- Оценка
    call_score DECIMAL(4,2) NULL COMMENT 'Оценка качества 0-10',
    call_category VARCHAR(100) NULL COMMENT 'Категория звонка',
    outcome VARCHAR(50) NULL COMMENT 'record/lead_no_record/refusal/etc',
    
    -- Детали
    is_target TINYINT(1) DEFAULT 0 COMMENT 'Целевой звонок?',
    requested_service_name VARCHAR(255) NULL,
    refusal_reason VARCHAR(255) NULL,
    number_checklist INT DEFAULT 0 COMMENT 'Кол-во пунктов чек-листа',
    
    -- Транскрипт
    transcript TEXT NULL,
    summary TEXT NULL,
    
    -- ML поля (см. Migration 003)
    ml_p_record DECIMAL(5,4) NULL COMMENT 'Вероятность записи 0-1',
    ml_score_pred DECIMAL(4,2) NULL COMMENT 'Предиктор оценки',
    ml_p_complaint DECIMAL(5,4) NULL COMMENT 'Вероятность жалобы 0-1',
    ml_updated_at DATETIME NULL,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_history_id (history_id),
    INDEX idx_operator_name (operator_name),
    INDEX idx_call_category (call_category),
    INDEX idx_outcome (outcome),
    
    CONSTRAINT fk_call_scores_history 
        FOREIGN KEY (history_id) REFERENCES call_history(history_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =============================================================================
-- 6. LM МЕТРИКИ (lm_value)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lm_value (
    id INT AUTO_INCREMENT PRIMARY KEY,
    history_id INT NOT NULL COMMENT 'Связь с call_history',
    call_score_id INT NULL COMMENT 'Связь с call_scores',
    
    -- Метрика
    metric_code VARCHAR(50) NOT NULL COMMENT 'Код метрики: response_speed_score, churn_risk_level, etc',
    metric_group VARCHAR(30) NOT NULL COMMENT 'operational/conversion/quality/risk/forecast/aux',
    
    -- Значения
    value_numeric DECIMAL(10,4) NULL COMMENT 'Числовое значение',
    value_label VARCHAR(100) NULL COMMENT 'Текстовое значение (high/medium/low)',
    value_json JSON NULL COMMENT 'JSON для сложных данных',
    
    -- Метаданные расчёта
    lm_version VARCHAR(20) NOT NULL DEFAULT 'lm_v2' COMMENT 'Версия модели',
    calc_method VARCHAR(20) NOT NULL DEFAULT 'rule' COMMENT 'rule/ml/hybrid',
    calc_source VARCHAR(50) NULL COMMENT 'worker_batch/realtime/backfill',
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_history_id (history_id),
    INDEX idx_call_score_id (call_score_id),
    INDEX idx_metric_code (metric_code),
    INDEX idx_metric_group (metric_group),
    INDEX idx_lm_version (lm_version),
    
    UNIQUE KEY uk_history_metric (history_id, metric_code, lm_version),
    
    CONSTRAINT fk_lm_value_history 
        FOREIGN KEY (history_id) REFERENCES call_history(history_id) ON DELETE CASCADE,
    CONSTRAINT fk_lm_value_score 
        FOREIGN KEY (call_score_id) REFERENCES call_scores(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =============================================================================
-- 7. CALL ANALYTICS (денормализованная)
-- =============================================================================
CREATE TABLE IF NOT EXISTS call_analytics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    history_id INT NOT NULL,
    call_score_id INT NULL,
    operator_name VARCHAR(255) NULL,
    
    -- Из call_history
    call_date DATETIME NULL,
    call_type VARCHAR(50) NULL,
    talk_duration INT DEFAULT 0,
    
    -- Из call_scores
    call_score DECIMAL(4,2) NULL,
    call_category VARCHAR(100) NULL,
    outcome VARCHAR(50) NULL,
    is_target TINYINT(1) DEFAULT 0,
    
    -- Агрегированные LM метрики
    response_speed_score DECIMAL(5,2) NULL,
    talk_time_efficiency DECIMAL(5,2) NULL,
    conversion_score DECIMAL(5,2) NULL,
    churn_risk_score DECIMAL(5,2) NULL,
    churn_risk_level VARCHAR(20) NULL,
    
    -- Метаданные
    synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_history_id (history_id),
    INDEX idx_operator_name (operator_name),
    INDEX idx_call_date (call_date),
    INDEX idx_outcome (outcome),
    
    CONSTRAINT fk_call_analytics_history 
        FOREIGN KEY (history_id) REFERENCES call_history(history_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =============================================================================
-- 8. ADMIN ACTION LOGS (аудит)
-- =============================================================================
CREATE TABLE IF NOT EXISTS admin_action_logs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    actor_id INT NOT NULL COMMENT 'UsersTelegaBot.id исполнителя',
    target_id INT NULL COMMENT 'UsersTelegaBot.id цели',
    
    action VARCHAR(50) NOT NULL COMMENT 'approve/decline/promote/demote/block/unblock/lookup',
    payload_json TEXT NULL COMMENT 'Дополнительные данные JSON',
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_actor (actor_id),
    INDEX idx_target (target_id),
    INDEX idx_action (action),
    INDEX idx_created (created_at),
    
    CONSTRAINT fk_admin_logs_actor 
        FOREIGN KEY (actor_id) REFERENCES UsersTelegaBot(id) ON DELETE CASCADE,
    CONSTRAINT fk_admin_logs_target 
        FOREIGN KEY (target_id) REFERENCES UsersTelegaBot(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =============================================================================
-- 9. OPERATOR DASHBOARDS (кеш дашбордов)
-- =============================================================================
CREATE TABLE IF NOT EXISTS operator_dashboards (
    operator_name VARCHAR(255) NOT NULL,
    period_type VARCHAR(50) NOT NULL COMMENT 'day/week/month',
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    
    -- Статистика звонков
    total_calls INT DEFAULT 0,
    accepted_calls INT DEFAULT 0,
    missed_calls INT DEFAULT 0,
    
    -- Конверсия
    records_count INT DEFAULT 0,
    leads_no_record INT DEFAULT 0,
    wish_to_record INT DEFAULT 0,
    conversion_rate DECIMAL(5,2) DEFAULT 0.00,
    
    -- Качество
    avg_score_all DECIMAL(4,2) DEFAULT 0.00,
    avg_score_leads DECIMAL(4,2) DEFAULT 0.00,
    avg_score_cancel DECIMAL(4,2) DEFAULT 0.00,
    
    -- Отмены
    cancel_calls INT DEFAULT 0,
    reschedule_calls INT DEFAULT 0,
    cancel_share DECIMAL(5,2) DEFAULT 0.00,
    
    -- Время
    avg_talk_all INT DEFAULT 0,
    total_talk_time INT DEFAULT 0,
    avg_talk_record INT DEFAULT 0,
    avg_talk_navigation INT DEFAULT 0,
    avg_talk_spam INT DEFAULT 0,
    
    -- Жалобы
    complaint_calls INT DEFAULT 0,
    avg_score_complaint DECIMAL(4,2) DEFAULT 0.00,
    
    -- Метаданные
    cached_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (operator_name, period_type, period_start),
    INDEX idx_cache_lookup (operator_name, period_type, period_start, cached_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =============================================================================
-- 10. OPERATOR RECOMMENDATIONS (LLM рекомендации)
-- =============================================================================
CREATE TABLE IF NOT EXISTS operator_recommendations (
    operator_name VARCHAR(255) NOT NULL,
    report_date DATE NOT NULL,
    
    recommendations TEXT NULL COMMENT 'Текст рекомендаций',
    call_samples_analyzed INT DEFAULT 0,
    
    generated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (operator_name, report_date),
    INDEX idx_recommendations_lookup (operator_name, report_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
