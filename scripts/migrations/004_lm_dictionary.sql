-- Миграция 004: словари и хиты для rule-engine LM

CREATE TABLE IF NOT EXISTS lm_dictionary_terms (
    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    dict_code VARCHAR(64) NOT NULL COMMENT 'Код словаря (complaint_risk, followup_intent и т.д.)',
    term VARCHAR(255) NOT NULL COMMENT 'Корень/фраза или регулярное выражение',
    match_type ENUM('stem', 'phrase', 'regex') NOT NULL DEFAULT 'phrase' COMMENT 'Тип сопоставления',
    weight INT NOT NULL COMMENT 'Вес срабатывания (положительный)',
    is_negative BOOLEAN NOT NULL DEFAULT FALSE COMMENT 'Стоп-фраза / анти-триггер',
    is_active BOOLEAN NOT NULL DEFAULT TRUE COMMENT 'Признак активности правила',
    comment VARCHAR(255) NULL COMMENT 'Бизнес-комментарий',
    version VARCHAR(32) NOT NULL DEFAULT 'v1' COMMENT 'Версия словаря',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_lm_dict_code (dict_code),
    INDEX idx_lm_dict_active (dict_code, is_active),
    INDEX idx_lm_dict_version (version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT='Rule-engine: словари триггеров для LM';


CREATE TABLE IF NOT EXISTS lm_dictionary_hits (
    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    history_id INT UNSIGNED NOT NULL COMMENT 'call_history.history_id',
    dict_code VARCHAR(64) NOT NULL COMMENT 'Код словаря (complaint_risk и т.д.)',
    term VARCHAR(255) NOT NULL COMMENT 'Конкретный сработавший термин',
    match_type ENUM('stem', 'phrase', 'regex') NOT NULL COMMENT 'Тип сопоставления',
    weight INT NOT NULL COMMENT 'Вес правила',
    hit_count INT NOT NULL DEFAULT 1 COMMENT 'Количество вхождений',
    snippet VARCHAR(512) NULL COMMENT 'Фрагмент транскрипта вокруг совпадения',
    dict_version VARCHAR(32) NOT NULL COMMENT 'Версия словаря',
    detected_at DATETIME NOT NULL COMMENT 'Когда зафиксировано срабатывание',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_lm_hits_history (history_id),
    INDEX idx_lm_hits_dict (dict_code),
    INDEX idx_lm_hits_history_dict (history_id, dict_code),
    INDEX idx_lm_hits_version (dict_version),
    CONSTRAINT fk_lm_hits_history
        FOREIGN KEY (history_id) REFERENCES call_history(history_id)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT='Rule-engine: факты срабатывания словарных правил';
