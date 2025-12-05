-- phpMyAdmin SQL Dump
-- version 5.2.1
-- https://www.phpmyadmin.net/
--
-- Хост: 82.97.254.49
-- Время создания: Дек 05 2025 г., 10:10
-- Версия сервера: 8.0.22-13
-- Версия PHP: 8.1.2-1ubuntu2.22

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- База данных: `mangoapi_db`
--

-- --------------------------------------------------------

--
-- Структура таблицы `admin_action_logs`
--

CREATE TABLE `admin_action_logs` (
  `id` int NOT NULL,
  `actor_id` int NOT NULL COMMENT 'User ID who performed the action',
  `target_id` int DEFAULT NULL COMMENT 'User ID affected by the action (NULL for system actions)',
  `action` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'approve, block, unblock, promote, demote, etc.',
  `payload_json` text COLLATE utf8mb4_unicode_ci COMMENT 'Additional JSON data',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'When action was performed'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Audit trail for all admin actions';

-- --------------------------------------------------------

--
-- Структура таблицы `call_analysis`
--

CREATE TABLE `call_analysis` (
  `id` bigint UNSIGNED NOT NULL,
  `history_id` int NOT NULL,
  `call_date` datetime DEFAULT NULL,
  `call_type` varchar(50) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `talk_duration` smallint UNSIGNED DEFAULT NULL COMMENT 'длительность, сек',
  `call_success` varchar(50) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `caller_info` varchar(255) COLLATE utf8mb4_general_ci NOT NULL,
  `called_info` varchar(255) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `call_category` text COLLATE utf8mb4_general_ci NOT NULL,
  `number_category` int NOT NULL,
  `category_checklist` text COLLATE utf8mb4_general_ci,
  `number_checklist` int DEFAULT NULL,
  `transcript` longtext COLLATE utf8mb4_general_ci,
  `result` longtext COLLATE utf8mb4_general_ci,
  `greeting_score` tinyint UNSIGNED NOT NULL,
  `name_usage_score` tinyint UNSIGNED NOT NULL,
  `active_listening_score` tinyint UNSIGNED NOT NULL,
  `speech_clarity_score` tinyint UNSIGNED NOT NULL,
  `need_identification_score` tinyint UNSIGNED NOT NULL,
  `knowledge_services_score` tinyint UNSIGNED NOT NULL,
  `price_address_score` tinyint UNSIGNED NOT NULL,
  `presentation_score` tinyint UNSIGNED NOT NULL,
  `conversation_mgmt_score` tinyint UNSIGNED NOT NULL,
  `alternatives_score` tinyint UNSIGNED NOT NULL,
  `hidden_objection_score` tinyint UNSIGNED NOT NULL,
  `objection_handling_score` tinyint UNSIGNED NOT NULL,
  `contact_followup_score` tinyint UNSIGNED NOT NULL,
  `summary_score` tinyint UNSIGNED NOT NULL,
  `motivation_score` tinyint UNSIGNED NOT NULL,
  `next_step_score` tinyint UNSIGNED NOT NULL,
  `overall_score` decimal(5,2) GENERATED ALWAYS AS (round(((((((((((((((((`greeting_score` + `name_usage_score`) + `active_listening_score`) + `speech_clarity_score`) + `need_identification_score`) + `knowledge_services_score`) + `price_address_score`) + `presentation_score`) + `conversation_mgmt_score`) + `alternatives_score`) + `hidden_objection_score`) + `objection_handling_score`) + `contact_followup_score`) + `summary_score`) + `motivation_score`) + `next_step_score`) / 16),2)) STORED,
  `call_score` json DEFAULT NULL,
  `prompt_tokens` int UNSIGNED DEFAULT NULL,
  `completion_tokens` int UNSIGNED DEFAULT NULL,
  `latency_ms` int UNSIGNED DEFAULT NULL,
  `cost_usd` decimal(7,4) DEFAULT NULL,
  `score_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `processed_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Структура таблицы `call_analytics`
--

CREATE TABLE `call_analytics` (
  `id` int UNSIGNED NOT NULL,
  `call_scores_id` int NOT NULL COMMENT 'PK из call_scores.id',
  `history_id` int UNSIGNED NOT NULL COMMENT 'history_id из call_scores для связи с call_history',
  `call_date` datetime NOT NULL COMMENT 'Дата звонка (копия из call_scores)',
  `call_type` varchar(50) DEFAULT NULL,
  `operator_name` varchar(255) DEFAULT NULL COMMENT 'Имя оператора',
  `operator_extension` varchar(50) DEFAULT NULL COMMENT 'Extension оператора',
  `is_target` tinyint(1) NOT NULL DEFAULT '0',
  `response_speed_score` decimal(5,2) DEFAULT NULL,
  `talk_time_efficiency` decimal(5,2) DEFAULT NULL,
  `conversion_score` decimal(5,2) DEFAULT NULL,
  `churn_risk_score` decimal(5,2) DEFAULT NULL,
  `churn_risk_level` varchar(20) DEFAULT NULL,
  `synced_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `outcome` varchar(32) DEFAULT NULL,
  `call_category` mediumtext,
  `call_score` float DEFAULT NULL,
  `talk_duration` smallint UNSIGNED DEFAULT NULL,
  `ml_p_record` decimal(5,4) DEFAULT NULL,
  `ml_score_pred` decimal(4,2) DEFAULT NULL,
  `ml_p_complaint` decimal(5,4) DEFAULT NULL,
  `ml_updated_at` timestamp NULL DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Структура таблицы `call_history`
--

CREATE TABLE `call_history` (
  `history_id` int UNSIGNED NOT NULL COMMENT 'pk',
  `context_type` tinyint(1) DEFAULT NULL COMMENT 'Направление звонка',
  `caller_number` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `caller_info` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Описание',
  `called_number` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `called_info` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Описание',
  `talk_duration` smallint UNSIGNED DEFAULT NULL COMMENT 'Время разговора',
  `await_sec` smallint UNSIGNED DEFAULT NULL COMMENT 'Ожидание (сек)',
  `context_start_time` int UNSIGNED DEFAULT NULL COMMENT 'Timestamp звонка',
  `context_status` tinyint(1) DEFAULT NULL COMMENT 'Статус',
  `recall_status` int UNSIGNED DEFAULT NULL COMMENT 'Перезвонов',
  `utm_source_by_number` mediumtext COLLATE utf8mb4_unicode_ci COMMENT 'Источник звонка',
  `categories` mediumtext COLLATE utf8mb4_unicode_ci COMMENT 'Категории',
  `transcript` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  `entry_id` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `caller_id` int UNSIGNED DEFAULT NULL COMMENT 'ID звонящего (mango)',
  `recording_id` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'ID записи (mango)',
  `phrases` json DEFAULT NULL COMMENT 'Массив фраз',
  `uploaded_at` datetime DEFAULT NULL COMMENT 'Дата выгрузки',
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `downloaded_at` datetime DEFAULT NULL COMMENT 'Загрузка аудио',
  `processed` tinyint DEFAULT NULL,
  `context_start_time_dt` datetime DEFAULT NULL,
  `transcription_status` enum('pending','running','done','failed') COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'pending',
  `transcription_worker` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `transcription_error` mediumtext COLLATE utf8mb4_unicode_ci,
  `error_class` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `error_message_short` varchar(500) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `error_http_status` int DEFAULT NULL,
  `transcribed_at` datetime DEFAULT NULL,
  `audio_path` mediumtext COLLATE utf8mb4_unicode_ci,
  `duration` smallint UNSIGNED DEFAULT NULL,
  `context_start_timestamp` int DEFAULT NULL,
  `needs_meta` tinyint DEFAULT NULL,
  `meta_source` enum('queries','stats','file-only','unknown') COLLATE utf8mb4_unicode_ci DEFAULT 'unknown',
  `meta_last_checked_at` datetime DEFAULT NULL,
  `answered_extension` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `answered_user_id` int DEFAULT NULL,
  `needs_enrichment` tinyint DEFAULT NULL,
  `download_attempts` int DEFAULT NULL,
  `download_error` mediumtext COLLATE utf8mb4_unicode_ci,
  `file_size` bigint DEFAULT NULL,
  `download_started_at` datetime DEFAULT NULL,
  `download_finished_at` datetime DEFAULT NULL,
  `download_retry_at` datetime DEFAULT NULL,
  `transcription_started_at` datetime DEFAULT NULL,
  `transcription_finished_at` datetime DEFAULT NULL,
  `transcription_attempts` int DEFAULT NULL,
  `context_ts` datetime DEFAULT NULL,
  `calc_cts` datetime DEFAULT NULL,
  `audio_size` bigint DEFAULT NULL,
  `downloaded` tinyint(1) NOT NULL DEFAULT '0',
  `file_size_mango` bigint DEFAULT NULL,
  `stt_status` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'pending',
  `stt_attempts` int NOT NULL DEFAULT '0',
  `stt_updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `has_audio` tinyint(1) NOT NULL DEFAULT '0'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Структура таблицы `call_scores`
--

CREATE TABLE `call_scores` (
  `id` int NOT NULL,
  `history_id` int UNSIGNED NOT NULL,
  `call_score` float NOT NULL,
  `score_date` datetime NOT NULL,
  `called_info` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
  `call_date` datetime DEFAULT NULL,
  `call_type` varchar(50) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `context_type` varchar(255) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `talk_duration` smallint UNSIGNED DEFAULT NULL COMMENT 'Время разговора',
  `call_success` varchar(50) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `transcript` mediumtext COLLATE utf8mb4_general_ci,
  `result` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
  `caller_info` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `caller_number` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `called_number` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `utm_source_by_number` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  `call_category` mediumtext COLLATE utf8mb4_general_ci NOT NULL,
  `number_category` int NOT NULL,
  `number_checklist` int DEFAULT NULL,
  `category_checklist` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
  `is_target` tinyint(1) NOT NULL DEFAULT '0' COMMENT '1 — целевой звонок, 0 — нецелевой',
  `outcome` varchar(32) COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT 'Итог звонка в нормализованном виде: record / lead_no_record / info_only / non_target',
  `requested_service_id` int UNSIGNED DEFAULT NULL COMMENT 'Ссылка на services.service_id (если нашли услугу в справочнике)',
  `requested_service_name` varchar(255) COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT 'Нормализованное название услуги (как в services.name, для людей)',
  `requested_doctor_id` int UNSIGNED DEFAULT NULL COMMENT 'Ссылка на doctors.doctor_id (если нашли врача в справочнике)',
  `requested_doctor_name` varchar(255) COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT 'ФИО врача, которого просил пациент (как в расшифровке)',
  `requested_doctor_speciality` varchar(255) COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT 'Специальность врача (невролог, терапевт и т.д.)',
  `refusal_reason` varchar(255) COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT 'Причина отказа, если исход не запись',
  `ml_p_record` decimal(5,4) DEFAULT NULL COMMENT 'Probability of record (0-1)',
  `ml_score_pred` decimal(4,2) DEFAULT NULL COMMENT 'Predicted quality score (0-10)',
  `ml_p_complaint` decimal(5,4) DEFAULT NULL COMMENT 'Probability of complaint (0-1)',
  `ml_updated_at` datetime DEFAULT NULL COMMENT 'When ML metrics were last updated'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Структура таблицы `company_employee_login`
--

CREATE TABLE `company_employee_login` (
  `id` int NOT NULL,
  `company_name` varchar(100) NOT NULL,
  `employee_name` varchar(100) NOT NULL,
  `login` varchar(50) NOT NULL,
  `password` varchar(50) NOT NULL,
  `license_expiry_date` date NOT NULL,
  `license_key` varchar(100) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Структура таблицы `operator_dashboards`
--

CREATE TABLE `operator_dashboards` (
  `id` int NOT NULL,
  `operator_name` varchar(255) NOT NULL,
  `period_type` enum('day','week','month') NOT NULL,
  `period_start` date NOT NULL,
  `period_end` date NOT NULL,
  `total_calls` int DEFAULT '0' COMMENT 'Всего звонков за период',
  `accepted_calls` int DEFAULT '0' COMMENT 'Принято звонков',
  `missed_calls` int DEFAULT '0' COMMENT 'Пропущено звонков',
  `records_count` int DEFAULT '0' COMMENT 'Записей на услугу',
  `leads_no_record` int DEFAULT '0' COMMENT 'Лидов без записи',
  `wish_to_record` int DEFAULT '0' COMMENT 'Желающих записаться (records + leads_no_record)',
  `conversion_rate` decimal(5,2) DEFAULT '0.00' COMMENT 'Конверсия в запись (%)',
  `avg_score_all` decimal(4,2) DEFAULT '0.00' COMMENT 'Средняя оценка всех звонков',
  `avg_score_leads` decimal(4,2) DEFAULT '0.00' COMMENT 'Средняя оценка звонков желающих записаться',
  `avg_score_cancel` decimal(4,2) DEFAULT '0.00' COMMENT 'Средняя оценка при отменах',
  `cancel_calls` int DEFAULT '0' COMMENT 'Количество отмен',
  `reschedule_calls` int DEFAULT '0' COMMENT 'Количество переносов',
  `cancel_share` decimal(5,2) DEFAULT '0.00' COMMENT 'Доля отмен (%)',
  `avg_talk_all` int DEFAULT '0' COMMENT 'Среднее время разговора (сек)',
  `total_talk_time` int DEFAULT '0' COMMENT 'Общее время разговоров (сек)',
  `avg_talk_record` int DEFAULT '0' COMMENT 'Среднее время при записи (сек)',
  `avg_talk_navigation` int DEFAULT '0' COMMENT 'Среднее время навигации (сек)',
  `avg_talk_spam` int DEFAULT '0' COMMENT 'Среднее время со спамом (сек)',
  `complaint_calls` int DEFAULT '0' COMMENT 'Звонков с жалобами',
  `avg_score_complaint` decimal(4,2) DEFAULT '0.00' COMMENT 'Средняя оценка жалоб',
  `expected_records` decimal(8,2) DEFAULT '0.00' COMMENT 'Ожидаемое число записей (ML)',
  `record_uplift` decimal(8,2) DEFAULT '0.00' COMMENT 'Переисполнение плана (ML)',
  `hot_missed_leads` int DEFAULT '0' COMMENT 'Упущенные горячие лиды (ML)',
  `difficulty_index` decimal(5,4) DEFAULT '0.0000' COMMENT 'Индекс сложности потока (ML)',
  `cached_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Когда кеш рассчитан'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Структура таблицы `operator_recommendations`
--

CREATE TABLE `operator_recommendations` (
  `id` int NOT NULL,
  `operator_name` varchar(255) NOT NULL,
  `report_date` date NOT NULL,
  `recommendations` text COMMENT 'Рекомендации по улучшению',
  `call_samples_analyzed` int DEFAULT '0' COMMENT 'Сколько звонков анализировали',
  `generated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Структура таблицы `PermissionsTelegaBot`
--

CREATE TABLE `PermissionsTelegaBot` (
  `id` int NOT NULL,
  `role_id` int DEFAULT NULL,
  `permission` varchar(255) COLLATE utf8mb4_general_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Структура таблицы `reports`
--

CREATE TABLE `reports` (
  `report_id` bigint UNSIGNED NOT NULL,
  `user_id` int DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `report_text` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  `period` varchar(20) NOT NULL,
  `report_date` varchar(50) NOT NULL,
  `total_calls` int DEFAULT '0',
  `accepted_calls` int DEFAULT '0',
  `booked_services` int DEFAULT '0',
  `conversion_rate` float DEFAULT '0',
  `avg_call_rating` float DEFAULT '0',
  `total_cancellations` int DEFAULT '0',
  `cancellation_rate` float DEFAULT '0',
  `total_conversation_time` float DEFAULT '0',
  `avg_conversation_time` float DEFAULT '0',
  `avg_spam_time` float DEFAULT '0',
  `total_spam_time` float DEFAULT '0',
  `avg_navigation_time` float DEFAULT '0',
  `complaint_calls` int DEFAULT '0',
  `complaint_rating` float DEFAULT '0',
  `recommendations` text,
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
  `missed_calls` int DEFAULT NULL,
  `missed_rate` float DEFAULT NULL,
  `total_leads` int DEFAULT NULL,
  `conversion_rate_leads` float DEFAULT NULL,
  `avg_lead_call_rating` float DEFAULT NULL,
  `avg_cancel_score` float DEFAULT NULL,
  `avg_service_time` float DEFAULT '0',
  `avg_time_spam` float DEFAULT '0',
  `avg_time_reminder` float DEFAULT '0',
  `avg_time_cancellation` float DEFAULT '0',
  `avg_time_complaints` float DEFAULT '0',
  `avg_time_reservations` float DEFAULT '0',
  `avg_time_reschedule` float DEFAULT '0'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Структура таблицы `RolesTelegaBot`
--

CREATE TABLE `RolesTelegaBot` (
  `id` int NOT NULL,
  `role_name` varchar(255) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `role_password` varchar(255) COLLATE utf8mb4_general_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Структура таблицы `roles_reference`
--

CREATE TABLE `roles_reference` (
  `role_id` tinyint UNSIGNED NOT NULL,
  `role_name` varchar(50) NOT NULL,
  `can_view_own_stats` tinyint(1) DEFAULT '1' COMMENT 'Может видеть свою статистику',
  `can_view_all_stats` tinyint(1) DEFAULT '0' COMMENT 'Может видеть статистику всех',
  `can_view_dashboard` tinyint(1) DEFAULT '1' COMMENT 'Доступ к dashboard',
  `can_generate_reports` tinyint(1) DEFAULT '1' COMMENT 'Может генерировать отчеты',
  `can_view_transcripts` tinyint(1) DEFAULT '1' COMMENT 'Доступ к расшифровкам',
  `can_manage_users` tinyint(1) DEFAULT '0' COMMENT 'Управление пользователями',
  `can_debug` tinyint(1) DEFAULT '0' COMMENT 'Команды отладки',
  `description` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Структура таблицы `users`
--

CREATE TABLE `users` (
  `user_id` int UNSIGNED NOT NULL COMMENT 'pk',
  `name` varchar(255) DEFAULT NULL COMMENT 'Имя',
  `mobile` varchar(255) DEFAULT NULL COMMENT 'Мобильный телефон',
  `login` varchar(255) DEFAULT NULL COMMENT 'Логин',
  `email` varchar(255) DEFAULT NULL COMMENT 'Email',
  `department` varchar(255) DEFAULT NULL COMMENT 'Отдел',
  `position` varchar(255) DEFAULT NULL COMMENT 'Должность',
  `extension` varchar(255) DEFAULT NULL COMMENT 'Идентификатор сотрудника ВАТС',
  `outgoingline` varchar(255) DEFAULT NULL COMMENT 'Исходящий номер',
  `number` varchar(255) DEFAULT NULL COMMENT 'mangosip',
  `protocol` varchar(255) DEFAULT NULL COMMENT 'Протокол номера',
  `order` smallint UNSIGNED DEFAULT '1' COMMENT 'Порядок использования номера',
  `wait_sec` smallint UNSIGNED DEFAULT '60' COMMENT 'Время ожидания ответа',
  `status` varchar(255) DEFAULT 'on' COMMENT 'Статус номера',
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `full_name` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Структура таблицы `UsersTelegaBot`
--

CREATE TABLE `UsersTelegaBot` (
  `id` int NOT NULL,
  `telegram_id` bigint DEFAULT NULL,
  `user_id` bigint DEFAULT NULL,
  `extension` varchar(50) COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT 'Extension оператора',
  `username` varchar(255) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `full_name` varchar(255) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `operator_name` varchar(255) COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT 'Имя оператора из call_scores (called_info/caller_info)',
  `role_id` tinyint UNSIGNED DEFAULT NULL COMMENT '1=Оператор,2=Администратор,3=Маркетолог,4=ЗавРег,5=СТАдмин,6=Руководство,7=SuperAdmin,8=Dev',
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `password` varchar(255) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `operator_id` varchar(255) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `chat_id` bigint DEFAULT NULL,
  `status` enum('pending','approved','blocked') COLLATE utf8mb4_general_ci NOT NULL DEFAULT 'pending' COMMENT 'User approval status',
  `approved_by` int DEFAULT NULL COMMENT 'User ID of admin who approved this user',
  `blocked_at` datetime DEFAULT NULL COMMENT 'When user was blocked',
  `last_active_at` datetime DEFAULT NULL COMMENT 'Последняя активность пользователя'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- Индексы таблицы `admin_action_logs`
--
ALTER TABLE `admin_action_logs`
  ADD PRIMARY KEY (`id`),
  ADD KEY `idx_actor` (`actor_id`),
  ADD KEY `idx_target` (`target_id`),
  ADD KEY `idx_action` (`action`),
  ADD KEY `idx_created` (`created_at`),
  ADD KEY `idx_action_created` (`action`,`created_at`);

--
-- Индексы таблицы `call_analysis`
--
ALTER TABLE `call_analysis`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `unique_history_id` (`history_id`),
  ADD UNIQUE KEY `uniq_history_id` (`history_id`),
  ADD KEY `idx_history` (`history_id`),
  ADD KEY `idx_call_date` (`call_date`),
  ADD KEY `idx_processed` (`processed_at`);

--
-- Индексы таблицы `call_analytics`
--
ALTER TABLE `call_analytics`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `uk_history` (`history_id`),
  ADD UNIQUE KEY `uk_call_scores_id` (`call_scores_id`),
  ADD KEY `idx_ca_operator_date` (`operator_name`,`call_date`),
  ADD KEY `idx_ca_target_outcome` (`is_target`,`outcome`,`call_date`),
  ADD KEY `idx_ca_quality` (`call_score`,`call_date`),
  ADD KEY `idx_ca_category` (`call_category`(191),`is_target`,`call_date`);

--
-- Индексы таблицы `call_history`
--
ALTER TABLE `call_history`
  ADD PRIMARY KEY (`history_id`),
  ADD UNIQUE KEY `uniq_entry_id` (`entry_id`),
  ADD UNIQUE KEY `uniq_recording_id` (`recording_id`),
  ADD KEY `idx_context_start_time` (`context_start_time`),
  ADD KEY `idx_history_context_start_time` (`history_id`,`context_start_time`),
  ADD KEY `idx_context_start_time_history_id` (`context_start_time`,`history_id`),
  ADD KEY `idx_call_history_processed` (`processed`),
  ADD KEY `idx_needs_enrichment` (`needs_enrichment`),
  ADD KEY `ix_ch_ans_user` (`answered_user_id`),
  ADD KEY `idx_call_history_enrichment_freshness` (`needs_enrichment`,`meta_last_checked_at`),
  ADD KEY `idx_call_history_needs_meta` (`needs_meta`),
  ADD KEY `ix_ch_dl` (`updated_at`),
  ADD KEY `ix_ch_tr` (`transcription_status`,`updated_at`),
  ADD KEY `ix_ch_answ_ext` (`answered_extension`),
  ADD KEY `ix_call_history_meta_last_checked_needs` (`meta_last_checked_at`,`needs_meta`,`needs_enrichment`),
  ADD KEY `ix_call_history_download_retry_at` (`download_retry_at`),
  ADD KEY `ix_call_history_download_started_at` (`download_started_at`),
  ADD KEY `ix_call_history_transcription_started_at` (`transcription_started_at`),
  ADD KEY `ix_call_history_file_size` (`file_size`),
  ADD KEY `idx_call_history_download_status` (`download_retry_at`,`context_start_time`,`updated_at`),
  ADD KEY `idx_call_history_transcribe_status` (`transcription_status`,`transcription_started_at`),
  ADD KEY `idx_call_history_meta_flags` (`needs_enrichment`,`needs_meta`),
  ADD KEY `idx_call_history_downloaded` (`downloaded`,`download_attempts`),
  ADD KEY `idx_ch_duration` (`duration`),
  ADD KEY `idx_ch_created` (`created_at`),
  ADD KEY `idx_stt_pending_audio` (`stt_status`,`has_audio`,`recording_id`);

--
-- Индексы таблицы `call_scores`
--
ALTER TABLE `call_scores`
  ADD PRIMARY KEY (`id`),
  ADD KEY `idx_call_date` (`call_date`),
  ADD KEY `idx_history_id` (`history_id`),
  ADD KEY `idx_call_date_history_id` (`call_date`,`history_id`),
  ADD KEY `idx_call_scores_target` (`is_target`,`outcome`),
  ADD KEY `idx_call_scores_service` (`requested_service_id`),
  ADD KEY `idx_call_scores_doctor` (`requested_doctor_id`);

--
-- Индексы таблицы `company_employee_login`
--
ALTER TABLE `company_employee_login`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `login` (`login`);

--
-- Индексы таблицы `operator_dashboards`
--
ALTER TABLE `operator_dashboards`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `uk_operator_period` (`operator_name`,`period_type`,`period_start`),
  ADD KEY `idx_cached_at` (`cached_at`),
  ADD KEY `idx_period_type` (`period_type`,`period_start`);

--
-- Индексы таблицы `operator_recommendations`
--
ALTER TABLE `operator_recommendations`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `uk_operator_date` (`operator_name`,`report_date`),
  ADD KEY `idx_report_date` (`report_date`),
  ADD KEY `idx_generated_at` (`generated_at`);

--
-- Индексы таблицы `PermissionsTelegaBot`
--
ALTER TABLE `PermissionsTelegaBot`
  ADD PRIMARY KEY (`id`),
  ADD KEY `role_id` (`role_id`);

--
-- Индексы таблицы `reports`
--
ALTER TABLE `reports`
  ADD PRIMARY KEY (`report_id`),
  ADD UNIQUE KEY `report_id` (`report_id`);

--
-- Индексы таблицы `RolesTelegaBot`
--
ALTER TABLE `RolesTelegaBot`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `role_name` (`role_name`);

--
-- Индексы таблицы `roles_reference`
--
ALTER TABLE `roles_reference`
  ADD PRIMARY KEY (`role_id`);

--
-- Индексы таблицы `users`
--
ALTER TABLE `users`
  ADD PRIMARY KEY (`user_id`),
  ADD UNIQUE KEY `number` (`number`),
  ADD KEY `idx_users_outgoingline` (`outgoingline`);

--
-- Индексы таблицы `UsersTelegaBot`
--
ALTER TABLE `UsersTelegaBot`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `user_id` (`user_id`),
  ADD UNIQUE KEY `uk_telegram_id` (`telegram_id`),
  ADD KEY `idx_telega_status` (`status`),
  ADD KEY `idx_telega_approved_by` (`approved_by`),
  ADD KEY `idx_users_operator_name` (`operator_name`),
  ADD KEY `idx_users_extension` (`extension`);

--
-- AUTO_INCREMENT для сохранённых таблиц
--

--
-- AUTO_INCREMENT для таблицы `admin_action_logs`
--
ALTER TABLE `admin_action_logs`
  MODIFY `id` int NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT для таблицы `call_analysis`
--
ALTER TABLE `call_analysis`
  MODIFY `id` bigint UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT для таблицы `call_analytics`
--
ALTER TABLE `call_analytics`
  MODIFY `id` int UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT для таблицы `call_history`
--
ALTER TABLE `call_history`
  MODIFY `history_id` int UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk', AUTO_INCREMENT=14079665;

--
-- AUTO_INCREMENT для таблицы `call_scores`
--
ALTER TABLE `call_scores`
  MODIFY `id` int NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=106585;

--
-- AUTO_INCREMENT для таблицы `company_employee_login`
--
ALTER TABLE `company_employee_login`
  MODIFY `id` int NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=5;

--
-- AUTO_INCREMENT для таблицы `operator_dashboards`
--
ALTER TABLE `operator_dashboards`
  MODIFY `id` int NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT для таблицы `operator_recommendations`
--
ALTER TABLE `operator_recommendations`
  MODIFY `id` int NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT для таблицы `PermissionsTelegaBot`
--
ALTER TABLE `PermissionsTelegaBot`
  MODIFY `id` int NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=127;

--
-- AUTO_INCREMENT для таблицы `reports`
--
ALTER TABLE `reports`
  MODIFY `report_id` bigint UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=2665;

--
-- AUTO_INCREMENT для таблицы `RolesTelegaBot`
--
ALTER TABLE `RolesTelegaBot`
  MODIFY `id` int NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=127;

--
-- AUTO_INCREMENT для таблицы `users`
--
ALTER TABLE `users`
  MODIFY `user_id` int UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk', AUTO_INCREMENT=742;

--
-- AUTO_INCREMENT для таблицы `UsersTelegaBot`
--
ALTER TABLE `UsersTelegaBot`
  MODIFY `id` int NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=19;

--
-- Ограничения внешнего ключа сохраненных таблиц
--

--
-- Ограничения внешнего ключа таблицы `admin_action_logs`
--
ALTER TABLE `admin_action_logs`
  ADD CONSTRAINT `fk_admin_logs_actor` FOREIGN KEY (`actor_id`) REFERENCES `UsersTelegaBot` (`id`) ON DELETE CASCADE,
  ADD CONSTRAINT `fk_admin_logs_target` FOREIGN KEY (`target_id`) REFERENCES `UsersTelegaBot` (`id`) ON DELETE CASCADE;

--
-- Ограничения внешнего ключа таблицы `PermissionsTelegaBot`
--
ALTER TABLE `PermissionsTelegaBot`
  ADD CONSTRAINT `PermissionsTelegaBot_ibfk_1` FOREIGN KEY (`role_id`) REFERENCES `RolesTelegaBot` (`id`);

--
-- Ограничения внешнего ключа таблицы `UsersTelegaBot`
--
ALTER TABLE `UsersTelegaBot`
  ADD CONSTRAINT `fk_telega_approved_by` FOREIGN KEY (`approved_by`) REFERENCES `UsersTelegaBot` (`id`) ON DELETE SET NULL;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
