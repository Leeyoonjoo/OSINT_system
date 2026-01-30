CREATE TABLE IF NOT EXISTS notifications_victims (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,

  victim_id INT NOT NULL,   -- victims.id (PK)

  channel_type ENUM('telegram_group','telegram_dm') NOT NULL,
  channel_id BIGINT NOT NULL,  -- telegram chat_id는 숫자(-100...)라 BIGINT 권장

  status ENUM('pending','queued','sent','failed') NOT NULL DEFAULT 'pending',
  retry_count INT NOT NULL DEFAULT 0,

  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  sent_at DATETIME NULL,
  last_error TEXT NULL,

  UNIQUE KEY uq_victim_channel (victim_id, channel_type, channel_id),  -- 중복 방지
  KEY idx_status_created (status, created_at)
);
