CREATE TABLE IF NOT EXISTS requests (
    request_id VARCHAR(64) PRIMARY KEY,
    domain_id VARCHAR(64) NOT NULL DEFAULT 'default',
    request_type VARCHAR(16) NOT NULL,
    requester_id VARCHAR(64) NOT NULL,
    target_spec TEXT,
    upload_id VARCHAR(64),
    priority INTEGER NOT NULL DEFAULT 0,
    reuse_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    state VARCHAR(32) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_requests_state_priority_created
    ON requests (state, priority DESC, created_at ASC);

CREATE TABLE IF NOT EXISTS staged_uploads (
    upload_id VARCHAR(64) PRIMARY KEY,
    domain_id VARCHAR(64) NOT NULL DEFAULT 'default',
    requester_id VARCHAR(64) NOT NULL,
    s3_key TEXT NOT NULL,
    content_type TEXT,
    size_bytes BIGINT,
    state VARCHAR(32) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS artifacts (
    artifact_id VARCHAR(64) PRIMARY KEY,
    domain_id VARCHAR(64) NOT NULL DEFAULT 'default',
    request_id VARCHAR(64),
    s3_key TEXT NOT NULL,
    state VARCHAR(32) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE requests
    ADD COLUMN IF NOT EXISTS domain_id VARCHAR(64) NOT NULL DEFAULT 'default';

ALTER TABLE staged_uploads
    ADD COLUMN IF NOT EXISTS domain_id VARCHAR(64) NOT NULL DEFAULT 'default';

ALTER TABLE artifacts
    ADD COLUMN IF NOT EXISTS domain_id VARCHAR(64) NOT NULL DEFAULT 'default';

CREATE INDEX IF NOT EXISTS idx_requests_domain_state_priority_created
    ON requests (domain_id, state, priority DESC, created_at ASC);
