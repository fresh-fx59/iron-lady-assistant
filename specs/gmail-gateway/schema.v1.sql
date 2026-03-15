PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS gateway_accounts (
    account_id TEXT PRIMARY KEY,
    gmail_email TEXT,
    status TEXT NOT NULL CHECK (status IN ('active', 'disabled', 'reauth_required')),
    auth_state TEXT NOT NULL CHECK (auth_state IN ('not_connected', 'connected', 'expired', 'revoked')),
    oauth_subject TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS gateway_oauth_tokens (
    token_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    access_token_ciphertext BLOB NOT NULL,
    refresh_token_ciphertext BLOB,
    token_type TEXT,
    scopes TEXT NOT NULL,
    expires_at TEXT,
    kms_key_version TEXT NOT NULL,
    rotation_state TEXT NOT NULL CHECK (rotation_state IN ('active', 'rotating', 'retired')),
    invalid_grant_count INTEGER NOT NULL DEFAULT 0,
    last_invalid_grant_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(account_id) REFERENCES gateway_accounts(account_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_gateway_oauth_tokens_account_id ON gateway_oauth_tokens(account_id);

CREATE TABLE IF NOT EXISTS gateway_oauth_sessions (
    session_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    redirect_url TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'completed', 'expired', 'failed')),
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    completed_at TEXT,
    FOREIGN KEY(account_id) REFERENCES gateway_accounts(account_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_gateway_oauth_sessions_account_id ON gateway_oauth_sessions(account_id);
CREATE INDEX IF NOT EXISTS idx_gateway_oauth_sessions_expires_at ON gateway_oauth_sessions(expires_at);

CREATE TABLE IF NOT EXISTS gateway_messages (
    account_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    history_id TEXT,
    subject TEXT,
    from_email TEXT,
    snippet TEXT,
    internal_ts TEXT,
    labels_json TEXT NOT NULL,
    payload_json TEXT,
    first_seen_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (account_id, message_id),
    FOREIGN KEY(account_id) REFERENCES gateway_accounts(account_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_gateway_messages_account_internal_ts ON gateway_messages(account_id, internal_ts DESC);
CREATE INDEX IF NOT EXISTS idx_gateway_messages_account_thread_id ON gateway_messages(account_id, thread_id);

CREATE TABLE IF NOT EXISTS gateway_sync_cursors (
    account_id TEXT PRIMARY KEY,
    last_history_id TEXT,
    watch_expiration_ts TEXT,
    sync_state TEXT NOT NULL CHECK (sync_state IN ('idle', 'bootstrap_running', 'delta_running', 'error')),
    last_successful_sync_at TEXT,
    last_error_code TEXT,
    last_error_message TEXT,
    stale_cursor_count INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(account_id) REFERENCES gateway_accounts(account_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS gateway_delivery_receipts (
    receipt_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    provider_message_id TEXT,
    status TEXT NOT NULL CHECK (status IN ('queued', 'sent', 'failed')),
    error_code TEXT,
    queued_at TEXT NOT NULL,
    sent_at TEXT,
    FOREIGN KEY(account_id) REFERENCES gateway_accounts(account_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_gateway_delivery_receipts_account_queued_at ON gateway_delivery_receipts(account_id, queued_at DESC);

CREATE TABLE IF NOT EXISTS gateway_idempotency_records (
    account_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    response_json TEXT,
    status_code INTEGER,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    PRIMARY KEY (account_id, operation, idempotency_key),
    FOREIGN KEY(account_id) REFERENCES gateway_accounts(account_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_gateway_idempotency_expiry ON gateway_idempotency_records(expires_at);
