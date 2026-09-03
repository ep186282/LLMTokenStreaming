CREATE TABLE IF NOT EXISTS generations (
    id UUID PRIMARY KEY,
    idempotency_key UUID NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (
        status IN ('running', 'completed', 'cancelled', 'failed', 'interrupted')
    ),
    model TEXT NOT NULL,
    request JSONB NOT NULL,
    final_seq INTEGER CHECK (final_seq IS NULL OR final_seq >= 0),
    finish_reason TEXT,
    usage JSONB,
    error TEXT,
    cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    CHECK (
        (status = 'running' AND final_seq IS NULL)
        OR (status <> 'running' AND final_seq IS NOT NULL)
    )
);

ALTER TABLE generations
    ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS chunks (
    generation_id UUID NOT NULL REFERENCES generations(id) ON DELETE CASCADE,
    seq INTEGER NOT NULL CHECK (seq > 0),
    event JSONB NOT NULL,
    PRIMARY KEY (generation_id, seq)
);

CREATE INDEX IF NOT EXISTS generations_running_idx
    ON generations (status)
    WHERE status = 'running';
