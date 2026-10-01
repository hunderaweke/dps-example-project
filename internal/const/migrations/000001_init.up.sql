CREATE TABLE IF NOT EXISTS examples (
    id          UUID PRIMARY KEY,
    name        TEXT        NOT NULL,
    description TEXT        NOT NULL DEFAULT '',
    owner_id    TEXT        NOT NULL,
    status      TEXT        NOT NULL DEFAULT 'pending',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_examples_owner_id ON examples (owner_id);
CREATE INDEX IF NOT EXISTS idx_examples_created_at ON examples (created_at DESC);
