-- Multi-tier caching layer — Postgres schema.
--
-- `raw_logs`               : seeded synthetic log rows. This is the SLOW source
--                            of truth that the cache fronts; C10's backend runs
--                            real GROUP BY scans over it.
-- `precomputed_aggregates` : the L3 materialized store. C10 upserts computed
--                            aggregates here (payload is a serialized blob).
--
-- Every statement is idempotent (IF NOT EXISTS) so `apply_schema` can run on
-- every container start without error.

CREATE TABLE IF NOT EXISTS raw_logs (
    id BIGSERIAL PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    level TEXT NOT NULL,
    latency_ms DOUBLE PRECISION NOT NULL,
    status_code INT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_raw_logs_source_ts ON raw_logs (source, ts);
CREATE INDEX IF NOT EXISTS idx_raw_logs_ts ON raw_logs (ts);

CREATE TABLE IF NOT EXISTS precomputed_aggregates (
    key TEXT PRIMARY KEY,
    query TEXT NOT NULL,
    params JSONB NOT NULL DEFAULT '{}'::jsonb,
    payload BYTEA NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    tags TEXT[] NOT NULL DEFAULT '{}'
);
