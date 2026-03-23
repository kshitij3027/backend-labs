"""
Synchronous database initialization script.
Runs as a one-shot container before the coordinator starts.
Creates all tables and indexes needed by the MapReduce framework.
"""

import os
import sys
import time

import psycopg2

DDL = """
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'PENDING',
    input_path TEXT NOT NULL,
    map_fn TEXT NOT NULL,
    reduce_fn TEXT NOT NULL,
    num_mappers INTEGER NOT NULL DEFAULT 2,
    num_reducers INTEGER NOT NULL DEFAULT 2,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    worker_id TEXT,
    partition_id INTEGER NOT NULL DEFAULT 0,
    retry_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS workers (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'ALIVE',
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    tasks_completed INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS results (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tasks_job_id ON tasks(job_id);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_results_job_id ON results(job_id);
CREATE INDEX IF NOT EXISTS idx_workers_status ON workers(status);
"""


def main():
    dsn = os.environ.get(
        "POSTGRES_SYNC_URL",
        "postgresql://mapreduce:mapreduce@postgres:5432/mapreduce",
    )

    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            print(f"Connecting to PostgreSQL (attempt {attempt}/{max_retries})...")
            conn = psycopg2.connect(dsn)
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(DDL)
            conn.close()
            print("Database initialized successfully.")
            return
        except psycopg2.OperationalError as e:
            print(f"Connection failed: {e}")
            if attempt < max_retries:
                time.sleep(2)
            else:
                print("Max retries reached. Exiting.")
                sys.exit(1)


if __name__ == "__main__":
    main()
