"""Database initialization script.

Uses synchronous SQLAlchemy (psycopg2) since this runs as a one-shot
container. Creates all tables and seeds default alert rules.
"""

import os
import sys
import time

from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import Session

# Add project root to path so we can import src modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.models import AlertRule, Base


def seed_default_rules(session: Session):
    """Seed default alert rules if none exist."""
    existing_count = session.query(AlertRule).count()
    if existing_count > 0:
        print(f"Found {existing_count} existing rules, skipping seed", flush=True)
        return

    default_rules = [
        AlertRule(
            name="auth_failure",
            pattern=r"authentication\s+failed|login\s+failed|auth\s+error",
            threshold=5,
            window_seconds=60,
            severity="high",
        ),
        AlertRule(
            name="database_error",
            pattern=r"database\s+error|connection\s+timeout|query\s+failed",
            threshold=3,
            window_seconds=120,
            severity="critical",
        ),
        AlertRule(
            name="api_error",
            pattern=r"api\s+error|endpoint\s+failed|request\s+timeout",
            threshold=5,
            window_seconds=60,
            severity="medium",
        ),
    ]

    for rule in default_rules:
        session.add(rule)
    session.commit()
    print(f"Seeded {len(default_rules)} default alert rules", flush=True)


def main():
    sync_url = os.environ.get(
        "SYNC_DATABASE_URL",
        "postgresql://alertuser:alertpass@postgres:5432/alertdb",
    )
    print(f"Connecting to database...", flush=True)

    engine = create_engine(sync_url)

    # Wait for postgres to be ready
    max_retries = 10
    for attempt in range(1, max_retries + 1):
        try:
            with engine.connect() as conn:
                from sqlalchemy import text
                conn.execute(text("SELECT 1"))
            print(f"PostgreSQL is ready (attempt {attempt})", flush=True)
            break
        except Exception as exc:
            print(
                f"Waiting for PostgreSQL (attempt {attempt}/{max_retries}): {exc}",
                flush=True,
            )
            if attempt < max_retries:
                time.sleep(2)
            else:
                print("Failed to connect to PostgreSQL after all retries", flush=True)
                sys.exit(1)

    # Create all tables
    print("Creating database tables...", flush=True)
    Base.metadata.create_all(engine)
    print("Tables created successfully", flush=True)

    # Seed default rules
    with Session(engine) as session:
        seed_default_rules(session)

    print("Database initialization complete", flush=True)


if __name__ == "__main__":
    main()
