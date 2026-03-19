#!/usr/bin/env python3
"""Initialize database schema and seed data."""

import sys
import os

# Ensure project root is on the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.config import load_config
from src.db.session import get_engine, get_session_factory, init_db


def main() -> None:
    config = load_config()
    print(f"Connecting to database: {config.db_url}")

    engine = get_engine(config.db_url)
    session_factory = get_session_factory(engine)

    init_db(engine, session_factory)

    # Verify
    session = session_factory()
    try:
        from src.db.models import Account, ExchangeRate

        accounts = session.query(Account).all()
        print(f"\nAccounts in database ({len(accounts)}):")
        for acct in accounts:
            print(
                f"  {acct.account_number}: type={acct.account_type}, "
                f"currency={acct.currency}, balance={acct.balance}, "
                f"daily_limit={acct.daily_limit}, credit_limit={acct.credit_limit}"
            )

        rates = session.query(ExchangeRate).all()
        print(f"\nExchange rates in database ({len(rates)}):")
        for r in rates:
            print(f"  {r.from_currency} -> {r.to_currency}: {r.rate}")

        print("\nDatabase initialization complete.")
    finally:
        session.close()
        engine.dispose()


if __name__ == "__main__":
    main()
