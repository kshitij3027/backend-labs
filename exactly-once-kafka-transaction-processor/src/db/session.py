"""Database engine, session factory, and initialization helpers."""

import structlog
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import Session, sessionmaker

from src.db.models import Account, Base, ExchangeRate

logger = structlog.get_logger(__name__)

SEED_ACCOUNTS = [
    {"account_number": "ACC001", "balance": 10000.00, "account_type": "CHECKING", "currency": "USD", "daily_limit": 5000.00},
    {"account_number": "ACC002", "balance": 10000.00, "account_type": "SAVINGS", "currency": "EUR", "daily_limit": None},
    {"account_number": "ACC003", "balance": 10000.00, "account_type": "CHECKING", "currency": "GBP", "daily_limit": 3000.00},
    {"account_number": "ACC004", "balance": 0.00, "account_type": "CREDIT_CARD", "currency": "USD", "credit_limit": 5000.00, "daily_limit": 2000.00},
    {"account_number": "ACC005", "balance": 10000.00, "account_type": "SAVINGS", "currency": "USD", "daily_limit": 10000.00},
]

SEED_EXCHANGE_RATES = [
    {"from_currency": "USD", "to_currency": "EUR", "rate": 0.92},
    {"from_currency": "EUR", "to_currency": "USD", "rate": 1.09},
    {"from_currency": "USD", "to_currency": "GBP", "rate": 0.79},
    {"from_currency": "GBP", "to_currency": "USD", "rate": 1.27},
    {"from_currency": "EUR", "to_currency": "GBP", "rate": 0.86},
    {"from_currency": "GBP", "to_currency": "EUR", "rate": 1.16},
]


def get_engine(db_url: str):
    """Create a SQLAlchemy engine with sensible pool defaults."""
    return create_engine(
        db_url,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        pool_recycle=300,
    )


def get_session_factory(engine) -> sessionmaker:
    """Return a sessionmaker bound to the given engine."""
    return sessionmaker(bind=engine, expire_on_commit=False)


def init_db(engine, session_factory: sessionmaker) -> None:
    """Create all tables and seed accounts if they don't already exist."""
    Base.metadata.create_all(engine)
    logger.info("database_tables_created")

    session: Session = session_factory()
    try:
        existing = session.query(Account).count()
        if existing > 0:
            logger.info("seed_accounts_skipped", existing_count=existing)
            return

        for acct_data in SEED_ACCOUNTS:
            account = Account(
                account_number=acct_data["account_number"],
                balance=acct_data["balance"],
                account_type=acct_data.get("account_type", "CHECKING"),
                currency=acct_data.get("currency", "USD"),
                daily_limit=acct_data.get("daily_limit"),
                credit_limit=acct_data.get("credit_limit"),
            )
            session.add(account)

        # Seed exchange rates
        for rate_data in SEED_EXCHANGE_RATES:
            rate = ExchangeRate(
                from_currency=rate_data["from_currency"],
                to_currency=rate_data["to_currency"],
                rate=rate_data["rate"],
            )
            session.add(rate)

        session.commit()
        logger.info("seed_accounts_created", count=len(SEED_ACCOUNTS))
        logger.info("seed_exchange_rates_created", count=len(SEED_EXCHANGE_RATES))
    except Exception:
        session.rollback()
        logger.exception("seed_accounts_failed")
        raise
    finally:
        session.close()
