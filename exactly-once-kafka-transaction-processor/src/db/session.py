"""Database engine, session factory, and initialization helpers."""

import structlog
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import Session, sessionmaker

from src.db.models import Account, Base

logger = structlog.get_logger(__name__)

SEED_ACCOUNTS = [
    {"account_number": "ACC001", "balance": 10000.00, "account_type": "CHECKING", "daily_limit": 5000.00},
    {"account_number": "ACC002", "balance": 10000.00, "account_type": "SAVINGS", "daily_limit": None},
    {"account_number": "ACC003", "balance": 10000.00, "account_type": "CHECKING", "daily_limit": 3000.00},
    {"account_number": "ACC004", "balance": 0.00, "account_type": "CREDIT_CARD", "credit_limit": 5000.00, "daily_limit": 2000.00},
    {"account_number": "ACC005", "balance": 10000.00, "account_type": "SAVINGS", "daily_limit": 10000.00},
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
                daily_limit=acct_data.get("daily_limit"),
                credit_limit=acct_data.get("credit_limit"),
            )
            session.add(account)

        session.commit()
        logger.info("seed_accounts_created", count=len(SEED_ACCOUNTS))
    except Exception:
        session.rollback()
        logger.exception("seed_accounts_failed")
        raise
    finally:
        session.close()
