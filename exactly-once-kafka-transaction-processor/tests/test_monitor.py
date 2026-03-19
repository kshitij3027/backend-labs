"""Tests for TransactionMonitor.get_stats using an in-memory SQLite database."""

import datetime
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.db.models import Account, Base, Transaction
from src.monitor import TransactionMonitor


@pytest.fixture()
def session():
    """Create an in-memory SQLite database and yield a session."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    sess = factory()
    yield sess
    sess.close()
    engine.dispose()


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

STAT_KEYS = {
    "total_transactions",
    "processed_count",
    "completed_count",
    "failed_count",
    "by_type",
    "success_rate",
    "accounts",
    "recent_transactions",
    "by_account_type",
    "large_transactions",
    "compliance_summary",
}


def _add_account(session, number: str, balance: float) -> Account:
    acct = Account(account_number=number, balance=Decimal(str(balance)))
    session.add(acct)
    session.flush()
    return acct


def _add_transaction(
    session,
    txn_id: str,
    txn_type: str = "TRANSFER",
    amount: float = 100.0,
    status: str = "completed",
    processed: bool = True,
) -> Transaction:
    txn = Transaction(
        transaction_id=txn_id,
        type=txn_type,
        amount=Decimal(str(amount)),
        status=status,
        processed=processed,
        created_at=datetime.datetime.now(datetime.timezone.utc),
    )
    session.add(txn)
    session.flush()
    return txn


# ------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------


class TestGetStatsStructure:
    """get_stats must always return a dict with the expected keys."""

    def test_returns_correct_keys(self, session):
        stats = TransactionMonitor.get_stats(session)
        assert set(stats.keys()) == STAT_KEYS

    def test_types(self, session):
        stats = TransactionMonitor.get_stats(session)
        assert isinstance(stats["total_transactions"], int)
        assert isinstance(stats["processed_count"], int)
        assert isinstance(stats["completed_count"], int)
        assert isinstance(stats["failed_count"], int)
        assert isinstance(stats["by_type"], dict)
        assert isinstance(stats["success_rate"], float)
        assert isinstance(stats["accounts"], list)
        assert isinstance(stats["recent_transactions"], list)


class TestGetStatsEmptyDB:
    """All counters should be zero / empty when the DB has no data."""

    def test_empty_totals(self, session):
        stats = TransactionMonitor.get_stats(session)
        assert stats["total_transactions"] == 0
        assert stats["processed_count"] == 0
        assert stats["completed_count"] == 0
        assert stats["failed_count"] == 0

    def test_empty_by_type(self, session):
        stats = TransactionMonitor.get_stats(session)
        assert stats["by_type"] == {}

    def test_empty_success_rate(self, session):
        stats = TransactionMonitor.get_stats(session)
        assert stats["success_rate"] == 0.0

    def test_empty_accounts(self, session):
        stats = TransactionMonitor.get_stats(session)
        assert stats["accounts"] == []

    def test_empty_recent(self, session):
        stats = TransactionMonitor.get_stats(session)
        assert stats["recent_transactions"] == []


class TestGetStatsWithData:
    """Verify counts when transactions and accounts exist."""

    def test_counts(self, session):
        _add_transaction(session, "t1", "TRANSFER", status="completed", processed=True)
        _add_transaction(session, "t2", "DEPOSIT", status="completed", processed=True)
        _add_transaction(session, "t3", "WITHDRAWAL", status="failed", processed=True)
        _add_transaction(session, "t4", "TRANSFER", status="pending", processed=False)
        session.commit()

        stats = TransactionMonitor.get_stats(session)
        assert stats["total_transactions"] == 4
        assert stats["processed_count"] == 3
        assert stats["completed_count"] == 2
        assert stats["failed_count"] == 1

    def test_by_type(self, session):
        _add_transaction(session, "t1", "TRANSFER")
        _add_transaction(session, "t2", "TRANSFER")
        _add_transaction(session, "t3", "DEPOSIT")
        session.commit()

        stats = TransactionMonitor.get_stats(session)
        assert stats["by_type"] == {"TRANSFER": 2, "DEPOSIT": 1}

    def test_success_rate(self, session):
        _add_transaction(session, "t1", status="completed")
        _add_transaction(session, "t2", status="completed")
        _add_transaction(session, "t3", status="failed")
        _add_transaction(session, "t4", status="pending", processed=False)
        session.commit()

        stats = TransactionMonitor.get_stats(session)
        # 2 completed out of 4 total = 50%
        assert stats["success_rate"] == 50.0

    def test_accounts(self, session):
        _add_account(session, "ACC001", 10000.00)
        _add_account(session, "ACC002", 5000.50)
        session.commit()

        stats = TransactionMonitor.get_stats(session)
        assert len(stats["accounts"]) == 2
        assert stats["accounts"][0]["account_number"] == "ACC001"
        assert stats["accounts"][0]["balance"] == 10000.00
        assert stats["accounts"][1]["account_number"] == "ACC002"
        assert stats["accounts"][1]["balance"] == 5000.50

    def test_recent_transactions_limit(self, session):
        for i in range(15):
            _add_transaction(session, f"t{i}")
        session.commit()

        stats = TransactionMonitor.get_stats(session)
        assert len(stats["recent_transactions"]) == 10

    def test_recent_transaction_fields(self, session):
        _add_transaction(session, "t1", "DEPOSIT", amount=250.0, status="completed")
        session.commit()

        stats = TransactionMonitor.get_stats(session)
        recent = stats["recent_transactions"][0]
        assert recent["transaction_id"] == "t1"
        assert recent["type"] == "DEPOSIT"
        assert recent["amount"] == 250.0
        assert recent["status"] == "completed"
        assert "created_at" in recent
