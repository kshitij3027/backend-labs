"""Tests for account types, overdraft protection, daily limits, and PAYMENT transactions."""

import uuid
from datetime import date
from decimal import Decimal
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.consumer import ExactlyOnceConsumer
from src.db.models import Account, AccountType, Base
from src.models import TransactionMessage, TransactionType


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def db_engine():
    """Create an in-memory SQLite engine."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture()
def db_session(db_engine):
    """Yield a session with no pre-seeded accounts."""
    Session = sessionmaker(bind=db_engine, expire_on_commit=False)
    session = Session()
    yield session
    session.close()


def _consumer() -> ExactlyOnceConsumer:
    """Build a consumer without initialising Kafka."""
    return object.__new__(ExactlyOnceConsumer)


def _add_account(
    session,
    number: str,
    balance: float = 10000.0,
    account_type: str = "CHECKING",
    daily_limit: float | None = None,
    credit_limit: float | None = None,
) -> Account:
    acct = Account(
        account_number=number,
        balance=Decimal(str(balance)),
        account_type=account_type,
        daily_limit=Decimal(str(daily_limit)) if daily_limit is not None else None,
        credit_limit=Decimal(str(credit_limit)) if credit_limit is not None else None,
    )
    session.add(acct)
    session.commit()
    return acct


def _txn_id() -> str:
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Overdraft protection
# ---------------------------------------------------------------------------


class TestCheckingOverdraft:
    """CHECKING accounts allow overdraft down to -500."""

    def test_overdraft_within_limit(self, db_session):
        _add_account(db_session, "CHK001", balance=100, account_type="CHECKING")
        consumer = _consumer()
        msg = TransactionMessage(
            transaction_id=_txn_id(),
            type=TransactionType.WITHDRAWAL,
            from_account="CHK001",
            amount=Decimal("500.00"),
        )
        status, err = consumer.process_transaction(db_session, msg)
        assert status == "completed"
        acct = db_session.query(Account).filter_by(account_number="CHK001").first()
        assert acct.balance == Decimal("-400.00")

    def test_overdraft_exactly_at_limit(self, db_session):
        _add_account(db_session, "CHK002", balance=0, account_type="CHECKING")
        consumer = _consumer()
        msg = TransactionMessage(
            transaction_id=_txn_id(),
            type=TransactionType.WITHDRAWAL,
            from_account="CHK002",
            amount=Decimal("500.00"),
        )
        status, err = consumer.process_transaction(db_session, msg)
        assert status == "completed"
        acct = db_session.query(Account).filter_by(account_number="CHK002").first()
        assert acct.balance == Decimal("-500.00")

    def test_overdraft_exceeds_limit(self, db_session):
        _add_account(db_session, "CHK003", balance=0, account_type="CHECKING")
        consumer = _consumer()
        msg = TransactionMessage(
            transaction_id=_txn_id(),
            type=TransactionType.WITHDRAWAL,
            from_account="CHK003",
            amount=Decimal("500.01"),
        )
        status, err = consumer.process_transaction(db_session, msg)
        assert status == "failed"
        assert err == "insufficient funds"
        acct = db_session.query(Account).filter_by(account_number="CHK003").first()
        assert acct.balance == Decimal("0")


class TestSavingsNoOverdraft:
    """SAVINGS accounts cannot go below zero."""

    def test_exact_balance_withdrawal(self, db_session):
        _add_account(db_session, "SAV001", balance=500, account_type="SAVINGS")
        consumer = _consumer()
        msg = TransactionMessage(
            transaction_id=_txn_id(),
            type=TransactionType.WITHDRAWAL,
            from_account="SAV001",
            amount=Decimal("500.00"),
        )
        status, err = consumer.process_transaction(db_session, msg)
        assert status == "completed"
        acct = db_session.query(Account).filter_by(account_number="SAV001").first()
        assert acct.balance == Decimal("0.00")

    def test_overdraft_blocked(self, db_session):
        _add_account(db_session, "SAV002", balance=500, account_type="SAVINGS")
        consumer = _consumer()
        msg = TransactionMessage(
            transaction_id=_txn_id(),
            type=TransactionType.WITHDRAWAL,
            from_account="SAV002",
            amount=Decimal("500.01"),
        )
        status, err = consumer.process_transaction(db_session, msg)
        assert status == "failed"
        assert err == "insufficient funds"
        acct = db_session.query(Account).filter_by(account_number="SAV002").first()
        assert acct.balance == Decimal("500")


class TestCreditCardLimits:
    """CREDIT_CARD accounts can go negative down to -credit_limit."""

    def test_spending_within_credit_limit(self, db_session):
        _add_account(db_session, "CC001", balance=0, account_type="CREDIT_CARD", credit_limit=5000)
        consumer = _consumer()
        msg = TransactionMessage(
            transaction_id=_txn_id(),
            type=TransactionType.WITHDRAWAL,
            from_account="CC001",
            amount=Decimal("3000.00"),
        )
        status, err = consumer.process_transaction(db_session, msg)
        assert status == "completed"
        acct = db_session.query(Account).filter_by(account_number="CC001").first()
        assert acct.balance == Decimal("-3000.00")

    def test_spending_at_credit_limit(self, db_session):
        _add_account(db_session, "CC002", balance=0, account_type="CREDIT_CARD", credit_limit=5000)
        consumer = _consumer()
        msg = TransactionMessage(
            transaction_id=_txn_id(),
            type=TransactionType.WITHDRAWAL,
            from_account="CC002",
            amount=Decimal("5000.00"),
        )
        status, err = consumer.process_transaction(db_session, msg)
        assert status == "completed"
        acct = db_session.query(Account).filter_by(account_number="CC002").first()
        assert acct.balance == Decimal("-5000.00")

    def test_spending_exceeds_credit_limit(self, db_session):
        _add_account(db_session, "CC003", balance=0, account_type="CREDIT_CARD", credit_limit=5000)
        consumer = _consumer()
        msg = TransactionMessage(
            transaction_id=_txn_id(),
            type=TransactionType.WITHDRAWAL,
            from_account="CC003",
            amount=Decimal("5000.01"),
        )
        status, err = consumer.process_transaction(db_session, msg)
        assert status == "failed"
        assert err == "insufficient funds"

    def test_credit_card_no_limit_set(self, db_session):
        """CREDIT_CARD with no credit_limit set — treated as 0 limit (no overdraft)."""
        _add_account(db_session, "CC004", balance=0, account_type="CREDIT_CARD")
        consumer = _consumer()
        msg = TransactionMessage(
            transaction_id=_txn_id(),
            type=TransactionType.WITHDRAWAL,
            from_account="CC004",
            amount=Decimal("1.00"),
        )
        status, err = consumer.process_transaction(db_session, msg)
        assert status == "failed"
        assert err == "insufficient funds"

    def test_credit_card_with_positive_balance(self, db_session):
        """CREDIT_CARD with positive balance (payment received) can spend balance + limit."""
        _add_account(db_session, "CC005", balance=1000, account_type="CREDIT_CARD", credit_limit=5000)
        consumer = _consumer()
        msg = TransactionMessage(
            transaction_id=_txn_id(),
            type=TransactionType.WITHDRAWAL,
            from_account="CC005",
            amount=Decimal("6000.00"),
        )
        status, err = consumer.process_transaction(db_session, msg)
        assert status == "completed"
        acct = db_session.query(Account).filter_by(account_number="CC005").first()
        assert acct.balance == Decimal("-5000.00")


# ---------------------------------------------------------------------------
# Daily limit enforcement
# ---------------------------------------------------------------------------


class TestDailyLimits:
    """Daily spending limit enforcement."""

    def test_within_daily_limit(self, db_session):
        _add_account(db_session, "DL001", balance=10000, account_type="CHECKING", daily_limit=5000)
        consumer = _consumer()
        msg = TransactionMessage(
            transaction_id=_txn_id(),
            type=TransactionType.WITHDRAWAL,
            from_account="DL001",
            amount=Decimal("3000.00"),
        )
        status, err = consumer.process_transaction(db_session, msg)
        assert status == "completed"

    def test_exceeds_daily_limit_single_txn(self, db_session):
        _add_account(db_session, "DL002", balance=10000, account_type="CHECKING", daily_limit=3000)
        consumer = _consumer()
        msg = TransactionMessage(
            transaction_id=_txn_id(),
            type=TransactionType.WITHDRAWAL,
            from_account="DL002",
            amount=Decimal("3001.00"),
        )
        status, err = consumer.process_transaction(db_session, msg)
        assert status == "failed"
        assert err == "daily limit exceeded"

    def test_cumulative_daily_limit(self, db_session):
        _add_account(db_session, "DL003", balance=10000, account_type="CHECKING", daily_limit=5000)
        consumer = _consumer()

        # First withdrawal: 3000 (within limit)
        msg1 = TransactionMessage(
            transaction_id=_txn_id(),
            type=TransactionType.WITHDRAWAL,
            from_account="DL003",
            amount=Decimal("3000.00"),
        )
        status, err = consumer.process_transaction(db_session, msg1)
        assert status == "completed"

        # Second withdrawal: 2001 (would exceed 5000 daily limit)
        msg2 = TransactionMessage(
            transaction_id=_txn_id(),
            type=TransactionType.WITHDRAWAL,
            from_account="DL003",
            amount=Decimal("2001.00"),
        )
        status, err = consumer.process_transaction(db_session, msg2)
        assert status == "failed"
        assert err == "daily limit exceeded"

    def test_no_daily_limit(self, db_session):
        """Account with no daily_limit (None) has no cap."""
        _add_account(db_session, "DL004", balance=10000, account_type="SAVINGS", daily_limit=None)
        consumer = _consumer()
        msg = TransactionMessage(
            transaction_id=_txn_id(),
            type=TransactionType.WITHDRAWAL,
            from_account="DL004",
            amount=Decimal("10000.00"),
        )
        status, err = consumer.process_transaction(db_session, msg)
        assert status == "completed"

    def test_daily_limit_resets_on_new_day(self, db_session):
        """Daily spent resets when the date changes."""
        _add_account(db_session, "DL005", balance=10000, account_type="CHECKING", daily_limit=3000)
        consumer = _consumer()

        # Simulate spending 3000 yesterday
        acct = db_session.query(Account).filter_by(account_number="DL005").first()
        acct.daily_spent = Decimal("3000")
        acct.daily_spent_date = date(2020, 1, 1)  # Old date
        db_session.commit()

        # Today's withdrawal should succeed because daily_spent resets
        msg = TransactionMessage(
            transaction_id=_txn_id(),
            type=TransactionType.WITHDRAWAL,
            from_account="DL005",
            amount=Decimal("2000.00"),
        )
        status, err = consumer.process_transaction(db_session, msg)
        assert status == "completed"

    def test_daily_limit_on_transfer(self, db_session):
        """Daily limit applies to the sender in transfers."""
        _add_account(db_session, "DLT01", balance=10000, account_type="CHECKING", daily_limit=1000)
        _add_account(db_session, "DLT02", balance=10000, account_type="CHECKING")
        consumer = _consumer()

        msg = TransactionMessage(
            transaction_id=_txn_id(),
            type=TransactionType.TRANSFER,
            from_account="DLT01",
            to_account="DLT02",
            amount=Decimal("1001.00"),
        )
        status, err = consumer.process_transaction(db_session, msg)
        assert status == "failed"
        assert err == "daily limit exceeded"


# ---------------------------------------------------------------------------
# PAYMENT transaction type
# ---------------------------------------------------------------------------


class TestPaymentType:
    """PAYMENT works like a withdrawal — deducts from from_account."""

    def test_payment_deducts_balance(self, db_session):
        _add_account(db_session, "PAY01", balance=5000, account_type="CHECKING")
        consumer = _consumer()
        msg = TransactionMessage(
            transaction_id=_txn_id(),
            type=TransactionType.PAYMENT,
            from_account="PAY01",
            amount=Decimal("1500.00"),
        )
        status, err = consumer.process_transaction(db_session, msg)
        assert status == "completed"
        acct = db_session.query(Account).filter_by(account_number="PAY01").first()
        assert acct.balance == Decimal("3500.00")

    def test_payment_insufficient_funds(self, db_session):
        _add_account(db_session, "PAY02", balance=100, account_type="SAVINGS")
        consumer = _consumer()
        msg = TransactionMessage(
            transaction_id=_txn_id(),
            type=TransactionType.PAYMENT,
            from_account="PAY02",
            amount=Decimal("100.01"),
        )
        status, err = consumer.process_transaction(db_session, msg)
        assert status == "failed"
        assert err == "insufficient funds"

    def test_payment_respects_daily_limit(self, db_session):
        _add_account(db_session, "PAY03", balance=10000, account_type="CHECKING", daily_limit=2000)
        consumer = _consumer()
        msg = TransactionMessage(
            transaction_id=_txn_id(),
            type=TransactionType.PAYMENT,
            from_account="PAY03",
            amount=Decimal("2001.00"),
        )
        status, err = consumer.process_transaction(db_session, msg)
        assert status == "failed"
        assert err == "daily limit exceeded"

    def test_payment_account_not_found(self, db_session):
        consumer = _consumer()
        msg = TransactionMessage(
            transaction_id=_txn_id(),
            type=TransactionType.PAYMENT,
            from_account="NONEXISTENT",
            amount=Decimal("100.00"),
        )
        status, err = consumer.process_transaction(db_session, msg)
        assert status == "failed"
        assert err == "account not found"

    def test_payment_on_credit_card(self, db_session):
        """PAYMENT on a credit card uses credit limit."""
        _add_account(db_session, "PAY04", balance=0, account_type="CREDIT_CARD", credit_limit=3000)
        consumer = _consumer()
        msg = TransactionMessage(
            transaction_id=_txn_id(),
            type=TransactionType.PAYMENT,
            from_account="PAY04",
            amount=Decimal("2000.00"),
        )
        status, err = consumer.process_transaction(db_session, msg)
        assert status == "completed"
        acct = db_session.query(Account).filter_by(account_number="PAY04").first()
        assert acct.balance == Decimal("-2000.00")


# ---------------------------------------------------------------------------
# Backwards compatibility
# ---------------------------------------------------------------------------


class TestBackwardsCompatibility:
    """Accounts without account_type should default to CHECKING behavior."""

    def test_legacy_account_allows_overdraft(self, db_session):
        """An account without account_type set should behave like CHECKING."""
        acct = Account(account_number="LEGACY01", balance=Decimal("100"))
        db_session.add(acct)
        db_session.commit()

        consumer = _consumer()
        msg = TransactionMessage(
            transaction_id=_txn_id(),
            type=TransactionType.WITHDRAWAL,
            from_account="LEGACY01",
            amount=Decimal("500.00"),
        )
        status, err = consumer.process_transaction(db_session, msg)
        assert status == "completed"
        acct = db_session.query(Account).filter_by(account_number="LEGACY01").first()
        assert acct.balance == Decimal("-400.00")
