"""Integration tests for Exactly-Once Semantics (EOS) guarantees.

Uses SQLite in-memory DB — no real Kafka needed. Tests focus on the
processing logic inside ExactlyOnceConsumer.process_transaction().
"""

import uuid
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from src.consumer import ExactlyOnceConsumer
from src.db.models import Account, Base, Transaction
from src.models import TransactionMessage, TransactionType


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

ACCOUNT_IDS = ["ACC001", "ACC002", "ACC003", "ACC004", "ACC005"]
INITIAL_BALANCE = Decimal("10000.00")
INITIAL_TOTAL = INITIAL_BALANCE * len(ACCOUNT_IDS)  # 50000.00


@pytest.fixture()
def db_session():
    """Create an in-memory SQLite database, seed 5 accounts, yield a session."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, expire_on_commit=False)
    session = Session()

    for acc_id in ACCOUNT_IDS:
        session.add(Account(account_number=acc_id, balance=INITIAL_BALANCE))
    session.commit()

    yield session

    session.close()
    engine.dispose()


def _make_consumer() -> ExactlyOnceConsumer:
    """Return an ExactlyOnceConsumer *without* initialising Kafka.

    We only need the instance so we can call process_transaction(); all
    Kafka-related attributes are left uninitialised on purpose.
    """
    consumer = object.__new__(ExactlyOnceConsumer)
    return consumer


# ---------------------------------------------------------------------------
# Test 1 — Message round-trip serialization
# ---------------------------------------------------------------------------


class TestMessageRoundtrip:
    """Serialize a TransactionMessage to Kafka bytes and back."""

    def test_message_roundtrip(self):
        original = TransactionMessage(
            transaction_id=str(uuid.uuid4()),
            type=TransactionType.TRANSFER,
            from_account="ACC001",
            to_account="ACC002",
            amount=Decimal("1234.56"),
            metadata={"source": "test"},
        )

        raw_bytes = original.to_kafka_value()
        restored = TransactionMessage.from_kafka_value(raw_bytes)

        assert restored.transaction_id == original.transaction_id
        assert restored.type == original.type
        assert restored.from_account == original.from_account
        assert restored.to_account == original.to_account
        assert restored.amount == original.amount
        assert restored.timestamp == original.timestamp
        assert restored.metadata == original.metadata
        # Decimal precision preserved
        assert str(restored.amount) == "1234.56"


# ---------------------------------------------------------------------------
# Test 2 — Idempotency / dedup
# ---------------------------------------------------------------------------


class TestIdempotencyDedup:
    """Processing the same transaction_id twice must be a no-op the second time."""

    def test_idempotency_dedup(self, db_session):
        consumer = _make_consumer()
        msg = TransactionMessage(
            transaction_id=str(uuid.uuid4()),
            type=TransactionType.DEPOSIT,
            to_account="ACC001",
            amount=Decimal("500.00"),
        )

        status1, err1 = consumer.process_transaction(db_session, msg)
        assert status1 == "completed"
        assert err1 is None

        status2, err2 = consumer.process_transaction(db_session, msg)
        assert status2 == "duplicate"
        assert err2 is None

        count = (
            db_session.query(func.count(Transaction.id))
            .filter_by(transaction_id=msg.transaction_id)
            .scalar()
        )
        assert count == 1


# ---------------------------------------------------------------------------
# Test 3 — Crash recovery: no duplicates
# ---------------------------------------------------------------------------


class TestCrashRecoveryNoDuplicates:
    """Simulate a crash (Kafka offset not committed) and re-process the same
    message. The idempotency key should prevent double-processing."""

    def test_crash_recovery_no_duplicates(self, db_session):
        consumer = _make_consumer()
        msg = TransactionMessage(
            transaction_id=str(uuid.uuid4()),
            type=TransactionType.TRANSFER,
            from_account="ACC001",
            to_account="ACC002",
            amount=Decimal("100.00"),
        )

        # First processing — succeeds
        status1, _ = consumer.process_transaction(db_session, msg)
        assert status1 == "completed"

        # Simulate crash: offset was NOT committed to Kafka, so the same
        # message is delivered again after restart.
        status2, _ = consumer.process_transaction(db_session, msg)
        assert status2 == "duplicate"

        # DB must contain exactly one record for this transaction
        records = (
            db_session.query(Transaction)
            .filter_by(transaction_id=msg.transaction_id)
            .all()
        )
        assert len(records) == 1
        assert records[0].status == "completed"


# ---------------------------------------------------------------------------
# Test 4 — Balance conservation across transfers
# ---------------------------------------------------------------------------


class TestBalanceConservation:
    """Process 20 TRANSFER transactions between accounts and verify total
    balance stays constant at 50000.00."""

    def test_balance_conservation(self, db_session):
        import random

        random.seed(42)
        consumer = _make_consumer()

        for _ in range(20):
            from_acc = random.choice(ACCOUNT_IDS)
            to_acc = random.choice([a for a in ACCOUNT_IDS if a != from_acc])
            amount = Decimal(str(round(random.uniform(10, 500), 2)))

            msg = TransactionMessage(
                transaction_id=str(uuid.uuid4()),
                type=TransactionType.TRANSFER,
                from_account=from_acc,
                to_account=to_acc,
                amount=amount,
            )
            consumer.process_transaction(db_session, msg)

        total_balance = db_session.query(func.sum(Account.balance)).scalar()
        assert total_balance == INITIAL_TOTAL, (
            f"Total balance drifted: expected {INITIAL_TOTAL}, got {total_balance}"
        )


# ---------------------------------------------------------------------------
# Test 5 — Concurrent-style mixed transactions correctness
# ---------------------------------------------------------------------------


class TestConcurrentTransactionsCorrectness:
    """Process a mix of transfers, deposits, and withdrawals. Verify:
    - No duplicate transaction records
    - All completed transactions have correct status
    - Balance = initial + deposits - withdrawals
    """

    def test_concurrent_transactions_correctness(self, db_session):
        import random

        random.seed(99)
        consumer = _make_consumer()

        deposit_total = Decimal("0")
        withdrawal_total = Decimal("0")
        sent_ids: list[str] = []

        # 10 deposits
        for _ in range(10):
            amount = Decimal(str(round(random.uniform(100, 1000), 2)))
            msg = TransactionMessage(
                transaction_id=str(uuid.uuid4()),
                type=TransactionType.DEPOSIT,
                to_account=random.choice(ACCOUNT_IDS),
                amount=amount,
            )
            sent_ids.append(msg.transaction_id)
            status, _ = consumer.process_transaction(db_session, msg)
            if status == "completed":
                deposit_total += amount

        # 5 withdrawals (small enough to succeed)
        for _ in range(5):
            amount = Decimal(str(round(random.uniform(10, 200), 2)))
            msg = TransactionMessage(
                transaction_id=str(uuid.uuid4()),
                type=TransactionType.WITHDRAWAL,
                from_account=random.choice(ACCOUNT_IDS),
                amount=amount,
            )
            sent_ids.append(msg.transaction_id)
            status, _ = consumer.process_transaction(db_session, msg)
            if status == "completed":
                withdrawal_total += amount

        # 10 transfers (net-zero effect on total balance)
        for _ in range(10):
            from_acc = random.choice(ACCOUNT_IDS)
            to_acc = random.choice([a for a in ACCOUNT_IDS if a != from_acc])
            amount = Decimal(str(round(random.uniform(10, 300), 2)))
            msg = TransactionMessage(
                transaction_id=str(uuid.uuid4()),
                type=TransactionType.TRANSFER,
                from_account=from_acc,
                to_account=to_acc,
                amount=amount,
            )
            sent_ids.append(msg.transaction_id)
            consumer.process_transaction(db_session, msg)

        # --- Assertions ---

        # 1. No duplicate transaction records
        dup_count = (
            db_session.query(Transaction.transaction_id)
            .group_by(Transaction.transaction_id)
            .having(func.count(Transaction.id) > 1)
            .count()
        )
        assert dup_count == 0, f"Found {dup_count} duplicate transaction IDs"

        # 2. All completed txns have status "completed"
        completed_rows = (
            db_session.query(Transaction)
            .filter(Transaction.status == "completed")
            .all()
        )
        for row in completed_rows:
            assert row.processed is True
            assert row.status == "completed"

        # 3. Balance = initial + deposits - withdrawals
        expected_total = INITIAL_TOTAL + deposit_total - withdrawal_total
        actual_total = db_session.query(func.sum(Account.balance)).scalar()
        assert actual_total == expected_total, (
            f"Balance mismatch: expected {expected_total}, got {actual_total}"
        )
