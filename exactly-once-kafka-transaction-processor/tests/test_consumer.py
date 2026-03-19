"""Tests for the exactly-once consumer with CTP pattern."""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.config import Settings
from src.db.models import Account, Base, Transaction
from src.models import TransactionMessage, TransactionType


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def db_session():
    """Create an in-memory SQLite database with seeded accounts."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, expire_on_commit=False)
    session = Session()

    # Seed accounts
    for acct_num in ["ACC001", "ACC002", "ACC003", "ACC004", "ACC005"]:
        session.add(Account(account_number=acct_num, balance=Decimal("10000.00")))
    session.commit()

    yield session
    session.close()


@pytest.fixture
def consumer_instance():
    """Create an ExactlyOnceConsumer with fully mocked Kafka components."""
    with (
        patch("src.consumer.Consumer") as mock_consumer_cls,
        patch("src.consumer.Producer") as mock_producer_cls,
        patch("src.consumer.get_engine") as mock_get_engine,
        patch("src.consumer.get_session_factory") as mock_get_sf,
    ):
        mock_consumer_cls.return_value = MagicMock()
        mock_producer_cls.return_value = MagicMock()
        mock_get_engine.return_value = MagicMock()
        mock_get_sf.return_value = MagicMock()

        from src.consumer import ExactlyOnceConsumer

        config = Settings()
        instance = ExactlyOnceConsumer(config)
        yield instance, mock_consumer_cls, mock_producer_cls


# ---------------------------------------------------------------------------
# Configuration tests
# ---------------------------------------------------------------------------


class TestConsumerConfig:
    """Verify consumer and CTP producer are configured for exactly-once."""

    @patch("src.consumer.get_session_factory")
    @patch("src.consumer.get_engine")
    @patch("src.consumer.Producer")
    @patch("src.consumer.Consumer")
    def test_consumer_auto_commit_disabled(
        self, mock_consumer_cls, mock_producer_cls, mock_engine, mock_sf
    ):
        mock_consumer_cls.return_value = MagicMock()
        mock_producer_cls.return_value = MagicMock()

        from src.consumer import ExactlyOnceConsumer

        ExactlyOnceConsumer(Settings())
        consumer_config = mock_consumer_cls.call_args[0][0]
        assert consumer_config["enable.auto.commit"] is False

    @patch("src.consumer.get_session_factory")
    @patch("src.consumer.get_engine")
    @patch("src.consumer.Producer")
    @patch("src.consumer.Consumer")
    def test_consumer_isolation_level_read_committed(
        self, mock_consumer_cls, mock_producer_cls, mock_engine, mock_sf
    ):
        mock_consumer_cls.return_value = MagicMock()
        mock_producer_cls.return_value = MagicMock()

        from src.consumer import ExactlyOnceConsumer

        ExactlyOnceConsumer(Settings())
        consumer_config = mock_consumer_cls.call_args[0][0]
        assert consumer_config["isolation.level"] == "read_committed"

    @patch("src.consumer.get_session_factory")
    @patch("src.consumer.get_engine")
    @patch("src.consumer.Producer")
    @patch("src.consumer.Consumer")
    def test_ctp_producer_idempotence_enabled(
        self, mock_consumer_cls, mock_producer_cls, mock_engine, mock_sf
    ):
        mock_consumer_cls.return_value = MagicMock()
        mock_producer_cls.return_value = MagicMock()

        from src.consumer import ExactlyOnceConsumer

        ExactlyOnceConsumer(Settings())
        producer_config = mock_producer_cls.call_args[0][0]
        assert producer_config["enable.idempotence"] is True
        assert producer_config["acks"] == "all"


# ---------------------------------------------------------------------------
# Transaction processing tests
# ---------------------------------------------------------------------------


class TestProcessTransaction:
    """Test business logic in process_transaction using real SQLite."""

    def _make_consumer(self, session_factory):
        """Helper: build a consumer with mocked Kafka but real session factory."""
        with (
            patch("src.consumer.Consumer") as mc,
            patch("src.consumer.Producer") as mp,
            patch("src.consumer.get_engine"),
            patch("src.consumer.get_session_factory") as mock_sf,
        ):
            mc.return_value = MagicMock()
            mp.return_value = MagicMock()
            mock_sf.return_value = session_factory

            from src.consumer import ExactlyOnceConsumer

            return ExactlyOnceConsumer(Settings())

    def test_duplicate_transaction_detected(self, db_session):
        """A transaction_id that already exists with processed=True is skipped."""
        txn_id = "dup-txn-001"
        db_session.add(
            Transaction(
                transaction_id=txn_id,
                type="DEPOSIT",
                to_account="ACC001",
                amount=Decimal("100.00"),
                status="completed",
                processed=True,
            )
        )
        db_session.commit()

        consumer = self._make_consumer(
            sessionmaker(bind=db_session.get_bind(), expire_on_commit=False)
        )
        msg = TransactionMessage(
            transaction_id=txn_id,
            type=TransactionType.DEPOSIT,
            to_account="ACC001",
            amount=Decimal("100.00"),
        )

        status, error = consumer.process_transaction(db_session, msg)
        assert status == "duplicate"
        assert error is None

    def test_transfer_moves_funds(self, db_session):
        """TRANSFER deducts from source and credits destination."""
        consumer = self._make_consumer(
            sessionmaker(bind=db_session.get_bind(), expire_on_commit=False)
        )
        msg = TransactionMessage(
            transaction_id="txn-transfer-001",
            type=TransactionType.TRANSFER,
            from_account="ACC001",
            to_account="ACC002",
            amount=Decimal("500.00"),
        )

        status, error = consumer.process_transaction(db_session, msg)
        assert status == "completed"
        assert error is None

        from_acct = (
            db_session.query(Account).filter_by(account_number="ACC001").first()
        )
        to_acct = (
            db_session.query(Account).filter_by(account_number="ACC002").first()
        )
        assert from_acct.balance == Decimal("9500.00")
        assert to_acct.balance == Decimal("10500.00")

    def test_transfer_insufficient_funds(self, db_session):
        """TRANSFER with insufficient balance returns failed."""
        consumer = self._make_consumer(
            sessionmaker(bind=db_session.get_bind(), expire_on_commit=False)
        )
        msg = TransactionMessage(
            transaction_id="txn-transfer-nsf",
            type=TransactionType.TRANSFER,
            from_account="ACC001",
            to_account="ACC002",
            amount=Decimal("99999.00"),
        )

        status, error = consumer.process_transaction(db_session, msg)
        assert status == "failed"
        assert error == "insufficient funds"

        # Balance unchanged
        from_acct = (
            db_session.query(Account).filter_by(account_number="ACC001").first()
        )
        assert from_acct.balance == Decimal("10000.00")

    def test_deposit_adds_to_account(self, db_session):
        """DEPOSIT increases the target account balance."""
        consumer = self._make_consumer(
            sessionmaker(bind=db_session.get_bind(), expire_on_commit=False)
        )
        msg = TransactionMessage(
            transaction_id="txn-deposit-001",
            type=TransactionType.DEPOSIT,
            to_account="ACC003",
            amount=Decimal("2500.00"),
        )

        status, error = consumer.process_transaction(db_session, msg)
        assert status == "completed"
        assert error is None

        acct = db_session.query(Account).filter_by(account_number="ACC003").first()
        assert acct.balance == Decimal("12500.00")

    def test_withdrawal_deducts_from_account(self, db_session):
        """WITHDRAWAL decreases the source account balance."""
        consumer = self._make_consumer(
            sessionmaker(bind=db_session.get_bind(), expire_on_commit=False)
        )
        msg = TransactionMessage(
            transaction_id="txn-withdraw-001",
            type=TransactionType.WITHDRAWAL,
            from_account="ACC004",
            amount=Decimal("3000.00"),
        )

        status, error = consumer.process_transaction(db_session, msg)
        assert status == "completed"
        assert error is None

        acct = db_session.query(Account).filter_by(account_number="ACC004").first()
        assert acct.balance == Decimal("7000.00")

    def test_withdrawal_insufficient_funds(self, db_session):
        """WITHDRAWAL with insufficient balance returns failed."""
        consumer = self._make_consumer(
            sessionmaker(bind=db_session.get_bind(), expire_on_commit=False)
        )
        msg = TransactionMessage(
            transaction_id="txn-withdraw-nsf",
            type=TransactionType.WITHDRAWAL,
            from_account="ACC005",
            amount=Decimal("50000.00"),
        )

        status, error = consumer.process_transaction(db_session, msg)
        assert status == "failed"
        assert error == "insufficient funds"

        acct = db_session.query(Account).filter_by(account_number="ACC005").first()
        assert acct.balance == Decimal("10000.00")
