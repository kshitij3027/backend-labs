"""Tests for cross-currency transfer support and compliance features."""

import json
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.consumer import ExactlyOnceConsumer
from src.db.models import Account, Base, ExchangeRate, Transaction
from src.models import TransactionMessage, TransactionType


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def db_engine():
    """Create an in-memory SQLite engine with all tables."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture()
def db_session(db_engine):
    """Yield a session with multi-currency accounts and exchange rates seeded."""
    Session = sessionmaker(bind=db_engine, expire_on_commit=False)
    session = Session()

    # Seed accounts with different currencies
    accounts = [
        Account(account_number="ACC001", balance=Decimal("10000.00"), account_type="CHECKING", currency="USD"),
        Account(account_number="ACC002", balance=Decimal("10000.00"), account_type="SAVINGS", currency="EUR"),
        Account(account_number="ACC003", balance=Decimal("10000.00"), account_type="CHECKING", currency="GBP"),
        Account(account_number="ACC004", balance=Decimal("0.00"), account_type="CREDIT_CARD", currency="USD", credit_limit=Decimal("5000.00")),
        Account(account_number="ACC005", balance=Decimal("10000.00"), account_type="SAVINGS", currency="USD"),
    ]
    for a in accounts:
        session.add(a)

    # Seed exchange rates
    rates = [
        ExchangeRate(from_currency="USD", to_currency="EUR", rate=Decimal("0.920000")),
        ExchangeRate(from_currency="EUR", to_currency="USD", rate=Decimal("1.090000")),
        ExchangeRate(from_currency="USD", to_currency="GBP", rate=Decimal("0.790000")),
        ExchangeRate(from_currency="GBP", to_currency="USD", rate=Decimal("1.270000")),
        ExchangeRate(from_currency="EUR", to_currency="GBP", rate=Decimal("0.860000")),
        ExchangeRate(from_currency="GBP", to_currency="EUR", rate=Decimal("1.160000")),
    ]
    for r in rates:
        session.add(r)

    session.commit()
    yield session
    session.close()


def _consumer() -> ExactlyOnceConsumer:
    """Build a consumer without initialising Kafka."""
    return object.__new__(ExactlyOnceConsumer)


def _txn_id() -> str:
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Cross-currency transfer tests
# ---------------------------------------------------------------------------


class TestCrossCurrencyTransfer:
    """Test transfers between accounts in different currencies."""

    def test_usd_to_eur_transfer(self, db_session):
        """USD->EUR transfer uses exchange rate to convert credit amount."""
        consumer = _consumer()
        msg = TransactionMessage(
            transaction_id=_txn_id(),
            type=TransactionType.TRANSFER,
            from_account="ACC001",  # USD
            to_account="ACC002",    # EUR
            amount=Decimal("1000.00"),
        )

        status, err = consumer.process_transaction(db_session, msg)
        assert status == "completed"
        assert err is None

        from_acct = db_session.query(Account).filter_by(account_number="ACC001").first()
        to_acct = db_session.query(Account).filter_by(account_number="ACC002").first()

        # Debited in USD
        assert from_acct.balance == Decimal("9000.00")
        # Credited in EUR: 1000 * 0.92 = 920.00
        assert to_acct.balance == Decimal("10920.000000")

    def test_gbp_to_usd_transfer(self, db_session):
        """GBP->USD transfer uses exchange rate."""
        consumer = _consumer()
        msg = TransactionMessage(
            transaction_id=_txn_id(),
            type=TransactionType.TRANSFER,
            from_account="ACC003",  # GBP
            to_account="ACC005",    # USD
            amount=Decimal("500.00"),
        )

        status, err = consumer.process_transaction(db_session, msg)
        assert status == "completed"
        assert err is None

        from_acct = db_session.query(Account).filter_by(account_number="ACC003").first()
        to_acct = db_session.query(Account).filter_by(account_number="ACC005").first()

        assert from_acct.balance == Decimal("9500.00")
        # 500 * 1.27 = 635.00
        assert to_acct.balance == Decimal("10635.000000")

    def test_missing_exchange_rate_fails(self, db_session):
        """Transfer between currencies with no exchange rate should fail."""
        # Remove the EUR->GBP rate to simulate missing rate
        db_session.query(ExchangeRate).filter_by(
            from_currency="EUR", to_currency="GBP"
        ).delete()
        db_session.commit()

        consumer = _consumer()
        msg = TransactionMessage(
            transaction_id=_txn_id(),
            type=TransactionType.TRANSFER,
            from_account="ACC002",  # EUR
            to_account="ACC003",    # GBP
            amount=Decimal("500.00"),
        )

        status, err = consumer.process_transaction(db_session, msg)
        assert status == "failed"
        assert "no exchange rate found" in err

        # Balances unchanged
        from_acct = db_session.query(Account).filter_by(account_number="ACC002").first()
        to_acct = db_session.query(Account).filter_by(account_number="ACC003").first()
        assert from_acct.balance == Decimal("10000.00")
        assert to_acct.balance == Decimal("10000.00")

    def test_same_currency_transfer_unchanged(self, db_session):
        """Same-currency transfer works as before (no conversion)."""
        consumer = _consumer()
        msg = TransactionMessage(
            transaction_id=_txn_id(),
            type=TransactionType.TRANSFER,
            from_account="ACC001",  # USD
            to_account="ACC005",    # USD
            amount=Decimal("2000.00"),
        )

        status, err = consumer.process_transaction(db_session, msg)
        assert status == "completed"
        assert err is None

        from_acct = db_session.query(Account).filter_by(account_number="ACC001").first()
        to_acct = db_session.query(Account).filter_by(account_number="ACC005").first()

        assert from_acct.balance == Decimal("8000.00")
        assert to_acct.balance == Decimal("12000.00")

    def test_exchange_rate_precision(self, db_session):
        """Verify that exchange rate multiplication preserves precision."""
        consumer = _consumer()
        msg = TransactionMessage(
            transaction_id=_txn_id(),
            type=TransactionType.TRANSFER,
            from_account="ACC001",  # USD
            to_account="ACC003",    # GBP
            amount=Decimal("1234.56"),
        )

        status, err = consumer.process_transaction(db_session, msg)
        assert status == "completed"

        to_acct = db_session.query(Account).filter_by(account_number="ACC003").first()
        # 1234.56 * 0.79 = 975.3024
        expected_credit = Decimal("1234.56") * Decimal("0.790000")
        expected_balance = Decimal("10000.00") + expected_credit
        assert to_acct.balance == expected_balance

    def test_eur_to_gbp_transfer(self, db_session):
        """EUR->GBP cross-currency transfer."""
        consumer = _consumer()
        msg = TransactionMessage(
            transaction_id=_txn_id(),
            type=TransactionType.TRANSFER,
            from_account="ACC002",  # EUR
            to_account="ACC003",    # GBP
            amount=Decimal("1000.00"),
        )

        status, err = consumer.process_transaction(db_session, msg)
        assert status == "completed"

        from_acct = db_session.query(Account).filter_by(account_number="ACC002").first()
        to_acct = db_session.query(Account).filter_by(account_number="ACC003").first()

        assert from_acct.balance == Decimal("9000.00")
        # 1000 * 0.86 = 860
        assert to_acct.balance == Decimal("10860.000000")


# ---------------------------------------------------------------------------
# Compliance endpoint test (via Flask test client)
# ---------------------------------------------------------------------------


class TestComplianceEndpoint:
    """Test the /api/compliance dashboard endpoint."""

    @pytest.fixture()
    def app(self, db_engine):
        """Create a Flask test app backed by the in-memory SQLite database."""
        Session = sessionmaker(bind=db_engine, expire_on_commit=False)
        session = Session()

        # Seed accounts
        for acct_no, balance, acct_type, currency in [
            ("ACC001", "10000.00", "CHECKING", "USD"),
            ("ACC002", "10000.00", "SAVINGS", "EUR"),
            ("ACC003", "10000.00", "CHECKING", "GBP"),
            ("ACC004", "0.00", "CREDIT_CARD", "USD"),
            ("ACC005", "10000.00", "SAVINGS", "USD"),
        ]:
            session.add(Account(
                account_number=acct_no,
                balance=Decimal(balance),
                account_type=acct_type,
                currency=currency,
            ))

        # Seed exchange rates
        for fc, tc, rate in [
            ("USD", "EUR", "0.920000"),
            ("EUR", "USD", "1.090000"),
        ]:
            session.add(ExchangeRate(from_currency=fc, to_currency=tc, rate=Decimal(rate)))

        # Seed a large transaction
        session.add(Transaction(
            transaction_id="large-txn-001",
            type="DEPOSIT",
            to_account="ACC001",
            amount=Decimal("6000.00"),
            status="completed",
            processed=True,
            created_at=datetime.now(timezone.utc),
        ))
        # Seed a completed cross-currency transfer
        session.add(Transaction(
            transaction_id="cross-txn-001",
            type="TRANSFER",
            from_account="ACC001",
            to_account="ACC002",
            amount=Decimal("500.00"),
            status="completed",
            processed=True,
            created_at=datetime.now(timezone.utc),
        ))
        # Seed a normal small transaction
        session.add(Transaction(
            transaction_id="normal-txn-001",
            type="TRANSFER",
            from_account="ACC001",
            to_account="ACC005",
            amount=Decimal("100.00"),
            status="completed",
            processed=True,
            created_at=datetime.now(timezone.utc),
        ))
        session.commit()
        session.close()

        from src.config import Settings
        from src.dashboard.app import create_app
        import src.dashboard.app as app_module

        config = Settings(db_url="sqlite:///:memory:")
        flask_app = create_app(config)
        flask_app.config["TESTING"] = True

        original_get_engine = app_module.get_engine
        original_get_session_factory = app_module.get_session_factory

        app_module.get_engine = lambda url: db_engine
        app_module.get_session_factory = lambda eng: Session

        yield flask_app

        app_module.get_engine = original_get_engine
        app_module.get_session_factory = original_get_session_factory

    @pytest.fixture()
    def client(self, app):
        return app.test_client()

    def test_compliance_returns_200(self, client):
        resp = client.get("/api/compliance")
        assert resp.status_code == 200

    def test_compliance_has_expected_keys(self, client):
        data = json.loads(client.get("/api/compliance").data)
        assert "large_transactions" in data
        assert "cross_currency_count" in data
        assert "total_flagged" in data
        assert "by_account_type" in data

    def test_compliance_large_transactions(self, client):
        data = json.loads(client.get("/api/compliance").data)
        # We seeded one transaction with amount=6000 (> 5000)
        assert len(data["large_transactions"]) == 1
        assert data["large_transactions"][0]["amount"] == 6000.0

    def test_compliance_cross_currency_count(self, client):
        data = json.loads(client.get("/api/compliance").data)
        # ACC001 (USD) -> ACC002 (EUR) is cross-currency
        assert data["cross_currency_count"] == 1

    def test_compliance_total_flagged(self, client):
        data = json.loads(client.get("/api/compliance").data)
        # 1 large + 1 cross-currency = 2
        assert data["total_flagged"] == 2

    def test_compliance_by_account_type(self, client):
        data = json.loads(client.get("/api/compliance").data)
        # All 3 completed transactions involve CHECKING or SAVINGS accounts
        assert isinstance(data["by_account_type"], dict)
        total = sum(data["by_account_type"].values())
        assert total == 3  # 3 completed transactions
