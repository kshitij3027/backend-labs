"""Tests for the Flask dashboard application."""

import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.config import Settings
from src.dashboard.app import create_app
from src.db.models import Account, Base, ExchangeRate, Transaction


@pytest.fixture()
def app():
    """Create a Flask test app backed by an in-memory SQLite database."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    session_factory = sessionmaker(bind=engine, expire_on_commit=False)
    session = session_factory()

    # Seed accounts — balances must be consistent with the seeded transactions.
    # Initial total was 40000 (ACC004 is CREDIT_CARD starting at 0).
    # Completed deposit of 500 to ACC001 means ACC001 should be 10500.
    # Transfer of 200 from ACC001->ACC002 means ACC001 = 10300, ACC002 = 10200.
    # Withdrawal of 100 from ACC003 FAILED, so ACC003 stays at 10000.
    # Expected total = 40000 + 500 = 40500.
    for acct_no, balance, acct_type in [
        ("ACC001", "10300.00", "CHECKING"),
        ("ACC002", "10200.00", "SAVINGS"),
        ("ACC003", "10000.00", "CHECKING"),
        ("ACC004", "0.00", "CREDIT_CARD"),
        ("ACC005", "10000.00", "SAVINGS"),
    ]:
        session.add(Account(
            account_number=acct_no,
            balance=Decimal(balance),
            account_type=acct_type,
        ))

    # Seed transactions
    session.add(Transaction(
        transaction_id="txn-001",
        type="DEPOSIT",
        to_account="ACC001",
        amount=Decimal("500.00"),
        status="completed",
        processed=True,
        created_at=datetime.now(timezone.utc),
    ))
    session.add(Transaction(
        transaction_id="txn-002",
        type="TRANSFER",
        from_account="ACC001",
        to_account="ACC002",
        amount=Decimal("200.00"),
        status="completed",
        processed=True,
        created_at=datetime.now(timezone.utc),
    ))
    session.add(Transaction(
        transaction_id="txn-003",
        type="WITHDRAWAL",
        from_account="ACC003",
        amount=Decimal("100.00"),
        status="failed",
        processed=True,
        error_message="insufficient funds",
        created_at=datetime.now(timezone.utc),
    ))
    session.commit()
    session.close()

    config = Settings(db_url="sqlite:///:memory:")
    flask_app = create_app(config)
    flask_app.config["TESTING"] = True

    # Monkey-patch the DB URL so the app uses our seeded in-memory DB.
    # SQLite in-memory DBs are per-connection, so we need to share the engine.
    import src.dashboard.app as app_module

    original_get_engine = app_module.get_engine
    original_get_session_factory = app_module.get_session_factory

    app_module.get_engine = lambda url: engine
    app_module.get_session_factory = lambda eng: session_factory

    yield flask_app

    # Restore originals
    app_module.get_engine = original_get_engine
    app_module.get_session_factory = original_get_session_factory


@pytest.fixture()
def client(app):
    """Flask test client."""
    return app.test_client()


class TestHealthEndpoint:
    def test_returns_200(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200

    def test_returns_healthy_status(self, client):
        data = resp_json(client.get("/health"))
        assert data["status"] == "healthy"
        assert "timestamp" in data


class TestStatsEndpoint:
    def test_returns_200(self, client):
        resp = client.get("/api/stats")
        assert resp.status_code == 200

    def test_has_expected_keys(self, client):
        data = resp_json(client.get("/api/stats"))
        expected_keys = {
            "total_transactions",
            "completed_count",
            "failed_count",
            "by_type",
            "success_rate",
            "accounts",
            "recent_transactions",
            "guarantee_status",
            "by_account_type",
            "large_transactions",
            "compliance_summary",
        }
        assert expected_keys.issubset(set(data.keys()))

    def test_transaction_counts(self, client):
        data = resp_json(client.get("/api/stats"))
        assert data["total_transactions"] == 3
        assert data["completed_count"] == 2
        assert data["failed_count"] == 1

    def test_accounts_present(self, client):
        data = resp_json(client.get("/api/stats"))
        assert len(data["accounts"]) == 5
        assert data["accounts"][0]["account_number"] == "ACC001"

    def test_guarantee_status_maintained(self, client):
        data = resp_json(client.get("/api/stats"))
        assert data["guarantee_status"] == "MAINTAINED"


class TestVerifyEosEndpoint:
    def test_returns_200(self, client):
        resp = client.get("/api/verify-eos")
        assert resp.status_code == 200

    def test_has_checks_and_status(self, client):
        data = resp_json(client.get("/api/verify-eos"))
        assert "checks" in data
        assert "guarantee_status" in data
        assert isinstance(data["checks"], list)
        assert len(data["checks"]) == 3

    def test_all_checks_pass(self, client):
        data = resp_json(client.get("/api/verify-eos"))
        for check in data["checks"]:
            assert check["passed"] is True, f"Check failed: {check['name']}"
        assert data["guarantee_status"] == "MAINTAINED"


class TestComplianceEndpoint:
    def test_returns_200(self, client):
        resp = client.get("/api/compliance")
        assert resp.status_code == 200

    def test_has_expected_keys(self, client):
        data = resp_json(client.get("/api/compliance"))
        expected_keys = {"large_transactions", "cross_currency_count", "total_flagged", "by_account_type"}
        assert expected_keys.issubset(set(data.keys()))

    def test_no_large_transactions_in_seed(self, client):
        data = resp_json(client.get("/api/compliance"))
        # All seeded transactions are <= 5000
        assert len(data["large_transactions"]) == 0

    def test_cross_currency_zero_for_same_currency(self, client):
        data = resp_json(client.get("/api/compliance"))
        # All test accounts default to USD, so no cross-currency transfers
        assert data["cross_currency_count"] == 0


class TestAppFactory:
    def test_creates_valid_flask_app(self):
        config = Settings(db_url="sqlite:///:memory:")
        app = create_app(config)
        assert app is not None
        assert app.name == "src.dashboard.app"

    def test_has_expected_routes(self):
        config = Settings(db_url="sqlite:///:memory:")
        app = create_app(config)
        rules = [rule.rule for rule in app.url_map.iter_rules()]
        assert "/" in rules
        assert "/health" in rules
        assert "/api/stats" in rules
        assert "/api/verify-eos" in rules
        assert "/api/compliance" in rules


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def resp_json(resp):
    """Extract JSON from a Flask test response."""
    return json.loads(resp.data)
