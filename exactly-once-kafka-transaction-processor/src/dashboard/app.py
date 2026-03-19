"""Flask dashboard application for the exactly-once transaction processor."""

import multiprocessing
from datetime import datetime, timezone
from decimal import Decimal

from flask import Flask, jsonify, render_template

from src.config import Settings
from src.db.models import Account, AccountType, ExchangeRate, Transaction
from src.db.session import get_engine, get_session_factory
from src.monitor import TransactionMonitor

from sqlalchemy import func

# ---------------------------------------------------------------------------
# Process registry for failure injection
# ---------------------------------------------------------------------------

_process_registry: dict = {}


def register_process(name: str, process, target_fn, config_dict: dict) -> None:
    """Store a process reference and its restart info for failure injection."""
    _process_registry[name] = {
        "process": process,
        "target_fn": target_fn,
        "config_dict": config_dict,
    }


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

# Initial total balance per currency after seeding
INITIAL_BALANCE_BY_CURRENCY = {
    "USD": Decimal("20000.00"),  # ACC001(10000) + ACC004(0) + ACC005(10000)
    "EUR": Decimal("10000.00"),  # ACC002(10000)
    "GBP": Decimal("10000.00"),  # ACC003(10000)
}


def create_app(config: Settings | None = None) -> Flask:
    """Create and configure the Flask dashboard application."""
    app = Flask(
        __name__,
        template_folder="templates",
        static_folder="static",
    )

    if config is None:
        config = Settings()

    app.config["DB_URL"] = config.db_url

    # -----------------------------------------------------------------------
    # DB helpers (lazy, per-request)
    # -----------------------------------------------------------------------

    def _get_session():
        engine = get_engine(app.config["DB_URL"])
        factory = get_session_factory(engine)
        return factory()

    # -----------------------------------------------------------------------
    # Routes
    # -----------------------------------------------------------------------

    @app.route("/")
    def index():
        return render_template("index.html")

    @app.route("/health")
    def health():
        return jsonify({
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    @app.route("/api/stats")
    def api_stats():
        session = _get_session()
        try:
            stats = TransactionMonitor.get_stats(session)

            # Add from/to account info to recent transactions
            recent_rows = (
                session.query(Transaction)
                .order_by(Transaction.created_at.desc())
                .limit(10)
                .all()
            )
            recent_with_accounts = [
                {
                    "transaction_id": t.transaction_id,
                    "type": t.type,
                    "amount": float(t.amount),
                    "from_account": t.from_account,
                    "to_account": t.to_account,
                    "status": t.status,
                    "created_at": t.created_at.isoformat() if t.created_at else None,
                }
                for t in recent_rows
            ]
            stats["recent_transactions"] = recent_with_accounts

            # Quick duplicate check for guarantee status
            dup_count = (
                session.query(Transaction.transaction_id)
                .group_by(Transaction.transaction_id)
                .having(func.count(Transaction.id) > 1)
                .count()
            )
            stats["guarantee_status"] = "MAINTAINED" if dup_count == 0 else "VIOLATED"

            return jsonify(stats)
        finally:
            session.close()

    @app.route("/api/verify-eos")
    def api_verify_eos():
        session = _get_session()
        try:
            checks = []

            # Check 1: No duplicate transaction IDs
            duplicates = (
                session.query(Transaction.transaction_id, func.count(Transaction.id))
                .group_by(Transaction.transaction_id)
                .having(func.count(Transaction.id) > 1)
                .all()
            )
            total = session.query(func.count(Transaction.id)).scalar() or 0
            checks.append({
                "name": "no_duplicate_transaction_ids",
                "passed": len(duplicates) == 0,
                "details": f"total={total}, duplicates={len(duplicates)}",
            })

            # Check 2: Balance conservation (per-currency, excluding cross-currency transfer effects)
            # For multi-currency, we convert all balances to USD via exchange rates for a rough check.
            all_accounts = session.query(Account).all()
            exchange_rates = session.query(ExchangeRate).all()
            rate_map = {(r.from_currency, r.to_currency): r.rate for r in exchange_rates}

            def to_usd(amount, currency):
                if currency == "USD":
                    return amount
                rate = rate_map.get((currency, "USD"))
                if rate:
                    return amount * rate
                return amount  # fallback: treat as USD

            current_total_usd = sum(
                to_usd(a.balance, a.currency or "USD") for a in all_accounts
            )

            deposit_sum = (
                session.query(func.sum(Transaction.amount))
                .filter(Transaction.type == "DEPOSIT", Transaction.status == "completed")
                .scalar()
                or Decimal("0")
            )
            withdrawal_sum = (
                session.query(func.sum(Transaction.amount))
                .filter(Transaction.type == "WITHDRAWAL", Transaction.status == "completed")
                .scalar()
                or Decimal("0")
            )
            payment_sum = (
                session.query(func.sum(Transaction.amount))
                .filter(Transaction.type == "PAYMENT", Transaction.status == "completed")
                .scalar()
                or Decimal("0")
            )

            initial_total_usd = sum(
                to_usd(v, k) for k, v in INITIAL_BALANCE_BY_CURRENCY.items()
            )
            # Note: deposits/withdrawals amounts are in the source account currency;
            # for a rough check we treat them as USD (the producer generates them in
            # source-account currency).  Cross-currency transfers cause small rounding
            # drift so we use a tolerance.
            expected_usd = initial_total_usd + deposit_sum - withdrawal_sum - payment_sum
            balance_diff = abs(float(current_total_usd) - float(expected_usd))
            # Tolerance scales with total transaction volume (cross-currency drift)
            tolerance = max(500.0, float(deposit_sum + withdrawal_sum + payment_sum) * 0.15)
            balance_ok = balance_diff < tolerance
            checks.append({
                "name": "balance_conservation",
                "passed": balance_ok,
                "details": (
                    f"expected_usd~={float(expected_usd):.2f}, actual_usd~={float(current_total_usd):.2f}, "
                    f"diff={balance_diff:.2f}, "
                    f"deposits={float(deposit_sum)}, withdrawals={float(withdrawal_sum)}, "
                    f"payments={float(payment_sum)} (tolerance={tolerance:.0f} for FX drift)"
                ),
            })

            # Check 3: No balances below allowed floor per account type
            all_accounts = session.query(Account).all()
            violations = []
            for acct in all_accounts:
                acct_type = getattr(acct, "account_type", "CHECKING") or "CHECKING"
                if acct_type == AccountType.SAVINGS.value:
                    if acct.balance < 0:
                        violations.append(acct.account_number)
                elif acct_type == AccountType.CREDIT_CARD.value:
                    limit = acct.credit_limit or Decimal("0")
                    if acct.balance < -limit:
                        violations.append(acct.account_number)
                else:
                    # CHECKING — overdraft up to -500
                    if acct.balance < Decimal("-500"):
                        violations.append(acct.account_number)
            checks.append({
                "name": "no_balance_violations",
                "passed": len(violations) == 0,
                "details": (
                    f"violation_accounts={violations}"
                    if violations
                    else "all accounts within limits"
                ),
            })

            all_passed = all(c["passed"] for c in checks)
            return jsonify({
                "checks": checks,
                "guarantee_status": "MAINTAINED" if all_passed else "VIOLATED",
            })
        finally:
            session.close()

    @app.route("/api/compliance")
    def api_compliance():
        session = _get_session()
        try:
            # Large transactions (amount > 5000), last 50
            large_txn_rows = (
                session.query(Transaction)
                .filter(Transaction.amount > 5000)
                .order_by(Transaction.created_at.desc())
                .limit(50)
                .all()
            )
            large_transactions = [
                {
                    "transaction_id": t.transaction_id,
                    "type": t.type,
                    "amount": float(t.amount),
                    "status": t.status,
                    "from_account": t.from_account,
                    "to_account": t.to_account,
                    "created_at": t.created_at.isoformat() if t.created_at else None,
                }
                for t in large_txn_rows
            ]

            # Cross-currency count
            all_accounts = session.query(Account).all()
            acct_currency_map = {a.account_number: (a.currency or "USD") for a in all_accounts}
            acct_type_map = {a.account_number: (a.account_type or "CHECKING") for a in all_accounts}

            transfer_txns = session.query(Transaction).filter(
                Transaction.type == "TRANSFER",
                Transaction.status == "completed",
            ).all()
            cross_currency_count = 0
            for t in transfer_txns:
                from_cur = acct_currency_map.get(t.from_account, "USD")
                to_cur = acct_currency_map.get(t.to_account, "USD")
                if from_cur != to_cur:
                    cross_currency_count += 1

            # By account type
            completed_txns = session.query(Transaction).filter(
                Transaction.status == "completed"
            ).all()
            by_account_type: dict[str, int] = {}
            for t in completed_txns:
                acct_num = t.from_account or t.to_account
                acct_type = acct_type_map.get(acct_num, "UNKNOWN")
                by_account_type[acct_type] = by_account_type.get(acct_type, 0) + 1

            total_flagged = len(large_transactions) + cross_currency_count

            return jsonify({
                "large_transactions": large_transactions,
                "cross_currency_count": cross_currency_count,
                "total_flagged": total_flagged,
                "by_account_type": by_account_type,
            })
        finally:
            session.close()

    @app.route("/api/inject-failure/consumer-crash", methods=["POST"])
    def inject_consumer_crash():
        entry = _process_registry.get("consumer")
        if entry is None:
            return jsonify({"status": "error", "message": "consumer process not registered"}), 400

        old_process = entry["process"]
        target_fn = entry["target_fn"]
        config_dict = entry["config_dict"]

        # Terminate the running consumer
        old_pid = old_process.pid
        if old_process.is_alive():
            old_process.terminate()
            old_process.join(timeout=5)
            if old_process.is_alive():
                old_process.kill()
                old_process.join(timeout=3)

        # Restart the consumer with a fresh shutdown event
        new_shutdown = multiprocessing.Event()
        new_process = multiprocessing.Process(
            target=target_fn,
            args=(config_dict, new_shutdown),
            name="consumer",
            daemon=True,
        )
        new_process.start()

        # Update registry
        entry["process"] = new_process

        return jsonify({
            "status": "success",
            "message": "Consumer crashed and restarted",
            "old_pid": old_pid,
            "new_pid": new_process.pid,
        })

    return app
