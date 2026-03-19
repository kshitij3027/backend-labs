"""Flask dashboard application for the exactly-once transaction processor."""

import multiprocessing
from datetime import datetime, timezone
from decimal import Decimal

from flask import Flask, jsonify, render_template

from src.config import Settings
from src.db.models import Account, Transaction
from src.db.session import get_engine, get_session_factory
from src.monitor import TransactionMonitor

from sqlalchemy import func

# ---------------------------------------------------------------------------
# Process registry for failure injection
# ---------------------------------------------------------------------------

_process_registry: dict = {}


def register_process(name: str, process, target_fn, args: tuple) -> None:
    """Store a process reference and its restart info for failure injection."""
    _process_registry[name] = {
        "process": process,
        "target_fn": target_fn,
        "args": args,
    }


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

INITIAL_TOTAL_BALANCE = Decimal("50000.00")


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

            # Check 2: Balance conservation
            current_total = session.query(func.sum(Account.balance)).scalar() or Decimal("0")
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
            expected = INITIAL_TOTAL_BALANCE + deposit_sum - withdrawal_sum
            balance_ok = current_total == expected
            checks.append({
                "name": "balance_conservation",
                "passed": balance_ok,
                "details": (
                    f"expected={float(expected)}, actual={float(current_total)}, "
                    f"deposits={float(deposit_sum)}, withdrawals={float(withdrawal_sum)}"
                ),
            })

            # Check 3: No negative balances
            negatives = (
                session.query(Account.account_number, Account.balance)
                .filter(Account.balance < 0)
                .all()
            )
            checks.append({
                "name": "no_negative_balances",
                "passed": len(negatives) == 0,
                "details": (
                    f"negative_accounts={[a[0] for a in negatives]}"
                    if negatives
                    else "all accounts non-negative"
                ),
            })

            all_passed = all(c["passed"] for c in checks)
            return jsonify({
                "checks": checks,
                "guarantee_status": "MAINTAINED" if all_passed else "VIOLATED",
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
        args = entry["args"]

        # Terminate the running consumer
        if old_process.is_alive():
            old_process.terminate()
            old_process.join(timeout=5)

        # Restart the consumer
        new_process = multiprocessing.Process(
            target=target_fn,
            args=args,
            name="consumer",
            daemon=True,
        )
        new_process.start()

        # Update registry
        entry["process"] = new_process

        return jsonify({
            "status": "success",
            "message": "Consumer crashed and restarted",
            "old_pid": old_process.pid,
            "new_pid": new_process.pid,
        })

    return app
