"""Transaction monitor — periodically queries DB and logs processing stats."""

import threading
from decimal import Decimal

import structlog
from sqlalchemy import func
from sqlalchemy.orm import Session

from src.config import Settings
from src.db.models import Account, ExchangeRate, Transaction
from src.db.session import get_engine, get_session_factory

logger = structlog.get_logger(__name__)


class TransactionMonitor:
    """Periodically polls the database and reports transaction statistics."""

    def __init__(self, config: Settings) -> None:
        self.config = config
        self.engine = get_engine(config.db_url)
        self.session_factory = get_session_factory(self.engine)

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    @staticmethod
    def get_stats(session: Session) -> dict:
        """Return a snapshot of transaction and account statistics."""
        total_transactions = session.query(func.count(Transaction.id)).scalar() or 0
        processed_count = (
            session.query(func.count(Transaction.id))
            .filter(Transaction.processed.is_(True))
            .scalar()
            or 0
        )
        completed_count = (
            session.query(func.count(Transaction.id))
            .filter(Transaction.status == "completed")
            .scalar()
            or 0
        )
        failed_count = (
            session.query(func.count(Transaction.id))
            .filter(Transaction.status == "failed")
            .scalar()
            or 0
        )

        # Breakdown by transaction type
        type_rows = (
            session.query(Transaction.type, func.count(Transaction.id))
            .group_by(Transaction.type)
            .all()
        )
        by_type: dict[str, int] = {row[0]: row[1] for row in type_rows}

        # Success rate
        success_rate = (
            round(float(completed_count) / float(total_transactions) * 100, 2)
            if total_transactions > 0
            else 0.0
        )

        # Account balances
        account_rows = session.query(Account).order_by(Account.account_number).all()
        accounts = [
            {
                "account_number": a.account_number,
                "balance": float(a.balance),
                "account_type": getattr(a, "account_type", "CHECKING") or "CHECKING",
                "daily_limit": float(a.daily_limit) if getattr(a, "daily_limit", None) is not None else None,
                "credit_limit": float(a.credit_limit) if getattr(a, "credit_limit", None) is not None else None,
            }
            for a in account_rows
        ]

        # Recent transactions (last 10)
        recent_rows = (
            session.query(Transaction)
            .order_by(Transaction.created_at.desc())
            .limit(10)
            .all()
        )
        recent_transactions = [
            {
                "transaction_id": t.transaction_id,
                "type": t.type,
                "amount": float(t.amount),
                "status": t.status,
                "created_at": t.created_at.isoformat() if t.created_at else None,
            }
            for t in recent_rows
        ]

        # Breakdown by account type (based on from_account)
        acct_type_map = {a.account_number: (a.account_type or "CHECKING") for a in account_rows}
        all_txns_for_acct_type = session.query(Transaction).filter(
            Transaction.status == "completed"
        ).all()
        by_account_type: dict[str, int] = {}
        for t in all_txns_for_acct_type:
            acct_num = t.from_account or t.to_account
            acct_type = acct_type_map.get(acct_num, "UNKNOWN")
            by_account_type[acct_type] = by_account_type.get(acct_type, 0) + 1

        # Large transactions (amount > 5000) for compliance
        large_txn_rows = (
            session.query(Transaction)
            .filter(Transaction.amount > 5000)
            .order_by(Transaction.created_at.desc())
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

        # Cross-currency transfer count: transfers where from/to accounts have different currencies
        cross_currency_count = 0
        transfer_txns = session.query(Transaction).filter(
            Transaction.type == "TRANSFER",
            Transaction.status == "completed",
        ).all()
        acct_currency_map = {a.account_number: (a.currency or "USD") for a in account_rows}
        for t in transfer_txns:
            from_cur = acct_currency_map.get(t.from_account, "USD")
            to_cur = acct_currency_map.get(t.to_account, "USD")
            if from_cur != to_cur:
                cross_currency_count += 1

        compliance_summary = {
            "total_large_txns": len(large_transactions),
            "total_cross_currency": cross_currency_count,
            "flagged_count": len(large_transactions) + cross_currency_count,
        }

        return {
            "total_transactions": total_transactions,
            "processed_count": processed_count,
            "completed_count": completed_count,
            "failed_count": failed_count,
            "by_type": by_type,
            "success_rate": success_rate,
            "accounts": accounts,
            "recent_transactions": recent_transactions,
            "by_account_type": by_account_type,
            "large_transactions": large_transactions,
            "compliance_summary": compliance_summary,
        }

    # ------------------------------------------------------------------
    # Pretty-print
    # ------------------------------------------------------------------

    @staticmethod
    def _print_summary(stats: dict) -> None:
        """Print a human-readable summary to stdout."""
        print("\n" + "=" * 60)
        print("  TRANSACTION MONITOR REPORT")
        print("=" * 60)
        print(f"  Total transactions : {stats['total_transactions']}")
        print(f"  Processed          : {stats['processed_count']}")
        print(f"  Completed          : {stats['completed_count']}")
        print(f"  Failed             : {stats['failed_count']}")
        print(f"  Success rate       : {stats['success_rate']:.2f}%")

        if stats["by_type"]:
            print("\n  By type:")
            for txn_type, count in sorted(stats["by_type"].items()):
                print(f"    {txn_type:20s} : {count}")

        if stats["accounts"]:
            print("\n  Account balances:")
            for acct in stats["accounts"]:
                print(f"    {acct['account_number']:10s} : {acct['balance']:>12.2f}")

        if stats["recent_transactions"]:
            print("\n  Recent transactions (last 10):")
            for t in stats["recent_transactions"]:
                print(
                    f"    {t['transaction_id'][:16]:16s}  "
                    f"{t['type']:12s}  "
                    f"{t['amount']:>10.2f}  "
                    f"{t['status']:10s}  "
                    f"{t['created_at'] or ''}"
                )

        print("=" * 60 + "\n")

    # ------------------------------------------------------------------
    # Run loop
    # ------------------------------------------------------------------

    def run(self, shutdown_event: threading.Event) -> None:
        """Poll the database every ``monitor_interval`` seconds until shutdown."""
        logger.info(
            "monitor_started", interval_seconds=self.config.monitor_interval
        )
        while not shutdown_event.is_set():
            session: Session = self.session_factory()
            try:
                stats = self.get_stats(session)
                logger.info(
                    "monitor_report",
                    total=stats["total_transactions"],
                    processed=stats["processed_count"],
                    completed=stats["completed_count"],
                    failed=stats["failed_count"],
                    success_rate=stats["success_rate"],
                )
                self._print_summary(stats)
            except Exception:
                logger.exception("monitor_query_failed")
            finally:
                session.close()

            shutdown_event.wait(timeout=self.config.monitor_interval)

        logger.info("monitor_stopped")
