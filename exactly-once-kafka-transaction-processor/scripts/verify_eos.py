#!/usr/bin/env python3
"""Verify Exactly-Once Semantics (EOS) guarantees against the database.

Run inside Docker:
    python scripts/verify_eos.py

Uses DB_URL env var (falls back to the default from Settings).
Exit code 0 = all checks pass, 1 = at least one failure.
"""

import sys
from decimal import Decimal

from sqlalchemy import func, text

from src.config import load_config
from src.db.models import Account, Transaction
from src.db.session import get_engine, get_session_factory

INITIAL_TOTAL_BALANCE = Decimal("50000.00")  # 5 accounts x 10 000


def check_no_duplicate_transaction_ids(session) -> bool:
    """Ensure every transaction_id appears exactly once."""
    duplicates = (
        session.query(Transaction.transaction_id, func.count(Transaction.id))
        .group_by(Transaction.transaction_id)
        .having(func.count(Transaction.id) > 1)
        .all()
    )

    if duplicates:
        print("FAIL  No-duplicate-transaction-ids")
        for txn_id, cnt in duplicates:
            print(f"       transaction_id={txn_id}  count={cnt}")
        return False

    total = session.query(func.count(Transaction.id)).scalar() or 0
    print(f"PASS  No-duplicate-transaction-ids  (total={total})")
    return True


def check_balance_conservation(session) -> bool:
    """Sum of account balances must equal initial total + deposits - withdrawals.

    Transfer transactions move money between accounts so they are net-zero.
    """
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

    if current_total != expected:
        print("FAIL  Balance-conservation")
        print(f"       expected={expected}  actual={current_total}")
        print(
            f"       initial={INITIAL_TOTAL_BALANCE}  "
            f"deposits={deposit_sum}  withdrawals={withdrawal_sum}"
        )
        return False

    print(
        f"PASS  Balance-conservation  "
        f"(balance={current_total}, deposits={deposit_sum}, withdrawals={withdrawal_sum})"
    )
    return True


def check_no_negative_balances(session) -> bool:
    """No account should have a negative balance."""
    negatives = (
        session.query(Account.account_number, Account.balance)
        .filter(Account.balance < 0)
        .all()
    )

    if negatives:
        print("FAIL  No-negative-balances")
        for acct, bal in negatives:
            print(f"       account={acct}  balance={bal}")
        return False

    print("PASS  No-negative-balances")
    return True


def main() -> int:
    config = load_config()
    engine = get_engine(config.db_url)
    session_factory = get_session_factory(engine)
    session = session_factory()

    print("=" * 60)
    print("  EXACTLY-ONCE SEMANTICS VERIFICATION")
    print("=" * 60)

    results: list[bool] = []
    try:
        results.append(check_no_duplicate_transaction_ids(session))
        results.append(check_balance_conservation(session))
        results.append(check_no_negative_balances(session))
    finally:
        session.close()
        engine.dispose()

    print("=" * 60)
    if all(results):
        print("  ALL CHECKS PASSED")
        print("=" * 60)
        return 0
    else:
        failed = len([r for r in results if not r])
        print(f"  {failed} CHECK(S) FAILED")
        print("=" * 60)
        return 1


if __name__ == "__main__":
    sys.exit(main())
