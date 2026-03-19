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
from src.db.models import Account, AccountType, Transaction
from src.db.session import get_engine, get_session_factory

INITIAL_TOTAL_BALANCE = Decimal("40000.00")  # ACC001-003,005 @ 10000 + ACC004 @ 0


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

    payment_sum = (
        session.query(func.sum(Transaction.amount))
        .filter(Transaction.type == "PAYMENT", Transaction.status == "completed")
        .scalar()
        or Decimal("0")
    )

    expected = INITIAL_TOTAL_BALANCE + deposit_sum - withdrawal_sum - payment_sum

    if current_total != expected:
        print("FAIL  Balance-conservation")
        print(f"       expected={expected}  actual={current_total}")
        print(
            f"       initial={INITIAL_TOTAL_BALANCE}  "
            f"deposits={deposit_sum}  withdrawals={withdrawal_sum}  payments={payment_sum}"
        )
        return False

    print(
        f"PASS  Balance-conservation  "
        f"(balance={current_total}, deposits={deposit_sum}, "
        f"withdrawals={withdrawal_sum}, payments={payment_sum})"
    )
    return True


def check_no_balance_violations(session) -> bool:
    """No account should exceed its allowed floor by account type."""
    accounts = session.query(Account).all()
    violations = []

    for acct in accounts:
        acct_type = acct.account_type or AccountType.CHECKING.value
        if acct_type == AccountType.SAVINGS.value:
            if acct.balance < 0:
                violations.append(f"{acct.account_number}={acct.balance}")
        elif acct_type == AccountType.CREDIT_CARD.value:
            limit = acct.credit_limit or Decimal("0")
            if acct.balance < -limit:
                violations.append(f"{acct.account_number}={acct.balance} (limit={limit})")
        else:  # CHECKING
            if acct.balance < Decimal("-500"):
                violations.append(f"{acct.account_number}={acct.balance}")

    if violations:
        print("FAIL  No-balance-violations")
        for v in violations:
            print(f"       {v}")
        return False

    print("PASS  No-balance-violations")
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
        results.append(check_no_balance_violations(session))
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
