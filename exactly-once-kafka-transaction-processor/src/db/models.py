"""SQLAlchemy ORM models for accounts and transactions."""

import enum

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class AccountType(str, enum.Enum):
    """Types of bank accounts."""

    CHECKING = "CHECKING"
    SAVINGS = "SAVINGS"
    CREDIT_CARD = "CREDIT_CARD"


class Account(Base):
    """Bank account with a tracked balance."""

    __tablename__ = "accounts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_number = Column(String(32), unique=True, nullable=False)
    account_type = Column(String(32), nullable=False, default="CHECKING")
    currency = Column(String(3), nullable=False, default="USD")
    balance = Column(Numeric(15, 2), nullable=False, default=0)
    daily_limit = Column(Numeric(15, 2), nullable=True)
    credit_limit = Column(Numeric(15, 2), nullable=True)
    daily_spent = Column(Numeric(15, 2), nullable=False, default=0)
    daily_spent_date = Column(Date, nullable=True)
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    def __repr__(self) -> str:
        return (
            f"<Account(account_number={self.account_number}, "
            f"type={self.account_type}, currency={self.currency}, balance={self.balance})>"
        )


class ExchangeRate(Base):
    """Currency exchange rate between two currencies."""

    __tablename__ = "exchange_rates"
    __table_args__ = (
        UniqueConstraint("from_currency", "to_currency", name="uq_currency_pair"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    from_currency = Column(String(3), nullable=False)
    to_currency = Column(String(3), nullable=False)
    rate = Column(Numeric(15, 6), nullable=False)
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
    )


class Transaction(Base):
    """Record of a processed (or pending) transaction."""

    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    transaction_id = Column(String(64), unique=True, nullable=False, index=True)
    type = Column(String(32), nullable=False)
    from_account = Column(String(32), nullable=True)
    to_account = Column(String(32), nullable=True)
    amount = Column(Numeric(15, 2), nullable=False)
    status = Column(String(32), nullable=False, default="pending")
    processed = Column(Boolean, nullable=False, default=False)
    error_message = Column(String(512), nullable=True)
    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
    )

    def __repr__(self) -> str:
        return (
            f"<Transaction(transaction_id={self.transaction_id}, "
            f"type={self.type}, status={self.status})>"
        )
