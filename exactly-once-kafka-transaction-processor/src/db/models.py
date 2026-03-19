"""SQLAlchemy ORM models for accounts and transactions."""

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Index,
    Integer,
    Numeric,
    String,
    func,
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Account(Base):
    """Bank account with a tracked balance."""

    __tablename__ = "accounts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_number = Column(String(32), unique=True, nullable=False)
    balance = Column(Numeric(15, 2), nullable=False, default=0)
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    def __repr__(self) -> str:
        return f"<Account(account_number={self.account_number}, balance={self.balance})>"


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
