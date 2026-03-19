"""Exactly-once consumer implementing the consume-transform-produce (CTP) pattern."""

import json
import threading
from datetime import date
from decimal import Decimal

import structlog
from confluent_kafka import Consumer, Producer, TopicPartition

from src.config import Settings
from src.db.models import Account, AccountType, Transaction
from src.db.session import get_engine, get_session_factory
from src.models import TransactionMessage, TransactionType

logger = structlog.get_logger(__name__)

# Overdraft allowance for CHECKING accounts
CHECKING_OVERDRAFT_LIMIT = Decimal("500.00")


class ExactlyOnceConsumer:
    """Consume pending transactions, process them, and produce results transactionally."""

    def __init__(self, config: Settings) -> None:
        self.config = config

        self.consumer = Consumer(
            {
                "bootstrap.servers": config.bootstrap_servers,
                "group.id": config.consumer_group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "isolation.level": "read_committed",
                "session.timeout.ms": 10000,
                "heartbeat.interval.ms": 3000,
            }
        )

        self.ctp_producer = Producer(
            {
                "bootstrap.servers": config.bootstrap_servers,
                "transactional.id": f"{config.transactional_id_prefix}-consumer-ctp",
                "enable.idempotence": True,
                "acks": "all",
            }
        )
        self.ctp_producer.init_transactions()

        self.consumer.subscribe([config.topic_pending])

        engine = get_engine(config.db_url)
        self.session_factory = get_session_factory(engine)

        logger.info(
            "exactly_once_consumer_initialized",
            group_id=config.consumer_group_id,
            topic=config.topic_pending,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _check_sufficient_funds(account: Account, amount: Decimal) -> bool:
        """Return True if *account* can support a debit of *amount*.

        Rules by account type:
        - CHECKING: balance can go down to -500 (overdraft protection)
        - SAVINGS:  balance must stay >= 0
        - CREDIT_CARD: balance can go negative down to -credit_limit
        - Default (unknown type): treated as CHECKING for backwards compat
        """
        acct_type = account.account_type or AccountType.CHECKING.value

        if acct_type == AccountType.SAVINGS.value:
            return account.balance - amount >= 0

        if acct_type == AccountType.CREDIT_CARD.value:
            limit = account.credit_limit or Decimal("0")
            return account.balance - amount >= -limit

        # CHECKING (or legacy accounts without type)
        return account.balance - amount >= -CHECKING_OVERDRAFT_LIMIT

    @staticmethod
    def _check_daily_limit(account: Account, amount: Decimal) -> bool:
        """Return True if the debit does not exceed the daily spending limit.

        If daily_limit is None the account has no daily cap.
        Resets daily_spent when the date rolls over.
        """
        if account.daily_limit is None:
            return True

        today = date.today()

        # Reset counter on a new day
        if account.daily_spent_date is None or account.daily_spent_date != today:
            account.daily_spent = Decimal("0")
            account.daily_spent_date = today

        return account.daily_spent + amount <= account.daily_limit

    @staticmethod
    def _record_daily_spent(account: Account, amount: Decimal) -> None:
        """Increment the daily spending counter after a successful debit."""
        if account.daily_limit is None:
            return

        today = date.today()
        if account.daily_spent_date is None or account.daily_spent_date != today:
            account.daily_spent = Decimal("0")
            account.daily_spent_date = today

        account.daily_spent += amount

    def _fail_transaction(
        self, session, msg: TransactionMessage, error: str
    ) -> tuple[str, str]:
        """Record a failed transaction and return the failure tuple."""
        txn_record = Transaction(
            transaction_id=msg.transaction_id,
            type=msg.type.value,
            from_account=msg.from_account,
            to_account=msg.to_account,
            amount=msg.amount,
            status="failed",
            processed=True,
            error_message=error,
        )
        session.add(txn_record)
        session.commit()
        return ("failed", error)

    # ------------------------------------------------------------------
    # Main processing
    # ------------------------------------------------------------------

    def process_transaction(
        self, session, msg: TransactionMessage
    ) -> tuple[str, str | None]:
        """Process a single transaction against the database.

        Returns:
            (status, error_message) where status is one of
            "duplicate", "completed", or "failed".
        """
        try:
            # Idempotency check
            existing = (
                session.query(Transaction)
                .filter_by(transaction_id=msg.transaction_id, processed=True)
                .first()
            )
            if existing:
                logger.info(
                    "duplicate_transaction_skipped",
                    transaction_id=msg.transaction_id,
                )
                return ("duplicate", None)

            # Business logic by transaction type
            if msg.type == TransactionType.TRANSFER:
                from_acct = (
                    session.query(Account)
                    .filter_by(account_number=msg.from_account)
                    .with_for_update()
                    .first()
                )
                to_acct = (
                    session.query(Account)
                    .filter_by(account_number=msg.to_account)
                    .with_for_update()
                    .first()
                )

                if from_acct is None or to_acct is None:
                    return ("failed", "account not found")

                if not self._check_sufficient_funds(from_acct, msg.amount):
                    return self._fail_transaction(session, msg, "insufficient funds")

                if not self._check_daily_limit(from_acct, msg.amount):
                    return self._fail_transaction(session, msg, "daily limit exceeded")

                from_acct.balance -= msg.amount
                to_acct.balance += msg.amount
                self._record_daily_spent(from_acct, msg.amount)

            elif msg.type == TransactionType.DEPOSIT:
                to_acct = (
                    session.query(Account)
                    .filter_by(account_number=msg.to_account)
                    .with_for_update()
                    .first()
                )
                if to_acct is None:
                    return ("failed", "account not found")

                to_acct.balance += msg.amount

            elif msg.type in (TransactionType.WITHDRAWAL, TransactionType.PAYMENT):
                from_acct = (
                    session.query(Account)
                    .filter_by(account_number=msg.from_account)
                    .with_for_update()
                    .first()
                )
                if from_acct is None:
                    return ("failed", "account not found")

                if not self._check_sufficient_funds(from_acct, msg.amount):
                    return self._fail_transaction(session, msg, "insufficient funds")

                if not self._check_daily_limit(from_acct, msg.amount):
                    return self._fail_transaction(session, msg, "daily limit exceeded")

                from_acct.balance -= msg.amount
                self._record_daily_spent(from_acct, msg.amount)

            # Record the successful transaction
            txn_record = Transaction(
                transaction_id=msg.transaction_id,
                type=msg.type.value,
                from_account=msg.from_account,
                to_account=msg.to_account,
                amount=msg.amount,
                status="completed",
                processed=True,
            )
            session.add(txn_record)
            session.commit()

            logger.info(
                "transaction_processed",
                transaction_id=msg.transaction_id,
                type=msg.type.value,
                status="completed",
            )
            return ("completed", None)

        except Exception as exc:
            session.rollback()
            logger.exception(
                "transaction_processing_error",
                transaction_id=msg.transaction_id,
            )
            return ("failed", str(exc))

    def run(self, shutdown_event: threading.Event) -> None:
        """Poll for messages and process them in the CTP pattern until shutdown."""
        logger.info("consumer_started")

        try:
            while not shutdown_event.is_set():
                raw_msg = self.consumer.poll(timeout=1.0)

                if raw_msg is None:
                    continue
                if raw_msg.error():
                    logger.error("consumer_poll_error", error=str(raw_msg.error()))
                    continue

                # Step 1: Deserialize
                txn_msg = TransactionMessage.from_kafka_value(raw_msg.value())
                log = logger.bind(transaction_id=txn_msg.transaction_id)

                # Step 2: Process against DB
                session = self.session_factory()
                try:
                    status, error_message = self.process_transaction(session, txn_msg)
                finally:
                    session.close()

                # Step 3: If duplicate, skip Kafka transaction entirely
                if status == "duplicate":
                    log.info("duplicate_skipped_no_kafka_txn")
                    continue

                # Step 4: CTP — produce result and commit offsets atomically
                try:
                    self.ctp_producer.begin_transaction()

                    # Determine output topic and payload
                    if status == "completed":
                        output_topic = self.config.topic_completed
                    else:
                        output_topic = self.config.topic_dlq

                    result_payload = {
                        "transaction_id": txn_msg.transaction_id,
                        "type": txn_msg.type.value,
                        "status": status,
                        "error_message": error_message,
                        "amount": str(txn_msg.amount),
                        "from_account": txn_msg.from_account,
                        "to_account": txn_msg.to_account,
                    }

                    self.ctp_producer.produce(
                        topic=output_topic,
                        key=txn_msg.transaction_id.encode("utf-8"),
                        value=json.dumps(result_payload).encode("utf-8"),
                    )

                    # Send consumed offsets to the transaction
                    group_metadata = self.consumer.consumer_group_metadata()
                    tp = TopicPartition(
                        raw_msg.topic(),
                        raw_msg.partition(),
                        raw_msg.offset() + 1,
                    )
                    self.ctp_producer.send_offsets_to_transaction(
                        [tp], group_metadata
                    )

                    self.ctp_producer.commit_transaction()

                    log.info(
                        "ctp_transaction_committed",
                        status=status,
                        output_topic=output_topic,
                    )

                except Exception:
                    log.exception("ctp_transaction_failed_aborting")
                    try:
                        self.ctp_producer.abort_transaction()
                    except Exception:
                        log.exception("abort_transaction_failed")

        finally:
            self.consumer.close()
            logger.info("consumer_stopped")
