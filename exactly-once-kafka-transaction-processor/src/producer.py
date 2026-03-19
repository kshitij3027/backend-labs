"""Transactional Kafka producer for generating financial transactions."""

import random
import threading
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import structlog
from confluent_kafka import Producer

from src.config import Settings
from src.models import TransactionMessage, TransactionType

logger = structlog.get_logger(__name__)

ACCOUNT_POOL = ["ACC001", "ACC002", "ACC003", "ACC004", "ACC005"]

# Account currency mapping for cross-currency transfer generation
ACCOUNT_CURRENCIES = {
    "ACC001": "USD",
    "ACC002": "EUR",
    "ACC003": "GBP",
    "ACC004": "USD",
    "ACC005": "USD",
}


class TransactionalProducer:
    """Idempotent, transactional Kafka producer for financial transactions."""

    def __init__(self, config: Settings) -> None:
        self.config = config
        self.producer = Producer(
            {
                "bootstrap.servers": config.bootstrap_servers,
                "transactional.id": f"{config.transactional_id_prefix}-producer",
                "enable.idempotence": True,
                "acks": "all",
                "max.in.flight.requests.per.connection": 5,
                "message.timeout.ms": 30000,
            }
        )
        self.producer.init_transactions()
        logger.info("transactional_producer_initialized")

    @staticmethod
    def _generate_transaction() -> TransactionMessage:
        """Generate a random financial transaction."""
        txn_type = random.choice(list(TransactionType))

        if txn_type == TransactionType.TRANSFER:
            # 30% chance of cross-currency transfer
            if random.random() < 0.3:
                # Pick accounts with different currencies
                from_account = random.choice(ACCOUNT_POOL)
                from_currency = ACCOUNT_CURRENCIES[from_account]
                cross_candidates = [
                    acc for acc in ACCOUNT_POOL
                    if acc != from_account and ACCOUNT_CURRENCIES[acc] != from_currency
                ]
                if cross_candidates:
                    to_account = random.choice(cross_candidates)
                else:
                    to_account = random.choice(
                        [acc for acc in ACCOUNT_POOL if acc != from_account]
                    )
            else:
                from_account = random.choice(ACCOUNT_POOL)
                to_account = random.choice(
                    [acc for acc in ACCOUNT_POOL if acc != from_account]
                )
            amount = Decimal(str(round(random.uniform(10, 5000), 2)))
            return TransactionMessage(
                transaction_id=str(uuid.uuid4()),
                type=txn_type,
                from_account=from_account,
                to_account=to_account,
                amount=amount,
                timestamp=datetime.now(timezone.utc).isoformat(),
            )

        if txn_type == TransactionType.DEPOSIT:
            to_account = random.choice(ACCOUNT_POOL)
            amount = Decimal(str(round(random.uniform(100, 10000), 2)))
            return TransactionMessage(
                transaction_id=str(uuid.uuid4()),
                type=txn_type,
                to_account=to_account,
                amount=amount,
                timestamp=datetime.now(timezone.utc).isoformat(),
            )

        # WITHDRAWAL
        from_account = random.choice(ACCOUNT_POOL)
        amount = Decimal(str(round(random.uniform(10, 2000), 2)))
        return TransactionMessage(
            transaction_id=str(uuid.uuid4()),
            type=txn_type,
            from_account=from_account,
            amount=amount,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )

    def _delivery_callback(self, err, msg) -> None:
        """Log delivery success or failure."""
        if err is not None:
            logger.error(
                "message_delivery_failed",
                error=str(err),
                topic=msg.topic(),
            )
        else:
            logger.debug(
                "message_delivered",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
            )

    def send_transaction(self, msg: TransactionMessage) -> None:
        """Produce a transaction message to the pending topic within a transaction."""
        self.producer.begin_transaction()
        self.producer.produce(
            topic=self.config.topic_pending,
            key=msg.transaction_id.encode("utf-8"),
            value=msg.to_kafka_value(),
            callback=self._delivery_callback,
        )
        self.producer.commit_transaction()

    def run(self, shutdown_event: threading.Event) -> None:
        """Continuously generate and send transactions until shutdown."""
        logger.info("producer_started", interval=self.config.producer_interval)

        while not shutdown_event.is_set():
            try:
                msg = self._generate_transaction()
                self.send_transaction(msg)
                self.producer.flush(timeout=5.0)
                logger.info(
                    "transaction_sent",
                    transaction_id=msg.transaction_id,
                    type=msg.type.value,
                    amount=str(msg.amount),
                    from_account=msg.from_account,
                    to_account=msg.to_account,
                )
            except Exception:
                logger.exception("producer_send_error")

            shutdown_event.wait(timeout=self.config.producer_interval)

        # Final flush before exit
        self.producer.flush(timeout=10.0)
        logger.info("producer_stopped")
