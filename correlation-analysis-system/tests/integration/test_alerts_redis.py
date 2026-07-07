"""Integration: AlertManager -> RedisStore alert mirroring + pub/sub fan-out.

Covers the C5 Redis contract against a real Redis: fired alerts land newest-
first in the capped ``corr:alerts:recent`` list as valid Alert JSON, and every
push is PUBLISHed to ``corr:alerts:channel`` so a live subscriber receives it.
The pub/sub poll uses a hard ~5 s deadline — same-host delivery is millisecond-
scale, so the deadline only bounds a pathological hang.
"""

import time

from src.alerts import AlertManager
from src.config import Settings
from src.models import Alert, Correlation, CorrelationType, EventRef, SourceType
from src.store import CHANNEL_ALERTS, KEY_ALERTS_RECENT, RedisStore

EPOCH = 1000.0


def ref(source: SourceType, ts: float) -> EventRef:
    return EventRef(
        id=f"{source.value}-{ts}",
        source=source,
        service=source.value,
        message=f"{source.value} event",
        timestamp=ts,
    )


def strong_corr(source_b: SourceType = SourceType.DATABASE) -> Correlation:
    """A fabricated correlation strong enough to trip the warning rule."""
    return Correlation(
        id=f"corr-{source_b.value}",
        detected_at=EPOCH,
        correlation_type=CorrelationType.SESSION,
        event_a=ref(SourceType.WEB, EPOCH - 2.0),
        event_b=ref(source_b, EPOCH),
        strength=0.9,
        confidence=0.95,
        details={},
    )


def test_push_alerts_mirrors_alert_json_into_capped_list(redis_client, redis_url):
    manager = AlertManager(Settings(_env_file=None))
    store = RedisStore(redis_url)

    fired = manager.evaluate([strong_corr()], now=EPOCH)
    assert len(fired) == 1
    store.push_alerts(fired)

    assert redis_client.llen(KEY_ALERTS_RECENT) == 1
    entry = Alert.model_validate_json(redis_client.lindex(KEY_ALERTS_RECENT, 0))
    assert entry.id == fired[0].id
    assert entry.severity == "warning"
    assert entry.correlation_type is CorrelationType.SESSION


def test_push_alerts_publishes_to_channel(redis_client, redis_url):
    manager = AlertManager(Settings(_env_file=None))
    store = RedisStore(redis_url)

    # Subscribe BEFORE publishing (pub/sub is fire-and-forget) and drain the
    # subscribe confirmation so the next message received is the alert itself.
    pubsub = redis_client.pubsub()
    pubsub.subscribe(CHANNEL_ALERTS)
    confirmation = pubsub.get_message(timeout=2.0)
    assert confirmation is not None and confirmation["type"] == "subscribe"

    fired = manager.evaluate([strong_corr(source_b=SourceType.PAYMENT)], now=EPOCH)
    assert len(fired) == 1
    store.push_alerts(fired)

    received = None
    deadline = time.monotonic() + 5.0
    while received is None and time.monotonic() < deadline:
        message = pubsub.get_message(timeout=2.0)
        if message is not None and message.get("type") == "message":
            received = message
    pubsub.close()

    assert received is not None, "no alert arrived on corr:alerts:channel within ~5s"
    assert received["channel"] == CHANNEL_ALERTS
    alert = Alert.model_validate_json(received["data"])
    assert alert.id == fired[0].id
    assert alert.severity == "warning"
