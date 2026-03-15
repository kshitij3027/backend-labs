"""Priority aging monitor that promotes long-waiting messages."""

import logging
import threading
import time

from src.config import Settings
from src.models import Priority
from src.priority_queue import ThreadSafePriorityQueue

logger = logging.getLogger(__name__)


class PriorityAgingMonitor:
    """Daemon that periodically scans the queue and promotes stale messages.

    A message whose ``created_at`` is older than ``aging_threshold_seconds``
    gets its priority bumped one level (e.g. LOW -> MEDIUM).  CRITICAL
    messages are never promoted further.  The ``created_at`` is reset by
    ``queue.promote()`` so the aging clock restarts after each promotion.
    """

    def __init__(
        self,
        queue: ThreadSafePriorityQueue,
        settings: Settings,
    ) -> None:
        self._queue = queue
        self._settings = settings
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Launch the aging daemon thread."""
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._aging_loop,
            name="priority-aging",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """Signal the aging thread to exit and wait for it."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def _aging_loop(self) -> None:
        """Periodically scan the queue for messages that should be promoted."""
        while not self._stop_event.is_set():
            # Interruptible sleep
            if self._stop_event.wait(timeout=self._settings.aging_check_interval):
                break

            now = time.time()

            # Phase 1: collect candidates while holding the queue lock
            to_promote: list[tuple[str, Priority, Priority]] = []

            with self._queue._lock:
                for msg_id, entry in list(self._queue._entry_map.items()):
                    msg = entry[2]
                    # Skip lazily-removed entries
                    from src.models import REMOVED

                    if msg is REMOVED:
                        continue

                    age = now - msg.created_at
                    if (
                        age > self._settings.aging_threshold_seconds
                        and msg.priority > Priority.CRITICAL
                    ):
                        old_priority = msg.priority
                        new_priority = Priority(msg.priority - 1)
                        to_promote.append((msg_id, old_priority, new_priority))

            # Phase 2: promote outside the lock (promote acquires its own)
            for msg_id, old_priority, new_priority in to_promote:
                success = self._queue.promote(msg_id, new_priority)
                if success:
                    logger.info(
                        "[aging] Promoted message %s from %s to %s",
                        msg_id[:8],
                        old_priority.name,
                        new_priority.name,
                    )

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()
