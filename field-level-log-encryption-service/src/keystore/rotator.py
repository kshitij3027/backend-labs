"""Time-driven DEK rotation policy.

:class:`RotationManager` is a **policy object**, not a scheduler. It owns the
question "is it time to rotate?" but does not own the answer to "who calls
me and when?" — that's left to the FastAPI startup wiring in C7, which will
schedule periodic ``maybe_rotate()`` calls via an ``asyncio`` background task.

Separating the policy from the scheduling has two benefits:

1. **Testability** — tests can drive rotation with a fake clock
   (``now_fn`` injection) instead of having to sleep or patch ``time``.
2. **Reusability** — the same policy works whether called from a sync
   background thread, an ``asyncio`` task, or a manual operator hook
   (``POST /v1/keys/rotate`` if we ever expose one).

The decision rule is intentionally simple — "active.created_at + interval
≤ now()" — and the active key's own timestamp is the source of truth. After
each rotation the new active record's ``created_at`` resets the clock, so
operator-triggered rotations are correctly factored in. We deliberately do
NOT keep a separate ``last_rotation_at`` field on the manager: it would
drift out of sync with the keystore's own timestamps after a destroy or a
manual rotation, and there is no value-add over reading the keystore.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Callable

from .store import KeyNotFoundError, KeyStore


def _utcnow() -> datetime:
    """Default wall clock — matches :func:`src.keystore.store._utcnow`."""
    return datetime.now(timezone.utc)


class RotationManager:
    """Decides whether the keystore's active DEK is due for rotation.

    Parameters
    ----------
    store : KeyStore
        The keystore this manager governs.
    interval_days : int | float
        Rotation interval in days. ``float`` is accepted so tests can
        pass fractional days (e.g., ``0.0001`` is ~8.6s) without
        having to mock time. Production uses the integer
        :data:`src.settings.Settings.key_rotation_days`.
    now_fn : Callable[[], datetime] | None, optional
        Clock injection point. ``None`` (default) uses
        ``datetime.now(timezone.utc)``. Tests pass a deterministic
        function so the rotation boundary can be probed without
        sleeping.

    Notes
    -----
    The manager is **stateless** beyond its construction params. All
    rotation state lives on the keystore. That makes it trivially safe
    to construct multiple managers or to recreate one after a restart.
    """

    def __init__(
        self,
        store: KeyStore,
        interval_days: int | float,
        *,
        now_fn: Callable[[], datetime] | None = None,
    ) -> None:
        self._store = store
        # timedelta accepts floats happily — `days=0.0001` becomes
        # roughly 8.6 seconds, which is what fast unit tests want.
        self._interval = timedelta(days=interval_days)
        self._now_fn = now_fn if now_fn is not None else _utcnow

    def maybe_rotate(self) -> bool:
        """Rotate iff the active key is older than the configured interval.

        Returns
        -------
        bool
            ``True``  — rotation occurred (a new active key now exists).
            ``False`` — no rotation needed (or no active key to rotate).

        Behavior on edge cases
        ----------------------
        * If the keystore has no active key yet (startup never called
          :meth:`KeyStore.create_initial_active`), returns ``False``
          rather than raising. The contract is "rotate if due", and
          "nothing to rotate" is a benign no-op, not an error. The
          startup wiring is responsible for bootstrapping.
        * If the active key was created in the future (clock skew),
          the comparison still works — ``created_at + interval`` will
          be even further in the future and no rotation triggers.
          That's the safe direction: better to rotate late than
          rotate prematurely on a skewed clock.
        """
        try:
            active = self._store.get_active()
        except KeyNotFoundError:
            # Nothing to rotate yet. Startup hook will bootstrap.
            return False

        if active.created_at + self._interval <= self._now_fn():
            self._store.rotate()
            return True
        return False
