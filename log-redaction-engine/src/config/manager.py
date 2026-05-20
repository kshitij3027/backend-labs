"""Thread-safe, atomically-reloadable configuration holder.

The manager owns exactly one mutable cell: ``self._config``. Every read is
a single attribute fetch under ``self._lock``; every write replaces the
reference (never the contents — :class:`RedactionConfig` is frozen) under
the same lock. Because the config is frozen, a reader that captured a
reference BEFORE a reload still observes the old config in full — there
is no torn read and no need to re-fetch.

Why RLock vs Lock
-----------------
``threading.RLock`` is reentrant, which costs a single extra owner-thread
check per acquire on the fast path. We pay that microcost so that a
caller holding the lock can call into another manager method (e.g., a
future ``current_preset_name`` accessor that internally calls ``get()``)
without deadlocking. In a system whose surface area will grow over the
next few commits, the safety margin is worth the few nanoseconds.

Why the lock is held for the entire ``get()``
---------------------------------------------
We could read ``self._config`` without a lock since CPython attribute
loads are atomic at the GIL level. We don't, because (a) the cost is
nanoseconds, (b) it makes the concurrency model trivially explicit, and
(c) it removes any future maintainer's temptation to add a "tiny" read
beside the load that would no longer be atomic. The contention story
under heavy reload churn is fine — the lock is held for a single dict
attribute fetch, which is far below any plausible scheduling quantum.

Failure semantics on reload
---------------------------
``reload_from_json`` calls ``model_validate_json`` BEFORE acquiring the
lock. If validation fails (bad JSON, unknown pattern, extra field, etc.),
the :class:`pydantic.ValidationError` propagates out and ``_config`` is
never touched. The old configuration therefore remains the active one —
this is the property C4's tests assert and the API layer in C7 will
rely on to return 400 without service interruption.
"""
from __future__ import annotations

import logging
import threading

from pydantic import ValidationError  # re-exported for callers/tests

from .models import RedactionConfig

logger = logging.getLogger(__name__)

# Re-export so ``from src.config.manager import ValidationError`` works for
# callers that only want the manager surface — keeps their imports tidy.
__all__ = ["ConfigurationManager", "ValidationError"]


class ConfigurationManager:
    """Holds the active :class:`RedactionConfig` and supports atomic reload.

    Construction takes the initial config; from then on every read goes
    through :meth:`get` and every write through :meth:`reload` or
    :meth:`reload_from_json`. There is no other public surface.
    """

    def __init__(self, initial: RedactionConfig) -> None:
        """Stash the initial config and create the guarding lock.

        The lock is created here (not at class scope) so each manager
        instance owns its own lock — tests that spin up multiple
        managers won't contend on a shared lock by accident.
        """
        self._config = initial
        # RLock so reentrant calls from helper methods are safe; the cost
        # over a non-reentrant Lock is negligible (single thread-id check
        # on the fast path).
        self._lock = threading.RLock()

    def get(self) -> RedactionConfig:
        """Return the currently-active configuration reference.

        The returned object is the actual frozen pydantic model; because
        it's frozen, callers can safely read it across the boundary of
        a future :meth:`reload` call. We do NOT copy — the immutability
        guarantee makes copying redundant and we want this call to be
        cheap enough to invoke per-request without thinking.
        """
        # Acquire-release pair is cheap (microseconds) but makes the
        # concurrency model fully explicit: every read is serialized
        # against every write, and writers can't see a half-initialised
        # state because there are no partial writes (single ref rebind).
        with self._lock:
            cfg = self._config
        # Returning OUTSIDE the lock is intentional — the lock guarded
        # the read of the reference; whatever the caller does with it
        # afterwards doesn't need the lock because the value is frozen.
        return cfg

    def reload(self, new: RedactionConfig) -> None:
        """Atomically swap the active configuration to ``new``.

        Old readers that called :meth:`get` before this call still hold
        a valid reference to the previous frozen config; new readers
        starting after this call observe ``new``. There is no torn-read
        window because the swap is a single Python ref rebind under the
        lock.
        """
        with self._lock:
            self._config = new
        # Log at INFO so deployments observe reloads without needing
        # DEBUG. We deliberately do not log the full config body — it
        # may be large and could leak rule details into logs that go
        # to less-trusted aggregators.
        logger.info("redaction config reloaded (version=%s)", new.version)

    def reload_from_json(self, json_str: str) -> RedactionConfig:
        """Validate ``json_str`` and atomically swap if it's well-formed.

        On success: the new config is active and is returned to the
        caller (so the API layer can echo it back in a 200 response).

        On failure (:class:`pydantic.ValidationError`): the old config
        remains active and the exception propagates. C4's tests assert
        this post-condition explicitly because the API caller relies
        on it to return 400 without losing service.
        """
        # Validate FIRST, outside any lock — this is the work that can
        # fail. If it succeeds we then acquire the lock and rebind; if
        # it fails the existing config is untouched.
        new = RedactionConfig.model_validate_json(json_str)
        self.reload(new)
        return new
