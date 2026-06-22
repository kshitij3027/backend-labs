"""A/B traffic router over registry versions with graceful fallback (Commit 13).

This module is the **serving layer** for Feature Area C (project requirements §3):
*A/B testing between model versions* and *graceful fallback during updates*. It sits
on top of the versioned :class:`src.model_store.ModelRegistry` and routes inference
across two of its versions — a **champion** ("A") and a **challenger** ("B") — with a
configurable traffic split, recording per-version serving metrics so the two versions
can be compared on live traffic before one is promoted.

The headline behaviour is **graceful fallback**: the version a request is assigned to
might be mid-swap, missing on disk, or otherwise broken. Rather than surface a ``500``
to the client, :meth:`ABRouter.classify` transparently falls back to the *other*
group's classifier (and, failing that, to the registry's current champion). The client
still gets a valid classification as long as **any** model can serve; only when nothing
can serve does it raise a :class:`RuntimeError` (which the API maps to a ``503``).

Design notes
------------
* **Load once per version, not per request.** Loading a :class:`~src.ensemble.LogClassifier`
  off disk (joblib) is expensive. The router caches the loaded classifier for each
  configured version id and only refreshes that cache on :meth:`~ABRouter.configure`
  or :meth:`~ABRouter.promote`, never on the hot ``classify`` path.
* **Thread-safe.** A single :class:`threading.Lock` guards the A/B configuration, the
  classifier cache and the per-version metrics, so the router is safe to share across
  the FastAPI request threads and the background retrain thread (which calls
  :meth:`configure` to install a freshly-trained challenger).
* **Non-invasive.** This is purely additive: the base ``/classify`` path and the
  registry's own API are untouched. The router only *reads* the registry (plus
  :meth:`~src.model_store.ModelRegistry.set_current` on an explicit promote).

Out of scope (explicitly): **distributed inference** is NOT implemented here — the
router serves entirely in-process.
"""

from __future__ import annotations

import random
import threading
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:  # pragma: no cover - typing only
    from src.ensemble import LogClassifier
    from src.model_store import ModelRegistry

#: The two A/B groups. "A" is always the champion (the promoted, live model); "B" is
#: the challenger (e.g. a freshly-trained version under evaluation).
GROUP_A = "A"
GROUP_B = "B"


def _empty_version_metrics() -> dict[str, Any]:
    """Return a fresh per-version serving-metrics accumulator.

    ``confidence_sum`` is kept internally so :meth:`ABRouter.metrics` can derive a
    rolling ``avg_confidence`` without storing every observation.
    """
    return {
        "requests": 0,
        "errors": 0,
        "fallbacks": 0,
        "confidence_sum": 0.0,
        "last_used": None,
    }


class ABRouter:
    """Routes inference across two registry versions with graceful fallback.

    The router holds two version ids — :attr:`a_version` (champion) and
    :attr:`b_version` (challenger) — and a split :attr:`split_b` in ``[0, 1]`` giving
    the probability a request is routed to B. Each configured version's
    :class:`~src.ensemble.LogClassifier` is loaded once and cached; per-version
    serving metrics (request/error/fallback counts, confidence, last-used) accumulate
    as traffic flows.

    All mutating state (config, cache, metrics) is guarded by a single
    :class:`threading.Lock`, so the router is safe to share across the API's request
    threads and the background retrain thread.

    Attributes:
        registry: The backing :class:`src.model_store.ModelRegistry`.
        split_b: Fraction of traffic routed to group B (challenger), in ``[0, 1]``.
        a_version: Champion version id served as group "A" (``None`` until configured).
        b_version: Challenger version id served as group "B" (``None`` until configured).
    """

    def __init__(
        self,
        registry: "ModelRegistry",
        split_b: float = 0.5,
        seed: Optional[int] = None,
    ) -> None:
        """Create a router over ``registry`` (initially unconfigured).

        Args:
            registry: The versioned model registry to route across.
            split_b: Initial fraction of traffic to send to group B (challenger),
                clamped to ``[0, 1]``. Defaults to an even 50/50 split.
            seed: Optional seed for a private :class:`random.Random` used by
                :meth:`assign` — makes routing deterministic in tests. When ``None``
                the module-level :mod:`random` is used (varies per call).
        """
        self.registry = registry
        self._lock = threading.Lock()

        self.split_b: float = self._clamp_split(split_b)
        self.a_version: Optional[str] = None
        self.b_version: Optional[str] = None

        #: version id -> cached fitted ``LogClassifier`` (load once, reuse).
        self._cache: dict[str, "LogClassifier"] = {}
        #: version id -> per-version serving metrics accumulator.
        self._metrics: dict[str, dict[str, Any]] = {}

        #: Private RNG when a seed is given (deterministic); else module ``random``.
        self._rng: Optional[random.Random] = random.Random(seed) if seed is not None else None

    # -- internal helpers --------------------------------------------------

    @staticmethod
    def _clamp_split(split_b: float) -> float:
        """Clamp ``split_b`` into ``[0.0, 1.0]`` (a probability)."""
        try:
            value = float(split_b)
        except (TypeError, ValueError):
            return 0.5
        return max(0.0, min(1.0, value))

    def _ensure_metrics(self, version: str) -> dict[str, Any]:
        """Return (creating if needed) the metrics accumulator for ``version``.

        Must be called with :attr:`_lock` held.
        """
        bucket = self._metrics.get(version)
        if bucket is None:
            bucket = _empty_version_metrics()
            self._metrics[version] = bucket
        return bucket

    def _load_and_cache(self, version: str) -> "LogClassifier":
        """Load ``version`` from the registry and cache it (load once per version).

        Must be called with :attr:`_lock` held. Raises ``KeyError`` (unknown
        version) or ``FileNotFoundError`` (missing artifacts) straight from the
        registry; :meth:`configure` validates before relying on this.
        """
        cached = self._cache.get(version)
        if cached is not None:
            return cached
        classifier = self.registry.load_version(version)
        self._cache[version] = classifier
        return classifier

    # -- configuration -----------------------------------------------------

    def configure(
        self,
        a_version: Optional[str] = None,
        b_version: Optional[str] = None,
        split_b: Optional[float] = None,
    ) -> None:
        """Set/refresh the A and/or B version ids and the traffic split.

        Any argument left ``None`` keeps its current value. Supplied version ids are
        validated against the registry **before** any state changes (so a bad id
        leaves the router untouched) and their classifiers are loaded and cached
        eagerly, so the first request after a reconfigure does not pay the load cost.

        Args:
            a_version: New champion (group A) version id, or ``None`` to leave as-is.
            b_version: New challenger (group B) version id, or ``None`` to leave as-is.
            split_b: New fraction of traffic to route to B, or ``None`` to leave as-is.

        Raises:
            KeyError: if a supplied version id is not known to the registry.
        """
        # Validate up front so an invalid id cannot leave us half-configured.
        known = {str(e.get("version")) for e in self.registry.list_versions()}
        for vid in (a_version, b_version):
            if vid is not None and vid not in known:
                raise KeyError(f"unknown model version: {vid!r}")

        with self._lock:
            if a_version is not None:
                self.a_version = a_version
                self._load_and_cache(a_version)
                self._ensure_metrics(a_version)
            if b_version is not None:
                self.b_version = b_version
                self._load_and_cache(b_version)
                self._ensure_metrics(b_version)
            if split_b is not None:
                self.split_b = self._clamp_split(split_b)

    def set_default_from_registry(self) -> None:
        """Initialise A = champion (``current``) and B = challenger (``latest``).

        Reads the registry's current version as the champion (group A) and its
        highest-numbered version as the challenger (group B), then caches both. When
        only one version exists the two ids coincide (A == B), which is fine — every
        request then serves that single model. A no-op (leaves the router
        unconfigured) when the registry has no models.
        """
        current = self.registry.current_version
        latest = self.registry.latest()
        a = current or latest
        b = latest or current
        if a is None and b is None:
            return  # no models yet — leave unconfigured
        self.configure(a_version=a, b_version=b)

    # -- routing -----------------------------------------------------------

    def assign(self) -> str:
        """Randomly assign a request to group "A" or "B" per :attr:`split_b`.

        Uses the private seeded RNG when one was supplied (deterministic for tests),
        otherwise the module-level :mod:`random` (varies per call). Returns ``"B"``
        with probability :attr:`split_b`, else ``"A"``.

        Returns:
            ``"B"`` (challenger) or ``"A"`` (champion).
        """
        draw = self._rng.random() if self._rng is not None else random.random()
        return GROUP_B if draw < self.split_b else GROUP_A

    def _record(
        self,
        version: str,
        group: str,
        confidence: float,
        *,
        error: bool,
        fallback: bool,
    ) -> None:
        """Update the per-version serving metrics for one served (or failed) request.

        Must be called with :attr:`_lock` held.
        """
        bucket = self._ensure_metrics(version)
        bucket["requests"] += 1
        if error:
            bucket["errors"] += 1
        if fallback:
            bucket["fallbacks"] += 1
        bucket["confidence_sum"] += float(confidence)
        # Best-effort wall-clock marker; imported lazily to keep the module light.
        from datetime import datetime

        bucket["last_used"] = datetime.utcnow().isoformat()

    def classify(
        self, raw_log: str, timestamp: Optional[str] = None
    ) -> dict[str, Any]:
        """Classify ``raw_log`` via the assigned A/B group, with graceful fallback.

        Flow:

        1. :meth:`assign` picks a group ("A" or "B"); the router serves that group's
           cached classifier.
        2. **Graceful fallback** — if that classifier is missing (``None``) or its
           ``classify`` raises, the router records the failure and retries with the
           *other* group's classifier, then (failing that) the registry's current
           champion. The client therefore still receives a valid classification as
           long as **any** model can serve.
        3. Per-version serving metrics are updated for whichever version actually
           served (and an ``error`` is booked against the originally-assigned version
           when it failed).

        The returned dict is the underlying :meth:`LogClassifier.classify` result
        (``severity`` / ``category`` / ``confidence`` / the two per-axis confidences)
        **plus** three serving keys:

        * ``model_version`` — the version id that actually served the request.
        * ``ab_group`` — the group the request was assigned to ("A" / "B").
        * ``fallback_used`` — ``True`` if the assigned version could not serve and a
          fallback answered instead.

        Args:
            raw_log: The raw log line to classify.
            timestamp: Optional ISO-8601 timestamp for temporal features.

        Returns:
            The classification result dict augmented with the three serving keys.

        Raises:
            RuntimeError: if **no** model can serve the request (the API maps this to
                a ``503``).
        """
        group = self.assign()

        with self._lock:
            assigned_version = self.a_version if group == GROUP_A else self.b_version
            other_version = self.b_version if group == GROUP_A else self.a_version
            # Build an ordered, de-duplicated list of (version, classifier) candidates:
            # the assigned group first, then the other group, then the live champion.
            candidates: list[tuple[Optional[str], Optional["LogClassifier"]]] = []
            seen: set[str] = set()
            for vid in (assigned_version, other_version):
                if vid is not None and vid not in seen:
                    seen.add(vid)
                    candidates.append((vid, self._cache.get(vid)))

            # Snapshot the config we need; release the lock before doing slow work
            # (classifier.classify / registry loads) so other threads aren't blocked.
            current_version = self.registry.current_version

        # The current champion as a last resort (loaded outside the lock so we don't
        # hold it across a potential joblib load).
        if current_version is not None and current_version not in seen:
            seen.add(current_version)
            try:
                champion = self.registry.load_version(current_version)
            except Exception:  # noqa: BLE001 - champion simply unavailable as fallback
                champion = None
            candidates.append((current_version, champion))

        first = True
        served_error = False  # did the originally-assigned version fail?
        for version, classifier in candidates:
            fallback_used = not first
            if classifier is None:
                # No cached classifier for this candidate; treat the assigned-version
                # miss as an error and move on to the next fallback.
                if first:
                    served_error = True
                first = False
                continue
            try:
                result = dict(classifier.classify(raw_log, timestamp))
            except Exception:  # noqa: BLE001 - this version failed; try the next one
                if first:
                    served_error = True
                first = False
                continue

            # Success — record metrics against the version that actually served.
            confidence = float(result.get("confidence", 0.0) or 0.0)
            with self._lock:
                self._record(
                    version,
                    group,
                    confidence,
                    error=False,
                    fallback=fallback_used,
                )
                # Book the failure against the originally-assigned version too, so its
                # error/fallback counters reflect that it could not serve.
                if served_error and version != assigned_version and assigned_version is not None:
                    bucket = self._ensure_metrics(assigned_version)
                    bucket["errors"] += 1
                    bucket["fallbacks"] += 1

            result["model_version"] = version
            result["ab_group"] = group
            result["fallback_used"] = fallback_used
            return result

        # Nothing could serve — record the assigned version's failure and signal 503.
        if assigned_version is not None:
            with self._lock:
                bucket = self._ensure_metrics(assigned_version)
                bucket["errors"] += 1
        raise RuntimeError("no model version is available to serve the request")

    # -- promotion ---------------------------------------------------------

    def promote(self, version: str) -> str:
        """Promote ``version`` to champion: make it ``current`` and group A.

        Validates the version exists, repoints the registry's ``current`` at it
        (:meth:`~src.model_store.ModelRegistry.set_current`), then installs it as
        group A and refreshes the cache. Group B (the challenger) is left as-is.

        Args:
            version: The version id to promote.

        Returns:
            The new champion version id (``== version``).

        Raises:
            KeyError: if ``version`` is not known to the registry.
        """
        known = {str(e.get("version")) for e in self.registry.list_versions()}
        if version not in known:
            raise KeyError(f"unknown model version: {version!r}")

        # Repoint the registry's champion, then mirror it into group A.
        self.registry.set_current(version)
        with self._lock:
            self.a_version = version
            # Force a cache refresh so a promoted-after-retrain version is reloaded.
            self._cache.pop(version, None)
            self._load_and_cache(version)
            self._ensure_metrics(version)
        return version

    # -- introspection -----------------------------------------------------

    def metrics(self) -> dict[str, Any]:
        """Return the A/B configuration plus per-version serving metrics.

        Returns:
            A dict of the form::

                {
                  "a_version": <str|None>,
                  "b_version": <str|None>,
                  "split_b": <float>,
                  "per_version": {
                    <version_id>: {
                      "requests": <int>,
                      "errors": <int>,
                      "fallbacks": <int>,
                      "avg_confidence": <float>,   # confidence_sum / requests
                      "last_used": <iso str|None>,
                    },
                    ...
                  },
                }
        """
        with self._lock:
            per_version = {
                vid: self._public_version_metrics(bucket)
                for vid, bucket in self._metrics.items()
            }
            return {
                "a_version": self.a_version,
                "b_version": self.b_version,
                "split_b": self.split_b,
                "per_version": per_version,
            }

    @staticmethod
    def _public_version_metrics(bucket: dict[str, Any]) -> dict[str, Any]:
        """Project an internal metrics accumulator to its public shape.

        Derives ``avg_confidence`` from the running ``confidence_sum`` / ``requests``
        and drops the internal-only ``confidence_sum`` key.
        """
        requests = int(bucket.get("requests", 0))
        conf_sum = float(bucket.get("confidence_sum", 0.0))
        avg_conf = round(conf_sum / requests, 4) if requests else 0.0
        return {
            "requests": requests,
            "errors": int(bucket.get("errors", 0)),
            "fallbacks": int(bucket.get("fallbacks", 0)),
            "avg_confidence": avg_conf,
            "last_used": bucket.get("last_used"),
        }

    def models(self) -> list[dict[str, Any]]:
        """List every registry version annotated with A/B + serving metrics.

        For each entry from :meth:`~src.model_store.ModelRegistry.list_versions` adds:

        * ``is_champion`` — ``True`` if this is the registry's current version.
        * ``ab_group`` — ``"A"`` / ``"B"`` (or ``None`` if it is neither the
          configured champion nor challenger). When A and B are the same version it
          reports ``"A"`` (champion takes precedence).
        * ``serving_metrics`` — this version's public per-version metrics (zeros if it
          has not served any traffic yet).

        Returns:
            A list of annotated version dicts, ordered by version number (an empty
            list when the registry has no models).
        """
        with self._lock:
            a_version = self.a_version
            b_version = self.b_version
            metrics_copy = {
                vid: self._public_version_metrics(bucket)
                for vid, bucket in self._metrics.items()
            }

        current = self.registry.current_version
        out: list[dict[str, Any]] = []
        for entry in self.registry.list_versions():
            vid = str(entry.get("version"))
            annotated = dict(entry)
            annotated["is_champion"] = vid == current
            if vid == a_version:
                annotated["ab_group"] = GROUP_A
            elif vid == b_version:
                annotated["ab_group"] = GROUP_B
            else:
                annotated["ab_group"] = None
            annotated["serving_metrics"] = metrics_copy.get(
                vid, ABRouter._public_version_metrics(_empty_version_metrics())
            )
            out.append(annotated)
        return out
