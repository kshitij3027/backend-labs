"""In-process, versioned model registry for the ML Log Classifier (Commit 7).

This module is the **persistence + versioning layer** that sits between the
trainer (:mod:`src.trainer`, which produces fitted :class:`~src.ensemble.LogClassifier`
instances) and everything downstream that *serves* a model — the FastAPI app
(C8/C9), the adaptive-retraining loop (C12) and the A/B serving router (C13). It
does exactly one job: store every trained model under a stable, monotonically
increasing version id, remember which version is "current", and let callers load
any version (or the current one) back. It deliberately does **not** train,
classify, or know anything about features — those concerns live elsewhere.

Storage layout
--------------
Everything lives under a single ``model_dir`` (default
:attr:`src.config.Settings.model_dir`, ``/app/models`` in Docker)::

    <model_dir>/
        metadata.json            # the registry index (schema below)
        v1/                      # one directory per version, written by
            feature_pipeline.joblib    #   LogClassifier.save(<model_dir>/v1)
            severity_ensemble.joblib
            category_ensemble.joblib
            meta.json
        v2/
            ...

``metadata.json`` schema (the single source of truth for the index)::

    {
      "counter": 2,                      # highest version number issued so far
      "current": "v2",                   # id of the active version, or null
      "versions": {
        "v1": {
          "version":   "v1",
          "accuracy":  0.93,             # the headline accuracy (see below)
          "metrics":   {...},            # the full metrics dict from the trainer
          "created_at": "2026-06-22T16:00:00.000000",   # ISO-8601
          "path":      "<model_dir>/v1"  # absolute dir passed to LogClassifier.save
        },
        "v2": {...}
      }
    }

The ``accuracy`` field is pulled from ``metrics["severity_test_accuracy"]`` (the
spec's "90%+ on test logs" headline) or, failing that, from an explicit
``metrics["accuracy"]`` key — but the **whole** metrics dict is always stored too,
so no information is lost.

Concurrency & atomicity
-----------------------
A single :class:`threading.Lock` guards **every mutating operation** (issuing a
version, writing artifacts, repointing ``current``). This is what makes the
graceful hot-swap in C12/C13 safe: a reader calling :meth:`get_current` while a
writer calls :meth:`save_version` / :meth:`set_current` will only ever observe the
old or the new ``current``, never a half-written index. ``metadata.json`` itself is
written atomically — to a temporary file in the same directory, then
:func:`os.replace`'d over the real path (an atomic rename on POSIX) — so a crash
mid-write can never corrupt the index.

The module is intentionally dependency-light: only :mod:`joblib` (via
``LogClassifier.save/load``, imported lazily to avoid a circular import with
:mod:`src.ensemble`), :mod:`json`, :mod:`os`, :mod:`threading` and
:mod:`datetime`.
"""

from __future__ import annotations

import json
import os
import threading
from datetime import datetime
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:  # pragma: no cover - typing only; avoids a runtime import cycle
    from src.ensemble import LogClassifier

from src.config import get_config

#: Name of the registry index file written under ``model_dir``.
METADATA_FILE = "metadata.json"

#: Key in a metrics dict that supplies the headline accuracy, in priority order.
_ACCURACY_KEYS = ("severity_test_accuracy", "accuracy")


def _utc_now_iso() -> str:
    """Return the current UTC wall-clock time as an ISO-8601 string.

    Application code (not the deterministic generator), so a real timestamp is
    appropriate here for ``created_at`` bookkeeping.
    """
    return datetime.utcnow().isoformat()


def _empty_metadata() -> dict[str, Any]:
    """Return a fresh, empty registry index (no versions, nothing current)."""
    return {"counter": 0, "current": None, "versions": {}}


def _version_sort_key(version_id: str) -> tuple[int, str]:
    """Sort key ordering ``v1, v2, ..., v10`` numerically (not lexically).

    Falls back to a lexical-only key for any id that does not match the ``v<int>``
    shape, so a hand-edited/legacy id never crashes a ``list_versions`` call.
    """
    if version_id.startswith("v") and version_id[1:].isdigit():
        return (int(version_id[1:]), "")
    return (0, version_id)


class ModelRegistry:
    """Thread-safe, joblib + JSON, versioned directory store for ``LogClassifier``s.

    Each call to :meth:`save_version` issues the next ``v<N>`` id, asks the
    classifier to persist itself under ``<model_dir>/<id>`` (reusing
    :meth:`src.ensemble.LogClassifier.save`), records an entry in
    ``metadata.json``, and (by default) repoints ``current`` at the new version.
    :meth:`get_current` / :meth:`load_version` rebuild a fitted classifier from
    disk on demand (with a small in-memory cache keyed by version id).

    All mutating operations hold :attr:`_lock`, and ``metadata.json`` is persisted
    atomically, so the store is safe to share across the API request threads and
    the background retraining thread.

    Attributes:
        model_dir: Root directory holding ``metadata.json`` and the per-version
            subdirectories.
        metadata: The in-memory mirror of ``metadata.json`` (see module docstring
            for the schema). Always kept in sync with disk under the lock.
    """

    def __init__(self, model_dir: Optional[str] = None) -> None:
        """Open (or initialise) a registry rooted at ``model_dir``.

        Creates ``model_dir`` if absent and loads an existing ``metadata.json``
        when present; otherwise starts from an empty index. A malformed index file
        is tolerated — it is treated as empty rather than crashing startup.

        Args:
            model_dir: Root directory for the registry. Defaults to
                :attr:`src.config.Settings.model_dir` from :func:`src.config.get_config`.
        """
        self.model_dir: str = model_dir if model_dir is not None else get_config().model_dir
        os.makedirs(self.model_dir, exist_ok=True)

        self._lock = threading.Lock()
        #: In-memory cache of loaded classifiers, keyed by version id. Populated
        #: lazily by :meth:`load_version` / :meth:`get_current`; never holds a
        #: stale "current" pointer because lookups always re-read ``current``.
        self._cache: dict[str, "LogClassifier"] = {}

        self.metadata: dict[str, Any] = self._read_metadata_file()

    # -- internal: paths & metadata I/O ------------------------------------

    @property
    def _metadata_path(self) -> str:
        """Absolute path of the registry's ``metadata.json`` index file."""
        return os.path.join(self.model_dir, METADATA_FILE)

    def _read_metadata_file(self) -> dict[str, Any]:
        """Load and validate ``metadata.json`` from disk, or return an empty index.

        Never raises on a missing/unreadable/malformed file: a corrupt index must
        not take the whole service down, so it falls back to an empty registry and
        lets the next :meth:`save_version` rebuild it.
        """
        path = self._metadata_path
        if not os.path.isfile(path):
            return _empty_metadata()
        try:
            with open(path, "r", encoding="utf-8") as fh:
                loaded = json.load(fh)
        except (OSError, ValueError):
            return _empty_metadata()
        if not isinstance(loaded, dict):
            return _empty_metadata()

        # Normalise the shape so callers can rely on the three keys existing.
        meta = _empty_metadata()
        meta["counter"] = int(loaded.get("counter", 0) or 0)
        current = loaded.get("current")
        meta["current"] = current if isinstance(current, str) else None
        versions = loaded.get("versions")
        if isinstance(versions, dict):
            meta["versions"] = {
                str(k): v for k, v in versions.items() if isinstance(v, dict)
            }
        # A dangling ``current`` (points at a missing version) is reset to None.
        if meta["current"] not in meta["versions"]:
            meta["current"] = None
        return meta

    def _persist_metadata(self) -> None:
        """Atomically write :attr:`metadata` to ``metadata.json``.

        Writes to a temporary file in the same directory and :func:`os.replace`'s
        it over the target — an atomic rename on POSIX — so a reader never sees a
        partially written index and a crash mid-write cannot corrupt it.

        Must be called with :attr:`_lock` held.
        """
        path = self._metadata_path
        tmp_path = f"{path}.tmp.{os.getpid()}"
        with open(tmp_path, "w", encoding="utf-8") as fh:
            json.dump(self.metadata, fh, indent=2, sort_keys=True)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, path)

    @staticmethod
    def _headline_accuracy(metrics: dict[str, Any]) -> Optional[float]:
        """Pick the headline accuracy out of a metrics dict.

        Prefers ``severity_test_accuracy`` (the spec's "90%+ on test logs"), then a
        plain ``accuracy`` key. Returns ``None`` if neither is a finite number.
        """
        for key in _ACCURACY_KEYS:
            if key in metrics:
                try:
                    return float(metrics[key])
                except (TypeError, ValueError):
                    continue
        return None

    # -- mutating operations (all hold the lock) ---------------------------

    def save_version(
        self,
        classifier: "LogClassifier",
        metrics: dict[str, Any],
        make_current: bool = True,
    ) -> str:
        """Persist ``classifier`` as the next version and record its metadata.

        Under the lock: increment the counter to mint ``v<N>``, write the model's
        three artifacts under ``<model_dir>/v<N>`` via
        :meth:`src.ensemble.LogClassifier.save`, store an index entry (headline
        ``accuracy`` + the full ``metrics`` dict + ``created_at`` + ``path``),
        optionally repoint ``current``, and atomically persist ``metadata.json``.

        Args:
            classifier: A **fitted** ``LogClassifier`` (``save`` raises otherwise).
            metrics: The trainer's metrics dict; stored verbatim. Its
                ``severity_test_accuracy`` (or ``accuracy``) becomes the entry's
                headline ``accuracy``.
            make_current: When ``True`` (default), the new version becomes the
                active one returned by :meth:`get_current`.

        Returns:
            The new version id (e.g. ``"v3"``).
        """
        with self._lock:
            counter = int(self.metadata.get("counter", 0)) + 1
            version_id = f"v{counter}"
            version_dir = os.path.join(self.model_dir, version_id)

            # Reuse LogClassifier.save — it creates the dir and writes the three
            # joblib artifacts + its own meta.json.
            classifier.save(version_dir)

            entry: dict[str, Any] = {
                "version": version_id,
                "accuracy": self._headline_accuracy(metrics),
                "metrics": dict(metrics),
                "created_at": _utc_now_iso(),
                "path": version_dir,
            }

            self.metadata["counter"] = counter
            self.metadata["versions"][version_id] = entry
            if make_current:
                self.metadata["current"] = version_id

            # Cache the live, already-fitted object so an immediate get_current()
            # avoids a needless reload.
            self._cache[version_id] = classifier

            self._persist_metadata()
            return version_id

    def set_current(self, version_id: str) -> None:
        """Repoint ``current`` at an existing version and persist the index.

        Args:
            version_id: The version to activate; must already exist.

        Raises:
            KeyError: if ``version_id`` is not a known version.
        """
        with self._lock:
            if version_id not in self.metadata["versions"]:
                raise KeyError(f"unknown model version: {version_id!r}")
            self.metadata["current"] = version_id
            self._persist_metadata()

    # -- read operations ---------------------------------------------------

    def load_version(self, version_id: str) -> "LogClassifier":
        """Load (and cache) the :class:`LogClassifier` for ``version_id``.

        The :class:`~src.ensemble.LogClassifier` import is deferred to call time to
        avoid a circular import (``ensemble`` does not import this module, but the
        trainer wires them together, so we keep the dependency one-directional).

        Args:
            version_id: The version to load.

        Returns:
            A fitted ``LogClassifier`` reconstructed from
            ``<model_dir>/<version_id>``.

        Raises:
            KeyError: if ``version_id`` is not a known version.
            FileNotFoundError: if the version's directory/artifacts are missing.
        """
        from src.ensemble import LogClassifier  # lazy: break the import cycle

        with self._lock:
            entry = self.metadata["versions"].get(version_id)
            if entry is None:
                raise KeyError(f"unknown model version: {version_id!r}")
            cached = self._cache.get(version_id)
            path = entry.get("path") or os.path.join(self.model_dir, version_id)

        if cached is not None:
            return cached

        if not os.path.isdir(path):
            raise FileNotFoundError(f"model version directory missing: {path}")

        classifier = LogClassifier.load(path)
        with self._lock:
            self._cache[version_id] = classifier
        return classifier

    def get_current(self) -> Optional[tuple[str, "LogClassifier"]]:
        """Return ``(version_id, classifier)`` for the active version, or ``None``.

        Loads the model named by ``current`` (via :meth:`load_version`, so the
        in-memory cache is reused). Returns ``None`` when no version has been marked
        current yet.

        Returns:
            A ``(version_id, LogClassifier)`` tuple, or ``None`` if the registry is
            empty / nothing is current.
        """
        with self._lock:
            version_id = self.metadata.get("current")
        if not version_id:
            return None
        classifier = self.load_version(version_id)
        return version_id, classifier

    def list_versions(self) -> list[dict[str, Any]]:
        """Return every version's metadata entry, ordered by version number.

        Returns:
            A list of the stored metadata dicts (each with ``version``,
            ``accuracy``, ``metrics``, ``created_at``, ``path``), sorted ascending
            by the numeric part of the version id (``v1, v2, ..., v10, v11``).
        """
        with self._lock:
            entries = list(self.metadata["versions"].values())
        return sorted(entries, key=lambda e: _version_sort_key(str(e.get("version", ""))))

    def latest(self) -> Optional[str]:
        """Return the highest-numbered version id, or ``None`` if there are none.

        This is the most *recently created* version (by counter), which is not
        necessarily the ``current`` one if :meth:`set_current` rolled back.
        """
        with self._lock:
            versions = self.metadata["versions"]
            if not versions:
                return None
            return max(versions.keys(), key=_version_sort_key)

    @property
    def current_version(self) -> Optional[str]:
        """The id of the active version (``current``), or ``None``."""
        with self._lock:
            return self.metadata.get("current")

    def has_models(self) -> bool:
        """Return ``True`` if at least one version has been saved."""
        with self._lock:
            return bool(self.metadata["versions"])
