"""Sequence & anomaly mining over per-entity event streams (Feature Area B).

This module detects *unusual event sequences*. Where clustering looks at one log at a time,
sequence mining looks at the **order** of events an entity emits: a healthy ``web`` service
emits a long run of ``INFO:2xx`` events, whereas a host under a brute-force attack emits a
run of ``WARN:4xx`` / ``CRITICAL:4xx`` events, and a failing service emits ``ERROR:5xx`` /
``CRITICAL:5xx``. Those transitions are the signal.

Pipeline
--------
1. :func:`build_sequences` groups logs by entity (``service`` or ``source_ip``), orders each
   group by timestamp, and maps every log to a coarse **event token** — ``LEVEL:status_class``
   (e.g. ``INFO:2xx``, ``ERROR:5xx``, ``WARN:4xx``). The token is deliberately coarse so the
   vocabulary stays tiny and *normal* traffic collapses onto a handful of tokens that an
   attack/error burst never produces.
2. :func:`fit_sequence_model` learns the set + frequencies of **normal n-grams** (sliding
   windows of size ``n``, default 2) from a NORMAL training corpus, plus a rarity threshold:
   a window is "rare" if it was never seen in training (or seen below a tiny frequency floor).
3. :func:`score_sequence` scores an entity's event sequence as the **fraction of its windows
   that are rare/unseen** vs the model — an anomaly score in ``[0, 1]``.
4. :func:`detect_sequence_anomalies` ties it together: fit (on a supplied normal model, or on
   the logs themselves as a mostly-normal baseline), score every entity, and flag those at or
   above the decision threshold.

The construction is intentionally separable on the seeded data: normal sequences are built
purely from ``INFO:2xx`` / ``DEBUG:2xx`` windows, while security/error bursts are built from
4xx/5xx windows that *never appear* in normal traffic — so an unseen-window fraction cleanly
separates the two classes (this is what lets the accompanying test demonstrate >= 95%
anomaly-detection accuracy). Everything is numpy/stdlib and fully deterministic.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
from typing import TYPE_CHECKING, Any

from src.preprocessing import parse_log

if TYPE_CHECKING:  # pragma: no cover - typing only
    from src.schemas import LogEntry

# Default sliding-window size for n-gram extraction.
_DEFAULT_N: int = 2

# A window seen at/below this share of training windows is treated as "rare" (alongside
# never-seen windows). Small, so only genuinely uncommon transitions count.
_RARITY_FLOOR: float = 0.005

# An entity whose rare-window fraction is >= this is flagged anomalous. With the coarse
# LEVEL:status_class tokenization, normal entities score ~0 and bursty ones ~1, so the
# midpoint is a robust, well-separated cut.
_ANOMALY_THRESHOLD: float = 0.5

# How many leading event tokens to echo back as a human-readable sample per anomaly.
_SAMPLE_EVENTS: int = 8


def _status_class(status: Any) -> str:
    """Map a numeric status code to a coarse class token (``2xx`` ... ``5xx`` / ``na``)."""
    if isinstance(status, int) and 100 <= status <= 599:
        return f"{status // 100}xx"
    return "na"


def _event_token(parsed: dict[str, Any]) -> str:
    """Map a parsed log to its coarse event token ``LEVEL:status_class``.

    Coarse on purpose: this collapses the high-cardinality message text onto a small alphabet
    so *normal* runs (``INFO:2xx``) are sharply distinct from attack/error runs (``WARN:4xx``,
    ``ERROR:5xx``). A missing level degrades to ``UNK``.
    """
    level = parsed.get("level") or "UNK"
    return f"{level}:{_status_class(parsed.get('status_code'))}"


def build_sequences(
    logs: "list[LogEntry | dict]", by: str = "service"
) -> dict[str, list[str]]:
    """Group logs into per-entity, time-ordered sequences of event tokens.

    Args:
        logs: A list of :class:`~src.schemas.LogEntry` or parsed/plain dicts.
        by: Entity key to group on — ``"service"`` (default) or ``"source_ip"``. Logs lacking
            the chosen key are skipped.

    Returns:
        ``{entity: [event_token, ...]}`` with each entity's tokens ordered by timestamp
        (logs without a timestamp are kept in their original relative order, sorted last).
    """
    if by not in ("service", "source_ip"):
        by = "service"

    # Collect (sort_key, token) per entity so we can order by timestamp deterministically.
    buckets: dict[str, list[tuple[Any, int, str]]] = defaultdict(list)
    for order, log in enumerate(logs or []):
        parsed = parse_log(log)
        key = parsed.get(by)
        if not key:
            continue
        ts = parsed.get("timestamp")
        # Timestamped logs sort first (0) by their datetime; untimed logs sort last (1) by
        # arrival order — both deterministic.
        sort_key = (0, ts) if isinstance(ts, datetime) else (1, order)
        buckets[str(key)].append((sort_key, order, _event_token(parsed)))

    sequences: dict[str, list[str]] = {}
    for entity, items in buckets.items():
        items.sort(key=lambda t: (t[0][0], t[0][1] if t[0][0] == 0 else t[1], t[1]))
        sequences[entity] = [tok for _sk, _o, tok in items]
    return sequences


def _ngrams(events: list[str], n: int) -> list[tuple[str, ...]]:
    """Return the sliding-window n-grams of ``events``.

    For sequences shorter than ``n`` the whole sequence is returned as a single (padded-by-
    nothing) window so a 1-event entity still contributes one comparable token.
    """
    if n <= 1:
        return [(e,) for e in events]
    if len(events) < n:
        return [tuple(events)] if events else []
    return [tuple(events[i : i + n]) for i in range(len(events) - n + 1)]


def fit_sequence_model(normal_logs: "list[LogEntry | dict]", n: int = _DEFAULT_N) -> dict:
    """Learn the normal n-gram distribution from a NORMAL training corpus.

    Builds per-entity sequences (grouped by ``service``), extracts every sliding-window
    n-gram, and records their frequencies. The returned model is plain JSON-able data so it
    can be cached / persisted.

    Args:
        normal_logs: Logs assumed to be (mostly) normal — the baseline of "expected" behavior.
        n: Sliding-window size (default 2).

    Returns:
        A model dict ``{"n", "vocab", "ngrams", "total", "rarity_floor", "threshold"}`` where
        ``ngrams`` maps ``"tok|tok"`` -> count, ``vocab`` is the sorted event alphabet, and
        ``total`` is the number of training windows.
    """
    n = max(1, int(n))
    sequences = build_sequences(normal_logs, by="service")

    counts: Counter[tuple[str, ...]] = Counter()
    vocab: set[str] = set()
    for events in sequences.values():
        vocab.update(events)
        counts.update(_ngrams(events, n))

    total = int(sum(counts.values()))
    return {
        "n": n,
        "vocab": sorted(vocab),
        # Serialize tuple keys as "a|b" so the model is JSON-able.
        "ngrams": {"|".join(k): int(v) for k, v in counts.items()},
        "total": total,
        "rarity_floor": _RARITY_FLOOR,
        "threshold": _ANOMALY_THRESHOLD,
    }


def score_sequence(events: list[str], model: dict) -> float:
    """Score one entity's event sequence against a fitted model -> anomaly score in [0, 1].

    The score is the **fraction of the sequence's n-gram windows that are rare** — i.e. never
    seen in training, or seen at/below the model's rarity floor. A sequence dominated by
    normal transitions scores near ``0``; one dominated by unseen attack/error transitions
    scores near ``1``. An empty sequence (no windows) scores ``0``.

    Args:
        events: The entity's ordered event tokens (from :func:`build_sequences`).
        model: A model dict from :func:`fit_sequence_model`.

    Returns:
        A float in ``[0, 1]`` — the rare-window fraction.
    """
    if not model:
        return 0.0
    n = int(model.get("n", _DEFAULT_N))
    windows = _ngrams(list(events or []), n)
    if not windows:
        return 0.0

    ngram_counts: dict[str, int] = model.get("ngrams", {})
    total = int(model.get("total", 0)) or 1
    floor = float(model.get("rarity_floor", _RARITY_FLOOR))

    rare = 0
    for w in windows:
        key = "|".join(w)
        share = ngram_counts.get(key, 0) / total
        if share <= floor:  # unseen (share 0) or vanishingly rare
            rare += 1
    return rare / len(windows)


def detect_sequence_anomalies(
    logs: "list[LogEntry | dict]",
    model: "dict | None" = None,
    n: int = _DEFAULT_N,
    by: str = "service",
) -> dict[str, Any]:
    """Detect entities whose event sequences are anomalous vs a normal model.

    If ``model`` is ``None`` a baseline model is fitted on ``logs`` themselves (treating the
    batch as mostly-normal); otherwise the supplied normal model is used. Every entity's
    sequence is scored and those at/above the model threshold are flagged.

    Args:
        logs: Logs to analyze.
        model: Optional pre-fitted normal model (from :func:`fit_sequence_model`). When
            ``None``, a model is fitted on ``logs``.
        n: Sliding-window size used when fitting a baseline model (ignored if ``model`` given).
        by: Entity key to group on (``"service"`` or ``"source_ip"``).

    Returns:
        ``{"analyzed", "window", "threshold", "model_ngrams", "anomalies"}`` where
        ``anomalies`` is a list of ``{entity, score, length, sample_events}`` sorted by score
        descending. ``analyzed`` is the number of entities scored.
    """
    if model is None:
        model = fit_sequence_model(logs, n=n)
    window = int(model.get("n", n))
    threshold = float(model.get("threshold", _ANOMALY_THRESHOLD))

    sequences = build_sequences(logs, by=by)

    anomalies: list[dict[str, Any]] = []
    for entity, events in sequences.items():
        score = score_sequence(events, model)
        if score >= threshold:
            anomalies.append(
                {
                    "entity": entity,
                    "score": round(float(score), 3),
                    "length": len(events),
                    "sample_events": events[:_SAMPLE_EVENTS],
                }
            )

    anomalies.sort(key=lambda a: (-a["score"], a["entity"]))
    return {
        "analyzed": len(sequences),
        "window": window,
        "threshold": round(threshold, 3),
        "model_ngrams": len(model.get("ngrams", {})),
        "anomalies": anomalies,
    }


__all__ = [
    "build_sequences",
    "fit_sequence_model",
    "score_sequence",
    "detect_sequence_anomalies",
]
