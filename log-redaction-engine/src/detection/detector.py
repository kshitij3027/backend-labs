"""Top-level detection orchestrator that fuses regex and NER hits.

:class:`Detector` is the single entry point used by the redaction layer
(C3 onwards). It composes two sub-detectors:

* :func:`src.detection.patterns.match_all` for the five compiled regexes.
* :class:`src.detection.ner.NERDetector` for spaCy PERSON / ORG entities
  (optional — pass ``None`` to disable NER entirely, e.g. in tests).

The interesting work happens in :meth:`Detector.detect`: it combines the
two hit lists and deduplicates by overlapping ``[start, end)`` spans so a
single piece of text doesn't get redacted twice. The dedup rule is:

1. Higher ``confidence`` wins.
2. On ties, ``source == "regex"`` beats ``source == "ner"``.
3. After picking a "kept" hit, any later hit that overlaps it is dropped.

That keeps the output minimal and downstream-friendly: each redaction
strategy in C3 sees a non-overlapping list it can apply in left-to-right
order without coordinate juggling.
"""
from __future__ import annotations

import logging
from typing import Optional

from .ner import NERDetector
from .patterns import Detection, match_all

logger = logging.getLogger(__name__)


class Detector:
    """Composes regex + NER detection and deduplicates overlapping spans.

    Parameters
    ----------
    ner_detector : NERDetector | None
        Optional NER detector. Pass ``None`` to skip NER entirely (useful
        in unit tests and for ``NER_ENABLED=false`` deployments).
    ner_min_length : int
        Minimum text length before NER runs. Forwarded to
        :meth:`NERDetector.detect`. Default 40 chars matches
        ``Settings.NER_MIN_LENGTH``.
    regex_timeout : float
        Per-pattern regex timeout in seconds. Forwarded to
        :func:`patterns.match_all`. Default 0.05 matches
        ``Settings.REGEX_TIMEOUT_SEC``.
    """

    def __init__(
        self,
        ner_detector: Optional[NERDetector] = None,
        ner_min_length: int = 40,
        regex_timeout: float = 0.05,
    ) -> None:
        self.ner_detector = ner_detector
        self.ner_min_length = ner_min_length
        self.regex_timeout = regex_timeout

    # -- public ----------------------------------------------------------

    def detect(self, text: str) -> list[Detection]:
        """Return the deduplicated list of detections in ``text``.

        Runs regex first, then NER (if configured), then dedups any
        overlapping spans (regex wins ties, higher confidence wins
        otherwise). Final list is sorted by ``start``.

        An empty input returns ``[]`` without invoking either subsystem.
        """
        if not text:
            return []

        regex_hits = match_all(text, timeout=self.regex_timeout)
        ner_hits: list[Detection] = []
        if self.ner_detector is not None:
            ner_hits = self.ner_detector.detect(text, min_length=self.ner_min_length)

        combined = regex_hits + ner_hits
        if not combined:
            return []

        return self._dedupe_overlaps(combined)

    # -- internal --------------------------------------------------------

    @staticmethod
    def _dedupe_overlaps(hits: list[Detection]) -> list[Detection]:
        """Drop later detections that overlap an earlier (higher-priority) hit.

        Algorithm:

        1. Sort by ``(start, -confidence, source_priority)`` so that the
           leftmost, highest-confidence, regex-preferred hit comes first
           when two detections share a span.
        2. Iterate; keep a hit only if its ``start`` is at or beyond the
           last kept hit's ``end``. Otherwise it overlaps something we've
           already committed to and is dropped.

        ``source_priority`` is 0 for ``"regex"`` and 1 for ``"ner"`` so
        regex sorts first under ascending order.
        """
        def sort_key(d: Detection) -> tuple[int, float, int]:
            # Lower tuple = sorts earlier = "wins" overlap contests.
            source_priority = 0 if d.source == "regex" else 1
            return (d.start, -d.confidence, source_priority)

        ordered = sorted(hits, key=sort_key)

        kept: list[Detection] = []
        last_end = -1
        for d in ordered:
            if d.start < last_end:
                # Overlaps the previously kept hit (which won the
                # priority contest by sort order). Drop this one.
                continue
            kept.append(d)
            last_end = d.end

        # ``kept`` is already in start order because we iterated in sort
        # order (start ascending). No second sort needed.
        return kept
