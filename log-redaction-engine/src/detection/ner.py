"""Named-entity recognition wrapper around spaCy's ``en_core_web_sm`` model.

NER is the most expensive stage in the detection pipeline (~1-10 ms per
short message, vs <0.1 ms for the full regex sweep) so we gate it on two
things:

1. **Length threshold** — short fragments (default < 40 chars) almost
   never contain a meaningful person/org reference; running spaCy on them
   is pure overhead. The threshold is configurable per call.
2. **Lazy model load** — the spaCy model is loaded on first ``detect()``
   call rather than at construction time. Production calls
   ``NERDetector._load()`` eagerly during FastAPI's lifespan (C7) so the
   first real request doesn't eat the ~200 ms warm-up; unit tests stub
   ``_nlp`` directly and never trigger the load path.

We only emit detections for ``PERSON`` and ``ORG`` labels. ``GPE`` (geo-
political entities like cities/countries) and other labels are intentionally
ignored — they're rarely PII in a log-redaction context and would flood the
result list.

NER confidence is fixed at ``0.85`` to reflect spaCy's well-known false-
positive rate on out-of-distribution text (server logs are not Wikipedia).
The :class:`Detector` orchestrator's dedup rule prefers regex hits over NER
hits on the same span, so a tight regex always wins.
"""
from __future__ import annotations

import logging

from .patterns import Detection

logger = logging.getLogger(__name__)


# spaCy entity labels we treat as PII. Keep the set tiny so callers can
# audit the policy at a glance.
_PII_LABELS: frozenset[str] = frozenset({"PERSON", "ORG"})

# Fixed NER confidence. Lower than regex (1.0) so the deduper prefers a
# regex hit when both fire on the same span.
_NER_CONFIDENCE: float = 0.85


class NERDetector:
    """spaCy-backed person/organization detector with lazy model loading.

    The model is loaded once on first use (or explicitly via
    :meth:`_load`) and cached on the instance. Subsequent ``detect``
    calls reuse the cached model.

    Parameters
    ----------
    model_name : str
        spaCy model identifier. Default ``"en_core_web_sm"`` matches the
        model installed in the Dockerfile.
    """

    def __init__(self, model_name: str = "en_core_web_sm") -> None:
        self.model_name = model_name
        # Lazy; populated on first _load() call. Tests inject a MagicMock
        # here directly so the real spaCy load never runs.
        self._nlp = None

    # -- public ----------------------------------------------------------

    def detect(self, text: str, min_length: int = 40) -> list[Detection]:
        """Return PERSON / ORG detections in ``text``.

        Returns ``[]`` immediately if ``len(text) < min_length`` — no
        model load, no spaCy invocation. This is the primary cost guard:
        most log lines are short and never need NER.

        Each returned :class:`Detection` has ``source="ner"``,
        ``confidence=0.85``, and ``pattern_name`` set to the lowercased
        spaCy label (``"person"`` or ``"org"``).
        """
        if len(text) < min_length:
            return []

        self._load()
        # Type checker: _load() guarantees self._nlp is non-None here.
        doc = self._nlp(text)  # type: ignore[misc]

        hits: list[Detection] = []
        for ent in doc.ents:
            if ent.label_ not in _PII_LABELS:
                continue
            hits.append(
                Detection(
                    pattern_name=ent.label_.lower(),
                    value=ent.text,
                    start=ent.start_char,
                    end=ent.end_char,
                    confidence=_NER_CONFIDENCE,
                    source="ner",
                )
            )
        return hits

    # -- internal --------------------------------------------------------

    def _load(self) -> None:
        """Idempotently load the spaCy model into ``self._nlp``.

        Safe to call multiple times; the second call is effectively a
        no-op. Production code calls this once during FastAPI lifespan
        startup so request latency doesn't include the ~200 ms model
        warm-up.
        """
        if self._nlp is None:
            import spacy  # imported lazily to keep test import cheap
            logger.info("Loading spaCy model %r", self.model_name)
            self._nlp = spacy.load(self.model_name)
