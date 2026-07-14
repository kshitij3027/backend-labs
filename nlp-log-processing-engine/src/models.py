"""Domain models for the NLP Log Processing Engine.

This module is the single source of truth for the API's request/response vocabulary.
C1 defines only what the scaffold needs: the two request bodies
(:class:`AnalyzeRequest`, :class:`BatchAnalyzeRequest`) and the health response
(:class:`HealthResponse`).

The response models that describe a full analysis — ``Entity``, ``IntentResult``,
``SentimentResult`` and the composed ``AnalysisResponse`` (``{message, entities[],
intent{label, confidence}, sentiment{label, score}, keywords[]}``) — are finalized in
C7, when the NLP engine and the ``/api/analyze`` routes land. They are intentionally
omitted here to keep C1 minimal; adding them later does not disturb these request shapes.
"""

from __future__ import annotations

from pydantic import BaseModel


class AnalyzeRequest(BaseModel):
    """Body of ``POST /api/analyze`` (C7) — a single free-text log line to analyze."""

    message: str


class BatchAnalyzeRequest(BaseModel):
    """Body of ``POST /api/analyze/batch`` (C7) — a batch of lines analyzed via nlp.pipe."""

    messages: list[str]


class HealthResponse(BaseModel):
    """Shape of the ``GET /api/health`` body — the frozen liveness contract.

    ``analyzer_ready`` is ``True`` in C1 (no engine yet) and reflects ``engine.ready``
    once the NLP engine is wired (C7).
    """

    status: str
    analyzer_ready: bool
