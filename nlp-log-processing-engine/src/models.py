"""Domain models for the NLP Log Processing Engine.

This module is the single source of truth for the API's request/response vocabulary:

* **Requests** — :class:`AnalyzeRequest` (one line) and :class:`BatchAnalyzeRequest`
  (many lines).
* **Health** — :class:`HealthResponse`, the frozen liveness contract.
* **Analysis (C7)** — the composed :class:`AnalysisResponse`
  (``{message, entities[], intent{label, confidence}, sentiment{label, score},
  keywords[]}``) built from :class:`Entity`, :class:`IntentResult` and
  :class:`SentimentResult`, plus the :class:`BatchAnalyzeResponse` envelope
  (``{results[], count}``). These are the ``response_model`` FastAPI validates the
  :class:`~src.nlp.NLPEngine` output against on ``/api/analyze`` and ``/api/analyze/batch``.
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


class Entity(BaseModel):
    """One recognised entity in a message.

    ``text`` and ``label`` are always present; ``start`` / ``end`` are the character offsets
    into the original message (``message[start:end] == text``) and are optional so the model
    also accepts a lighter ``{text, label}`` entity when offsets are not carried.
    """

    text: str
    label: str
    start: int | None = None
    end: int | None = None


class IntentResult(BaseModel):
    """The classified intent and its confidence (max class probability in ``[0, 1]``)."""

    label: str
    confidence: float


class SentimentResult(BaseModel):
    """The severity/sentiment label and its raw VADER compound score in ``[-1, 1]``."""

    label: str
    score: float


class AnalysisResponse(BaseModel):
    """Full analysis of one log line — the ``POST /api/analyze`` response body."""

    message: str
    entities: list[Entity]
    intent: IntentResult
    sentiment: SentimentResult
    keywords: list[str]


class BatchAnalyzeResponse(BaseModel):
    """Envelope for ``POST /api/analyze/batch`` — per-line results plus their ``count``."""

    results: list[AnalysisResponse]
    count: int
