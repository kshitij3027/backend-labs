"""The NLP layer of the log-processing engine — one analyzer per capability.

Each capability lives in its own module so it can be built, unit-tested and reasoned
about in isolation:

* :mod:`src.nlp.entity`    — spaCy NER + deterministic log-entity rules (C3).
* ``src.nlp.intent``       — TF-IDF + LogisticRegression intent classifier (C4).
* ``src.nlp.sentiment``    — VADER sentiment/severity with an ops lexicon (C5).
* ``src.nlp.keyword``      — YAKE keyword extraction + trending aggregation (C6).

C7 turns this package into the ``NLPEngine`` orchestrator: it loads the four analyzers
once (sharing a single spaCy model instance via ``EntityAnalyzer.nlp``), exposes a
``.ready`` flag, and offers ``analyze(msg)`` / ``analyze_batch(msgs)``. This ``__init__``
is intentionally kept empty until then so C7 can populate it without colliding with C3.
"""
