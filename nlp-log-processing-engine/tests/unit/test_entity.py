"""Unit tests for the entity-recognition analyzer (:mod:`src.nlp.entity`).

These pin the behaviour C10 later scores for NER recall: every log-specific label is
extracted with its exact surface and char offsets, log labels win over general spaCy
labels on overlap (an IP is never left as ``CARDINAL``), the statistical ``ner`` still
fires for general entities, odd input never raises, and — the fidelity gate — the analyzer
recovers the ground-truth entities emitted by the corpus generator.

The spaCy model load is expensive, so a single :class:`EntityAnalyzer` is built once per
module and shared by every test.
"""

import pytest

from src.generators import sample_messages
from src.nlp.entity import EntityAnalyzer, LOG_LABELS


@pytest.fixture(scope="module")
def analyzer() -> EntityAnalyzer:
    """One configured analyzer for the whole module (amortises the model load)."""
    return EntityAnalyzer()


def _has(result: list[dict], text: str, label: str) -> bool:
    """True if some entity has exactly this surface and label."""
    return any(e["text"] == text and e["label"] == label for e in result)


def _pairs(result: list[dict]) -> set[tuple[str, str]]:
    """The ``(text, label)`` set an analysis produced."""
    return {(e["text"], e["label"]) for e in result}


# --------------------------------------------------------------------------------------
# Per-label extraction
# --------------------------------------------------------------------------------------
def test_ip_is_extracted_and_not_left_as_cardinal(analyzer):
    result = analyzer.analyze("connection from 10.52.44.216 refused")
    # The dotted-quad is a single IP entity with its exact surface...
    assert _has(result, "10.52.44.216", "IP")
    # ...not split, and not left as a general CARDINAL number.
    assert not any(e["label"] == "CARDINAL" for e in result)


def test_url_is_extracted_with_exact_surface(analyzer):
    result = analyzer.analyze("posted to https://api.example.com/v1/pay")
    assert _has(result, "https://api.example.com/v1/pay", "URL")


def test_path_is_extracted(analyzer):
    result = analyzer.analyze("reading /var/log/app.log failed")
    assert _has(result, "/var/log/app.log", "PATH")


def test_service_gazetteer_is_extracted(analyzer):
    result = analyzer.analyze("payments-api rejected the request")
    assert _has(result, "payments-api", "SERVICE")


def test_error_code_is_extracted(analyzer):
    result = analyzer.analyze("raised error E4012 downstream")
    assert _has(result, "E4012", "ERROR_CODE")


def test_user_id_contextual_bare_digits(analyzer):
    # Bare digits become USER_ID only via the "user"/"for"/"by" context, not on their own.
    result = analyzer.analyze("invalid password for user 4821")
    assert _has(result, "4821", "USER_ID")


def test_user_id_shape_token(analyzer):
    result = analyzer.analyze("u_1002 signed in")
    assert _has(result, "u_1002", "USER_ID")


def test_port_is_extracted(analyzer):
    result = analyzer.analyze("listening on port 5432")
    assert _has(result, "5432", "PORT")


def test_host_is_extracted(analyzer):
    result = analyzer.analyze("restarted on web-03")
    assert _has(result, "web-03", "HOST")


# --------------------------------------------------------------------------------------
# Priority / overlap: a log label must win over a general span at the same offset
# --------------------------------------------------------------------------------------
@pytest.mark.parametrize(
    "message, surface, label",
    [
        ("invalid password for user 4821", "4821", "USER_ID"),
        ("listening on port 5432", "5432", "PORT"),
        ("connection from 10.52.44.216 refused", "10.52.44.216", "IP"),
    ],
)
def test_log_label_wins_and_has_no_overlapping_general_span(analyzer, message, surface, label):
    result = analyzer.analyze(message)
    hits = [e for e in result if e["text"] == surface]
    # The surface exists and carries the log label (not CARDINAL / a general label).
    assert hits, f"{surface!r} not extracted from {message!r}: {result}"
    assert all(e["label"] == label for e in hits)
    # Nothing else overlaps that offset span — the log entity owns it outright.
    target = hits[0]
    overlapping = [
        e
        for e in result
        if e is not target and e["start"] < target["end"] and target["start"] < e["end"]
    ]
    assert overlapping == [], f"unexpected overlap on {surface!r}: {overlapping}"


# --------------------------------------------------------------------------------------
# Char offsets round-trip
# --------------------------------------------------------------------------------------
def test_char_offsets_are_exact(analyzer):
    messages = [
        "connection from 10.52.44.216 refused",
        "reading /var/log/app.log failed on web-03 port 5432",
        "posted to https://api.example.com/v1/pay by user 4821",
        "payments-api returned E503 while writing to /data/db/wal",
    ]
    for message in messages:
        for e in analyzer.analyze(message):
            assert message[e["start"]:e["end"]] == e["text"]


# --------------------------------------------------------------------------------------
# The statistical ner component is still active for general entities
# --------------------------------------------------------------------------------------
def test_general_ner_still_runs(analyzer):
    result = analyzer.analyze("The outage in London affected Amazon and Google teams.")
    general = [e for e in result if e["label"] not in LOG_LABELS]
    assert general, f"expected at least one general spaCy entity, got {result}"


# --------------------------------------------------------------------------------------
# Robustness: empty / whitespace input never raises and returns []
# --------------------------------------------------------------------------------------
@pytest.mark.parametrize("bad", ["", "   ", "\n\t "])
def test_empty_and_whitespace_input(analyzer, bad):
    assert analyzer.analyze(bad) == []


def test_analyze_batch_matches_analyze_and_handles_empties(analyzer):
    messages = [
        "restarted on web-03 port 5432",
        "",
        "failed login for user 9930 from 10.1.2.3 returned E404",
    ]
    batch = analyzer.analyze_batch(messages)
    assert len(batch) == len(messages)
    assert batch[1] == []  # empty item -> empty result
    # Batch and single-item analysis agree per line.
    for message, got in zip(messages, batch):
        assert got == analyzer.analyze(message)


# --------------------------------------------------------------------------------------
# Ground-truth recall smoke test (soft fidelity gate — the C10 metric in miniature)
# --------------------------------------------------------------------------------------
def test_ground_truth_recall_on_corpus(analyzer):
    samples = sample_messages(30, seed=13)

    total_gt = 0
    total_hit = 0
    per_sample: list[float] = []
    misses: list[tuple[str, tuple[str, str]]] = []

    for sample in samples:
        found = _pairs(analyzer.analyze(sample.message))
        truth = {(surface, label) for surface, label in sample.entities}
        hit = truth & found
        total_gt += len(truth)
        total_hit += len(hit)
        per_sample.append(len(hit) / len(truth))
        misses.extend((sample.message, pair) for pair in (truth - found))

    aggregate = total_hit / total_gt
    # Strong-but-safe aggregate bar: the rules recover essentially all corpus entities.
    assert aggregate >= 0.8, f"aggregate NER recall {aggregate:.2%} too low; misses={misses}"
    # And no single line falls below a reasonable fraction of its own entities.
    assert min(per_sample) >= 0.6, (
        f"worst-line recall {min(per_sample):.2%} too low; misses={misses}"
    )
