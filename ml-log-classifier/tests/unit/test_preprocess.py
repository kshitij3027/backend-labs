"""Unit tests for :mod:`src.preprocess` (Commit 3).

These exercise the log-text preprocessing pipeline that turns a noisy, timestamped
``raw_log`` line into clean, normalized text for TF-IDF: regex stripping of
high-cardinality noise (timestamps, IPv4/IPv6, UUIDs, latency, ports, ``key=<id>``
tokens), lowercasing, whitespace normalization, NLTK tokenization, and stopword /
punctuation / pure-numeric removal — plus the three public-API guarantees
(idempotency on clean text, empty/None coercion, importable regex constants) and an
end-to-end pass over records straight from :func:`src.log_generator.generate_logs`.

When these run **inside Docker** the real NLTK ``punkt`` + ``stopwords`` corpora are
present (``NLTK_DATA=/usr/share/nltk_data``), so the genuine tokenizer + stopword
path is exercised — not the regex/built-in fallback. The stopword-removal test below
is the canary for that: it asserts true English stopwords (``the``/``with``/``a``/
``after``) are dropped while the content words survive.

Notes on intentional behavior (do *not* tighten these into failures):
* The structured severity word inside the text (``error``/``warn``/``info`` ...) is
  deliberately KEPT as a plain-text token — we never assert it is removed.
* ``clean_text`` may still contain punctuation (``[`` ``]`` ``=`` ``/``); that is
  stripped only in :func:`~src.preprocess.tokenize` / :func:`~src.preprocess.preprocess`.
"""

from __future__ import annotations

import re

import pytest

from src.log_generator import generate_logs
from src.preprocess import (
    HEX_RE,
    IPV4_RE,
    IPV6_RE,
    KV_ID_RE,
    LATENCY_RE,
    NUM_RE,
    TIMESTAMP_RE,
    UUID_RE,
    clean_text,
    preprocess,
    tokenize,
)

# ---------------------------------------------------------------------------
# Detector regexes used by the assertions (independent of the module's own).
# ---------------------------------------------------------------------------

#: Any ISO-style date fragment ``YYYY-MM-DD`` — must never survive cleaning.
_DATE_FRAGMENT_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
#: A clock fragment ``HH:MM:SS`` — must never survive cleaning.
_TIME_FRAGMENT_RE = re.compile(r"\d{1,2}:\d{2}:\d{2}")
#: The leading fragment of a canonical UUID (``8hex-4hex``) — a UUID-leak canary.
_UUID_FRAGMENT_RE = re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}\b")
#: A dotted IPv4 quad.
_IPV4_QUAD_RE = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")


# ---------------------------------------------------------------------------
# (1) Timestamp stripping.
# ---------------------------------------------------------------------------


def test_iso_timestamp_removed():
    text = "2026-06-21T15:32:10.123Z database connection failed"
    cleaned = clean_text(text)
    processed = preprocess(text)

    for out in (cleaned, processed):
        assert "2026" not in out, f"year leaked: {out!r}"
        assert not _DATE_FRAGMENT_RE.search(out), f"date fragment leaked: {out!r}"
        assert not _TIME_FRAGMENT_RE.search(out), f"time fragment leaked: {out!r}"
        # No ':'-bearing clock fragment at all.
        assert ":" not in out, f"':' leaked: {out!r}"
    # The genuine words survive.
    assert "database" in processed and "connection" in processed


def test_space_separated_timestamp_removed():
    # The syslog-ish space-separated form must go too.
    out = clean_text("2026-06-21 15:32:10 cache server restarted")
    assert "2026" not in out and not _TIME_FRAGMENT_RE.search(out)
    assert "cache" in out and "server" in out


# ---------------------------------------------------------------------------
# (2) IP addresses.
# ---------------------------------------------------------------------------


def test_ipv4_removed():
    out = clean_text("connection refused by gateway 10.12.34.56 route upstream")
    assert "10.12.34.56" not in out
    assert not _IPV4_QUAD_RE.search(out), f"IPv4 quad leaked: {out!r}"
    assert "gateway" in out and "upstream" in out


def test_ipv6_removed():
    out = clean_text("tls handshake failed with peer 2001:db8:85a3::8a2e:370:7334 cipher")
    assert "2001" not in out
    assert "::" not in out and "db8" not in out, f"IPv6 leaked: {out!r}"
    assert "handshake" in out


# ---------------------------------------------------------------------------
# (3) UUID.
# ---------------------------------------------------------------------------


def test_uuid_removed():
    uuid = "a1b2c3d4-e5f6-4789-a012-3456789abcde"
    out = clean_text(f"deadlock detected txn {uuid} on table orders")
    assert uuid not in out
    assert not _UUID_FRAGMENT_RE.search(out), f"UUID fragment leaked: {out!r}"
    assert "deadlock" in out and "orders" in out


# ---------------------------------------------------------------------------
# (4) Latency, ports, and key=<id> tokens.
# ---------------------------------------------------------------------------


def test_latency_port_and_id_tokens_removed():
    text = (
        "upstream timed out after 4523ms on host port :5432 "
        "conn_id=ab12cd34ef req_id=99887766"
    )
    out = clean_text(text)
    assert "4523ms" not in out and not re.search(r"\d+\s*ms\b", out)
    assert ":5432" not in out and "5432" not in out
    # The opaque id values are gone ...
    assert "ab12cd34ef" not in out and "99887766" not in out
    # ... and so are the bare keys (whole key=value token consumed).
    assert "conn_id" not in out and "req_id" not in out
    # Genuine words survive.
    assert "upstream" in out and "host" in out


# ---------------------------------------------------------------------------
# (5) Lowercasing.
# ---------------------------------------------------------------------------


def test_output_is_lowercased():
    text = "2026-06-21T15:32:10.123Z [ERROR] WEB Upstream Connection TIMED OUT"
    processed = preprocess(text)
    # Output equals its own lowercase, and an uppercase input is lowered.
    assert processed == processed.lower()
    assert "upstream" in processed
    assert "UPSTREAM" not in processed and "TIMED" not in processed


# ---------------------------------------------------------------------------
# (6) Whitespace normalization.
# ---------------------------------------------------------------------------


def test_whitespace_collapsed_and_trimmed():
    out = clean_text("   web    service\tconnection\n\nlost   to   replica   ")
    # No leading/trailing space, and no run of 2+ internal whitespace chars.
    assert out == out.strip()
    assert "  " not in out
    assert "\t" not in out and "\n" not in out
    assert out.split() == ["web", "service", "connection", "lost", "to", "replica"]


# ---------------------------------------------------------------------------
# (7) Stopword removal — REAL NLTK when run inside Docker.
# ---------------------------------------------------------------------------


def test_stopwords_removed_real_nltk():
    text = "the database connection failed with a timeout after the error"
    tokens = tokenize(text)

    # English stopwords dropped (NLTK's corpus covers all of these; so does the
    # built-in fallback — but in Docker this is the real NLTK path).
    for stop in ("the", "with", "a", "after"):
        assert stop not in tokens, f"stopword {stop!r} survived: {tokens}"
    # Content words kept (the severity word 'error' is intentionally retained).
    for content in ("database", "connection", "timeout", "error"):
        assert content in tokens, f"content word {content!r} dropped: {tokens}"


# ---------------------------------------------------------------------------
# (8) Punctuation and pure-numeric tokens.
# ---------------------------------------------------------------------------


def test_punctuation_and_numeric_tokens_dropped():
    text = "request handled [GET] /api/v1/users status = 200 took 1234ms rows 9999"
    tokens = tokenize(clean_text(text))
    # No pure-punctuation tokens and no all-digit tokens.
    for tok in tokens:
        assert tok not in ("[", "]", "=", "/"), f"punct token survived: {tokens}"
        assert not tok.isdigit(), f"numeric token survived: {tok!r} in {tokens}"
    # The route word and the verb survive as text.
    assert "request" in tokens and "handled" in tokens


# ---------------------------------------------------------------------------
# (9) Idempotency.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text",
    [
        "2026-06-21T15:32:10.123Z [ERROR] web Upstream timed out 4523ms req_id=ab12cd34",
        "Database connection failed with timeout error conn_id=99aa88bb",
        "TLS handshake failed with peer 10.0.0.1:443 cipher_mismatch",
        "the quick brown fox",
        "",
    ],
)
def test_preprocess_and_clean_text_idempotent(text):
    once_p = preprocess(text)
    assert preprocess(once_p) == once_p, "preprocess not idempotent"

    once_c = clean_text(text)
    assert clean_text(once_c) == once_c, "clean_text not idempotent"


# ---------------------------------------------------------------------------
# (10) Empty / None / whitespace coercion.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad", ["", "   ", "\t\n  ", None])
def test_empty_none_whitespace_coerced_to_empty(bad):
    assert preprocess(bad) == ""
    assert clean_text(bad) == ""
    assert tokenize(bad) == []


# ---------------------------------------------------------------------------
# (11) Regex constants importable and behaving.
# ---------------------------------------------------------------------------


def test_regex_constants_importable_and_behave():
    assert UUID_RE.search("a1b2c3d4-e5f6-4789-a012-3456789abcde")
    assert IPV4_RE.search("10.0.0.1")
    assert IPV6_RE.search("2001:db8:85a3::8a2e:370:7334")
    assert TIMESTAMP_RE.search("2026-06-21T15:32:10.123Z")
    assert LATENCY_RE.search("took 4523ms")
    assert KV_ID_RE.search("req_id=ab12cd34ef")
    assert HEX_RE.search("deadbeef12")
    assert NUM_RE.search("rows 9999")
    # Negative: a plain word is not mistaken for a number or an IP.
    assert not NUM_RE.search("database")
    assert not IPV4_RE.search("not.an.ip.here")


# ---------------------------------------------------------------------------
# (12) Generator integration — preprocess real generated records.
# ---------------------------------------------------------------------------


def test_preprocess_generated_records_are_clean():
    records = generate_logs(count=200, seed=42)
    # Sample a spread across the corpus.
    sample = records[:: max(1, len(records) // 25)]
    assert sample, "no records sampled"

    for rec in sample:
        out = preprocess(rec["raw_log"])
        assert out == out.lower(), f"not lowercased: {out!r}"
        # ERROR/INFO records always carry descriptive text, so they tokenize to
        # at least one surviving content word.
        if rec["severity"] in ("ERROR", "INFO"):
            assert out, f"empty preprocess for {rec['severity']} log: {rec['raw_log']!r}"
        # No high-cardinality noise leaked through.
        assert "2026" not in out, f"timestamp leaked: {out!r}"
        assert not _UUID_FRAGMENT_RE.search(out), f"uuid leaked: {out!r}"
        assert "conn_id" not in out and "req_id" not in out, f"id token leaked: {out!r}"
        assert not _IPV4_QUAD_RE.search(out), f"ipv4 leaked: {out!r}"
