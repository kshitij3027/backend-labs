"""The pure helpers ``scripts/verify_e2e.py``'s gates rest on.

The verifier itself runs under ``make e2e``, against a live stack, and that is the right place for
it: it is a black-box harness and mocking the box out would leave it testing itself. But the
**arithmetic** underneath its gates is ordinary code, and it decides whether a run passes. A
percentile that indexed one element off, or a comparison that read ``>=`` where the harness's own
docstring says ``>``, would move every ceiling in the project by one sample or one unit — silently,
and only in the direction of passing.

So this module imports the script and exercises the functions its own section comment marks as
"kept free of I/O and of module state" for exactly this purpose. Nothing here opens a socket.

.. rubric:: Importing the script is safe, and that is a property worth keeping

``scripts/verify_e2e.py`` builds an ``httpx.Client`` at module scope, which parses a base URL and
allocates a connection pool — it does not resolve DNS and does not connect. Everything else at
module scope is constants read from the environment. If an import of this module ever starts
requiring a reachable ``TARGET_URL``, this file fails at collection, which is the correct place to
find out.
"""

from __future__ import annotations

import hashlib

import pytest

from scripts.verify_e2e import (
    CheckFailure,
    _family_total,
    _metric_samples,
    error_codes,
    gate,
    percentile,
    require_int,
    require_list,
    sha256_hex,
    unique_suffix,
)

# =================================================================================================
# percentile — ceil-rank, and the reported value is always one that was observed
# =================================================================================================


def test_an_empty_sample_is_zero_rather_than_an_error() -> None:
    """A gate with nothing to grade must not explode inside the harness's own arithmetic."""
    assert percentile([], 95) == 0.0
    assert percentile([], 50) == 0.0


@pytest.mark.parametrize(
    ("pct", "expected"),
    [
        # ceil-rank over 1..10: index = ceil(pct/100 * 10) - 1.
        (10, 1.0),
        (50, 5.0),
        (90, 9.0),
        (95, 10.0),
        (100, 10.0),
    ],
)
def test_the_rank_is_ceiling_based(pct: float, expected: float) -> None:
    """The exact definition, on a sample where every plausible method gives a different answer.

    A linear-interpolation p95 over 1..10 is 9.55, and a floor-rank one is 9.0. Only ceil-rank
    returns 10.0 — so this parametrisation distinguishes the three rather than merely checking that
    a number came back.
    """
    assert percentile([float(n) for n in range(1, 11)], pct) == expected


@pytest.mark.parametrize("pct", [1, 25, 50, 75, 95, 99, 100])
def test_every_reported_percentile_is_a_value_that_was_actually_observed(pct: float) -> None:
    """The property the ceil-rank choice exists for, asserted as a property.

    A latency gate reporting 97.3 ms when no request took 97.3 ms invites an argument about the
    method instead of about the latency. Interpolation would break this for most of these inputs.
    """
    samples = [12.5, 3.25, 99.0, 41.75, 7.0, 60.5, 88.125]

    assert percentile(samples, pct) in samples


def test_the_input_is_sorted_rather_than_assumed_sorted() -> None:
    """Latency samples arrive in the order they were measured, which is not sorted order."""
    assert percentile([5.0, 1.0, 3.0], 50) == 3.0
    assert percentile([5.0, 1.0, 3.0], 100) == 5.0


def test_a_single_sample_is_that_sample_at_every_percentile() -> None:
    """``E2E_LATENCY_SAMPLES=1`` is a legal, if useless, configuration; it must not index off."""
    assert percentile([42.0], 50) == 42.0
    assert percentile([42.0], 95) == 42.0
    assert percentile([42.0], 100) == 42.0


def test_the_index_never_falls_off_either_end() -> None:
    """The two boundaries the ``max(0, ...)`` and the ``- 1`` exist for."""
    samples = [1.0, 2.0, 3.0, 4.0]

    assert percentile(samples, 0.0001) == 1.0, "a tiny percentile is the smallest observation"
    assert percentile(samples, 100) == 4.0, "the top percentile is the largest, not an IndexError"


def test_the_input_sequence_is_not_mutated() -> None:
    """The caller keeps its samples: ``check_perf_and_memory`` takes p50 and p95 off one list."""
    samples = [9.0, 1.0, 5.0]

    percentile(samples, 95)

    assert samples == [9.0, 1.0, 5.0]


# =================================================================================================
# gate — one comparison for every ceiling in the harness
# =================================================================================================


def test_a_measurement_under_the_ceiling_passes_and_returns_evidence() -> None:
    """The evidence string is what a PASS line prints, so it has to carry both numbers."""
    evidence = gate(120.0, 250.0, "sequential logs(limit: 50) p95")

    assert "120.0ms" in evidence
    assert "250.0ms" in evidence
    assert "sequential logs(limit: 50) p95" in evidence
    assert "<=" in evidence


def test_a_measurement_over_the_ceiling_raises_check_failure() -> None:
    """A gate that returned a string on failure would print PASS with damning evidence in it."""
    with pytest.raises(CheckFailure) as raised:
        gate(251.0, 250.0, "sequential p95")

    message = str(raised.value)
    assert "251.0ms" in message and "250.0ms" in message
    assert "sequential p95" in message


def test_the_comparison_is_strictly_greater_so_the_boundary_passes() -> None:
    """Exactly at the ceiling is within it. The boundary is a decision, not an accident.

    ``MAX_P95_MS=250`` means "250 is acceptable"; a ``>=`` here would fail a run that met its
    stated budget exactly, and the two spellings are indistinguishable on every other input.
    """
    assert gate(250.0, 250.0, "at the boundary")

    with pytest.raises(CheckFailure):
        gate(250.000001, 250.0, "a hair over")


def test_a_zero_ceiling_cannot_be_met_by_any_real_measurement() -> None:
    """The docstring's own falsifiability claim: ``MAX_P95_MS=0 make e2e`` must fail.

    That is what makes the gate demonstrably a gate — a ceiling nobody can drive to failure from
    outside is decoration. A zero *reading* against a zero ceiling would pass, and never happens.
    """
    with pytest.raises(CheckFailure):
        gate(0.1, 0.0, "any real latency")


def test_the_unit_is_carried_into_the_message() -> None:
    """The memory gate reports MiB and the latency gates report ms, through one function."""
    evidence = gate(412.0, 600.0, "backend RSS", unit="MiB")

    assert "412.0MiB" in evidence and "600.0MiB" in evidence
    assert "ms" not in evidence

    with pytest.raises(CheckFailure) as raised:
        gate(700.0, 600.0, "backend RSS", unit="MiB")

    assert "MiB" in str(raised.value)


def test_check_failure_is_an_assertion_error() -> None:
    """``check()`` catches ``CheckFailure`` for a clean one-line FAIL and everything else as
    "unexpected". Being an ``AssertionError`` is what keeps a bare ``assert`` in a check usable."""
    assert issubclass(CheckFailure, AssertionError)


# =================================================================================================
# sha256_hex — the APQ hash the server recomputes
# =================================================================================================


def test_the_hash_is_the_published_sha256_of_the_utf8_bytes() -> None:
    """Compared against ``hashlib`` directly: the server hashes the bytes it received.

    A helper that normalised, trimmed or re-encoded would register a document under one hash and
    send another, and check 9 would fail with a "persisted query not found" that looked like a
    server bug.
    """
    document = "{ logs { id service } }"

    assert sha256_hex(document) == hashlib.sha256(document.encode("utf-8")).hexdigest()
    assert len(sha256_hex(document)) == 64


def test_whitespace_changes_the_hash() -> None:
    """"Exactly as sent" has teeth only if a pretty-printed document hashes differently."""
    assert sha256_hex("{ logs { id } }") != sha256_hex("{ logs { id } }\n")
    assert sha256_hex("{ logs { id } }") != sha256_hex("{logs{id}}")


def test_a_non_ascii_document_hashes_as_utf8() -> None:
    """The encoding is named explicitly, so a default-locale change cannot move every hash."""
    document = '{ logs(filters: {searchText: "日本語"}) { id } }'

    assert sha256_hex(document) == hashlib.sha256(document.encode("utf-8")).hexdigest()


def test_the_empty_document_hashes_to_the_published_sha256_of_nothing() -> None:
    """A fixed vector, so this file cannot be satisfied by an implementation that agrees with
    ``hashlib`` about the wrong thing."""
    assert sha256_hex("") == (
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    )


# =================================================================================================
# unique_suffix — probe isolation
# =================================================================================================


def test_the_suffix_is_short_hex_and_does_not_repeat() -> None:
    """Probe ids are built from this. A collision would make one run's frames another run's."""
    suffixes = {unique_suffix() for _ in range(2000)}

    assert len(suffixes) == 2000, "2000 draws produced a duplicate"
    sample = unique_suffix()
    assert len(sample) == 12
    assert all(character in "0123456789abcdef" for character in sample)


# =================================================================================================
# The shape guards — require_list / require_int
# =================================================================================================


def test_require_list_returns_the_list_and_rejects_everything_else() -> None:
    """A GraphQL ``data`` field that came back ``null`` must fail the check, not the indexing."""
    payload = [1, 2, 3]

    assert require_list(payload, "logs") is payload

    with pytest.raises(CheckFailure) as raised:
        require_list(None, "logs")
    assert "logs" in str(raised.value)

    for wrong in ({"a": 1}, "abc", 7):
        with pytest.raises(CheckFailure):
            require_list(wrong, "logs")


def test_require_int_rejects_booleans_which_are_ints_in_python() -> None:
    """``isinstance(True, int)`` is ``True``, so a ``totalCount`` of ``true`` would sail through.

    That is not hypothetical paranoia: a resolver returning a boolean where a count belongs is
    exactly the kind of regression a black-box check is there to notice, and the naive guard misses
    it.
    """
    assert require_int(0, "totalCount") == 0
    assert require_int(42, "totalCount") == 42

    for wrong in (True, False, 1.0, "7", None):
        with pytest.raises(CheckFailure):
            require_int(wrong, "totalCount")


def test_the_shape_guards_name_the_field_in_their_failure() -> None:
    """A FAIL line is one line; "want an int" without the field name is not diagnosable."""
    with pytest.raises(CheckFailure) as raised:
        require_int("nope", "logStats.totalLogs")

    assert "logStats.totalLogs" in str(raised.value)


# =================================================================================================
# error_codes — reading a GraphQL errors envelope
# =================================================================================================


def test_every_extensions_code_is_returned_in_order() -> None:
    """Order matters: the cost-gate check asserts on the first code, not on membership."""
    body = {
        "errors": [
            {"message": "too big", "extensions": {"code": "COST_LIMIT_EXCEEDED"}},
            {"message": "bad", "extensions": {"code": "VALIDATION_ERROR"}},
        ]
    }

    assert error_codes(body) == ["COST_LIMIT_EXCEEDED", "VALIDATION_ERROR"]


@pytest.mark.parametrize(
    "body",
    [
        pytest.param({}, id="no-errors-key"),
        pytest.param({"errors": None}, id="errors-null"),
        pytest.param({"errors": []}, id="errors-empty"),
    ],
)
def test_a_successful_envelope_has_no_codes(body: dict[str, object]) -> None:
    """The success path runs through this too, so it must not raise on a missing key."""
    assert error_codes(body) == []


def test_an_error_without_extensions_contributes_none_rather_than_raising() -> None:
    """A GraphQL error is not obliged to carry extensions; an unmasked one from a library will not.

    The check that reads this then sees ``[None]`` and fails on the *code it wanted being absent*,
    which is the useful failure, instead of on a ``KeyError`` inside the harness.
    """
    body = {"errors": [{"message": "boom"}, {"message": "x", "extensions": {}}]}

    assert error_codes(body) == [None, None]


# =================================================================================================
# The Prometheus text-exposition reader
# =================================================================================================


EXPOSITION = """\
# HELP gql_operation_duration_seconds How long an operation took.
# TYPE gql_operation_duration_seconds histogram
gql_operation_duration_seconds_bucket{le="0.005",operation="query"} 3.0
gql_operation_duration_seconds_count{operation="query"} 11.0
gql_operation_duration_seconds_sum{operation="query"} 0.42
# TYPE gql_active_subscriptions gauge
gql_active_subscriptions 0.0
gql_broker_published_total 17.0
gql_errors_total{code="VALIDATION_ERROR"} 2.0

"""


def test_help_and_type_lines_and_blank_lines_are_skipped() -> None:
    """A five-line parser is deliberate — it must still know what is not a sample."""
    samples = _metric_samples(EXPOSITION)

    assert not any(name.startswith("#") for name in samples)
    assert "" not in samples
    assert samples["gql_active_subscriptions"] == 0.0
    assert samples["gql_broker_published_total"] == 17.0


def test_labels_stay_in_the_series_name() -> None:
    """The parser is honest about not understanding labels; the family matcher splits on ``{``."""
    samples = _metric_samples(EXPOSITION)

    assert samples['gql_errors_total{code="VALIDATION_ERROR"}'] == 2.0
    assert samples['gql_operation_duration_seconds_count{operation="query"}'] == 11.0


def test_an_unparseable_value_is_skipped_rather_than_fatal() -> None:
    """Scraping is a check, not a parser conformance suite: one odd line must not end the run."""
    samples = _metric_samples("weird_metric NaNsense\ngood_metric 1.0\n")

    assert "weird_metric" not in samples
    assert samples["good_metric"] == 1.0


def test_a_family_is_matched_exactly_and_by_suffix() -> None:
    """``gql_operation_duration_seconds`` must find its ``_count``/``_sum``/``_bucket`` children.

    This is what lets the required-family list name the histogram once instead of naming its three
    derived series, and it is why the non-zero list can name ``..._count`` specifically.
    """
    samples = _metric_samples(EXPOSITION)

    present, total = _family_total(samples, "gql_operation_duration_seconds")
    assert present is True
    assert total == pytest.approx(3.0 + 11.0 + 0.42)

    present, total = _family_total(samples, "gql_operation_duration_seconds_count")
    assert present is True
    assert total == 11.0


def test_a_family_that_is_absent_reports_absent_rather_than_zero() -> None:
    """Presence and non-emptiness fail for different reasons — a missing family means the
    exposition is reading the wrong registry, and a zero one means the instrument is never
    recorded into. Collapsing them would lose that distinction."""
    present, total = _family_total(_metric_samples(EXPOSITION), "gql_cache_hits_total")

    assert present is False
    assert total == 0.0


def test_a_gauge_at_zero_is_present_but_totals_zero() -> None:
    """``gql_active_subscriptions`` is legitimately 0 once every socket has closed, which is why it
    is required to be present and deliberately not required to be non-zero."""
    present, total = _family_total(_metric_samples(EXPOSITION), "gql_active_subscriptions")

    assert present is True
    assert total == 0.0


def test_a_labelled_series_is_found_by_its_family_name() -> None:
    """``gql_errors_total{code="..."}`` must count towards ``gql_errors_total``."""
    present, total = _family_total(_metric_samples(EXPOSITION), "gql_errors_total")

    assert present is True
    assert total == 2.0


def test_a_family_name_is_not_matched_by_a_mere_prefix() -> None:
    """``gql_broker_published_total`` must not be satisfied by ``gql_broker_published_totally``.

    The matcher splits on ``_``-suffix boundaries rather than using ``startswith`` on the raw name,
    which is what stops one instrument from vouching for a differently-named one.
    """
    samples = _metric_samples("gql_broker_publishedX 5.0\ngql_broker_published_totalish 9.0\n")

    present, _total = _family_total(samples, "gql_broker_published_total")
    assert present is False
