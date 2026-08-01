"""Unit tests for src/keys.py — the exact key strings, the calendar, and the label collapse.

This is the heaviest unit-test surface in the project, and it is heavy for a specific reason: every
string built here becomes a Redis key that a Lua script reads and writes. A typo in a prefix does
not raise anything — it silently creates a *second*, empty key, so the caller gets a fresh full
bucket or a zeroed quota and the limiter reports success while enforcing nothing. Nothing downstream
can catch that; only asserting the literal strings can.

Three groups of assertions carry most of the weight:

* the exact key strings from the plan's schema table, written out as literals rather than rebuilt
  from the same f-strings the module uses (an assertion that recomputes the value it is checking
  passes just as happily after both sides are broken together);
* the hash-tag invariant — one user's bucket, window, quotas and record must all tag identically,
  because the four-gate decision script touches all of them in one EVALSHA;
* the calendar edges, where every quota reset either lands on the right instant or hands somebody a
  free period.
"""

from datetime import date, datetime, timedelta, timezone

import pytest

from src import keys
from src.config import DEFAULT_COST_CATEGORY
from src.keys import (
    APIKEY_DIGEST_LEN,
    CLASSIFY_CACHE_SIZE,
    CONFIG_TIERS_KEY,
    CONFIG_VERSION_KEY,
    DEFAULT_ENDPOINT_CATEGORY,
    MS_PER_HOUR,
    MS_PER_MINUTE,
    ROUTE_TABLE,
    UNKNOWN_ENDPOINT_LABEL,
    apikey_key,
    bucket_key,
    classify,
    daily_quota_key,
    day_expire_at,
    day_string,
    hash_tag,
    hour_index,
    minute_index,
    month_expire_at,
    month_string,
    monthly_quota_key,
    recent_minute_indices,
    sanitise_user_id,
    sliding_window_key,
    sliding_window_prefix,
    stats_hour_key,
    stats_minute_key,
    stats_top_key,
    user_key,
    window_index,
)

#: A 64-character lower-case hex string, i.e. what `hmac_sha256(...).hexdigest()` produces.
VALID_DIGEST = "a" * 64


# --------------------------------------------------------------------------------------------
# The exact key strings from the plan's schema table
# --------------------------------------------------------------------------------------------


def test_bucket_key_is_the_specs_literal_string():
    """`rate_limit:{user_id}:{endpoint}` — the spec's own key, braces and all."""
    assert (
        bucket_key("alice", "GET:/api/v1/logs/query")
        == "rate_limit:{alice}:GET:/api/v1/logs/query"
    )


def test_sliding_window_prefix_and_key():
    """The prefix is what the script gets; the indexed form is what tests and admin reads use."""
    assert sliding_window_prefix("alice") == "sw:{alice}"
    assert sliding_window_key("alice", 29775511) == "sw:{alice}:29775511"
    # The indexed key must literally be the prefix plus ":<index>", because the Lua script builds
    # it by concatenation from the prefix it was handed. If these two ever disagree, Python and Lua
    # are addressing different keys and the account-wide gate quietly stops being account-wide.
    assert sliding_window_key("alice", 7).startswith(sliding_window_prefix("alice") + ":")


def test_quota_keys_are_the_specs_literal_strings():
    day = date(2026, 8, 10)
    assert daily_quota_key("alice", day) == "quota:daily:{alice}:2026-08-10"
    assert monthly_quota_key("alice", day) == "quota:monthly:{alice}:2026-08"


def test_user_and_apikey_keys():
    assert user_key("alice") == "user:{alice}"
    assert apikey_key(VALID_DIGEST) == f"apikey:v1:{VALID_DIGEST}"


def test_stats_keys_are_the_specs_literal_strings():
    assert stats_minute_key(29775511) == "stats:min:29775511"
    assert stats_hour_key(487459) == "stats:hour:487459"
    assert stats_top_key(29775511) == "stats:top:min:29775511"


def test_config_keys_are_untagged():
    """`config:*` is global and must NOT carry a hash tag.

    Tagging it would pin the tier table to one user's slot, and every replica's config read would
    then land on whichever shard that user hashes to — a hot spot created by a copy-paste.
    """
    assert CONFIG_TIERS_KEY == "config:tiers"
    assert CONFIG_VERSION_KEY == "config:version"
    assert "{" not in CONFIG_TIERS_KEY
    assert "{" not in CONFIG_VERSION_KEY


def test_stats_keys_are_untagged():
    """Same rule for analytics: global, time-bucketed, never touched by the decision script."""
    for key in (stats_minute_key(1), stats_hour_key(1), stats_top_key(1)):
        assert "{" not in key


# --------------------------------------------------------------------------------------------
# The hash tag
# --------------------------------------------------------------------------------------------


def test_hash_tag_wraps_the_user_id_in_literal_braces():
    assert hash_tag("alice") == "{alice}"


def _tag_of(key: str) -> str:
    """Extract the Redis Cluster hash tag from ``key`` the way Redis itself computes it.

    Deliberately reimplemented here (first ``{``, first ``}`` after it) rather than calling
    :func:`src.keys.hash_tag`: this is the *server's* rule, and the point of the test is that our
    builders satisfy it — not that they agree with themselves.
    """
    open_at = key.index("{")
    close_at = key.index("}", open_at)
    return key[open_at + 1 : close_at]


def test_every_user_scoped_key_shares_one_hash_tag():
    """One EVALSHA touches all of these, so in a cluster they must all be in one slot.

    This is the assertion that keeps the four-gate script possible. A cross-slot key set makes the
    script a `CROSSSLOT` error the day this is sharded, and the only fix at that point is splitting
    the atomic decision into several non-atomic ones — i.e. reintroducing exactly the
    charge-a-token-for-a-request-the-quota-then-refuses race the single script exists to prevent.
    """
    day = datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)
    user_keys = [
        bucket_key("alice", "GET:/api/v1/logs/query"),
        sliding_window_key("alice", 29775511),
        daily_quota_key("alice", day),
        monthly_quota_key("alice", day),
        user_key("alice"),
    ]

    tags = {_tag_of(key) for key in user_keys}

    assert tags == {"alice"}


def test_different_users_get_different_hash_tags():
    assert _tag_of(user_key("alice")) != _tag_of(user_key("bob"))


# --------------------------------------------------------------------------------------------
# Brace injection
# --------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "hostile",
    [
        "a}x{b",  # tags as "a" — a caller choosing their own slot
        "alice}:x:{alice",  # tags as "alice" — colliding with a DIFFERENT principal's keys
        "{nested}",
        "trailing}",
        "leading{",
    ],
)
def test_a_user_id_containing_a_brace_is_refused(hostile):
    """Braces are structure, not data — an id containing one can forge another user's slot.

    Redis tags on the FIRST `{`…`}` pair, so `user:{alice}:x:{alice}` computed from the id
    `alice}:x:{alice` tags as `alice` — identical to the real `alice`. A caller who can pick their
    own id could then be made to share, or drain, someone else's bucket.
    """
    with pytest.raises(ValueError, match="brace"):
        sanitise_user_id(hostile)


def test_brace_injection_is_refused_at_every_key_builder():
    """The guard has to sit in `hash_tag`, so no builder can be the one that forgot it."""
    day = date(2026, 8, 10)
    builders = [
        lambda uid: bucket_key(uid, "GET:/api/v1/whoami"),
        sliding_window_prefix,
        lambda uid: sliding_window_key(uid, 1),
        lambda uid: daily_quota_key(uid, day),
        lambda uid: monthly_quota_key(uid, day),
        user_key,
    ]

    for build in builders:
        with pytest.raises(ValueError):
            build("a}x{b")


def test_an_empty_user_id_is_refused():
    """`rate_limit:{}:...` is a valid key with an EMPTY tag — one shared bucket for everyone."""
    with pytest.raises(ValueError, match="non-empty"):
        sanitise_user_id("")


def test_an_ordinary_user_id_passes_through_unchanged():
    """Validation must not rewrite the id: a normalised id meters a different principal."""
    for ordinary in ["alice", "user-42", "a.b@example.com", "01J8ZK9Q", "üser"]:
        assert sanitise_user_id(ordinary) == ordinary


def test_apikey_key_refuses_anything_that_is_not_a_64_char_lowercase_hex_digest():
    """The catastrophic mistake here is passing the RAW key where the digest belongs.

    That would write the caller's plaintext secret into a Redis KEY NAME, which then appears in
    MONITOR output, in the slowlog, in the AOF/RDB and in every backup of them. A raw key is not 64
    lower-case hex characters, so this refuses instead of persisting it.
    """
    for bad in [
        "demo-free-key",  # the actual failure mode: a raw API key
        "",
        "abc",
        VALID_DIGEST[:-1],  # 63 chars
        VALID_DIGEST + "a",  # 65 chars
        "A" * 64,  # upper case — hexdigest() is lower case, so this came from elsewhere
        "g" * 64,  # right length, not hex
    ]:
        with pytest.raises(ValueError, match="digest"):
            apikey_key(bad)

    assert APIKEY_DIGEST_LEN == 64


# --------------------------------------------------------------------------------------------
# Time-bucket arithmetic
# --------------------------------------------------------------------------------------------


def test_minute_and_hour_indices_are_floor_division():
    assert MS_PER_MINUTE == 60_000
    assert MS_PER_HOUR == 3_600_000

    assert minute_index(0) == 0
    assert minute_index(59_999) == 0
    assert minute_index(60_000) == 1
    assert minute_index(1_786_530_660_000) == 29_775_511

    assert hour_index(0) == 0
    assert hour_index(3_599_999) == 0
    assert hour_index(3_600_000) == 1


def test_an_hour_index_covers_exactly_sixty_minute_indices():
    """The two ladders must agree, or the dashboard's hourly roll-up double-counts a boundary."""
    epoch_ms = 1_786_530_660_000
    assert hour_index(epoch_ms) == minute_index(epoch_ms) // 60


def test_window_index_buckets_by_the_configured_width():
    assert window_index(0, 60_000) == 0
    assert window_index(59_999, 60_000) == 0
    assert window_index(60_000, 60_000) == 1
    # A non-default window width still buckets correctly (SLIDING_WINDOW_SEC is configurable).
    assert window_index(30_000, 10_000) == 3


def test_window_index_refuses_a_non_positive_width():
    """A bare ZeroDivisionError from inside the limiter names neither the setting nor the caller."""
    with pytest.raises(ValueError, match="window_ms"):
        window_index(1_000, 0)
    with pytest.raises(ValueError, match="window_ms"):
        window_index(1_000, -60_000)


def test_recent_minute_indices_is_contiguous_and_descending():
    assert recent_minute_indices(100, 3) == [100, 99, 98]
    # Contiguity is the property the arithmetic buys us over SCAN: consecutive integers, no gaps.
    result = recent_minute_indices(29_775_511, 10)
    assert len(result) == 10
    assert result[0] == 29_775_511
    assert all(earlier - later == 1 for earlier, later in zip(result, result[1:]))


def test_recent_minute_indices_respects_the_cap():
    """ANALYTICS_MAX_BUCKETS is a response-size bound; asking for more must not get more."""
    assert len(recent_minute_indices(1_000, 120)) == 120
    assert recent_minute_indices(100, 1) == [100]
    assert recent_minute_indices(100, 0) == []
    assert recent_minute_indices(100, -5) == []


def test_recent_minute_indices_never_walks_below_the_epoch():
    """Clamped at index 0 rather than producing negative bucket names."""
    assert recent_minute_indices(2, 10) == [2, 1, 0]
    assert recent_minute_indices(0, 10) == [0]


def test_recent_minute_indices_refuses_a_negative_latest_index():
    with pytest.raises(ValueError, match="latest_index"):
        recent_minute_indices(-1, 5)


# --------------------------------------------------------------------------------------------
# Period boundaries
# --------------------------------------------------------------------------------------------


def test_day_and_month_strings_are_the_documented_formats():
    moment = datetime(2026, 8, 10, 13, 45, 30, tzinfo=timezone.utc)
    assert day_string(moment) == "2026-08-10"
    assert month_string(moment) == "2026-08"
    # A plain `date` works too — the quota key builders accept either.
    assert day_string(date(2026, 1, 5)) == "2026-01-05"
    assert month_string(date(2026, 1, 5)) == "2026-01"


def test_day_and_month_strings_use_the_utc_calendar_not_the_local_one():
    """An aware non-UTC value is converted before the date is taken.

    23:30 on the 10th in UTC+13 is 10:30 on the 10th in UTC — but 00:30 on the 11th in UTC+13 is
    still the 10th in UTC. Without the conversion, a caller in that timezone would roll into the
    next day's quota key thirteen hours early, i.e. get two daily allowances in one UTC day.
    """
    plus_13 = timezone(timedelta(hours=13))
    just_after_local_midnight = datetime(2026, 8, 11, 0, 30, tzinfo=plus_13)

    assert day_string(just_after_local_midnight) == "2026-08-10"
    assert month_string(datetime(2026, 9, 1, 0, 30, tzinfo=plus_13)) == "2026-08"


def test_a_naive_datetime_is_read_as_utc():
    naive = datetime(2026, 8, 10, 23, 59, 59)
    aware = datetime(2026, 8, 10, 23, 59, 59, tzinfo=timezone.utc)
    assert day_string(naive) == day_string(aware)
    assert day_expire_at(naive) == day_expire_at(aware)


def test_day_expire_at_lands_on_the_next_utc_midnight():
    expected = int(datetime(2026, 8, 11, tzinfo=timezone.utc).timestamp())

    assert day_expire_at(datetime(2026, 8, 10, 0, 0, 0, tzinfo=timezone.utc)) == expected
    assert day_expire_at(datetime(2026, 8, 10, 12, 0, 0, tzinfo=timezone.utc)) == expected
    # The interesting one: one second before rollover must still target tomorrow, not today.
    assert day_expire_at(datetime(2026, 8, 10, 23, 59, 59, tzinfo=timezone.utc)) == expected


def test_day_expire_at_is_always_strictly_in_the_future():
    """An EXPIREAT in the past deletes the key immediately — a free daily allowance on the spot."""
    for moment in [
        datetime(2026, 8, 10, 0, 0, 0, tzinfo=timezone.utc),
        datetime(2026, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
        datetime(2028, 2, 29, 23, 59, 59, tzinfo=timezone.utc),  # leap day
    ]:
        assert day_expire_at(moment) > int(moment.timestamp())


def test_day_expire_at_crosses_a_month_and_a_year_boundary():
    assert day_expire_at(datetime(2026, 8, 31, 20, 0, tzinfo=timezone.utc)) == int(
        datetime(2026, 9, 1, tzinfo=timezone.utc).timestamp()
    )
    assert day_expire_at(datetime(2026, 12, 31, 20, 0, tzinfo=timezone.utc)) == int(
        datetime(2027, 1, 1, tzinfo=timezone.utc).timestamp()
    )


def test_month_expire_at_lands_on_the_first_of_next_month():
    assert month_expire_at(datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)) == int(
        datetime(2026, 9, 1, tzinfo=timezone.utc).timestamp()
    )


def test_month_expire_at_rolls_december_into_january_of_the_next_year():
    """The branch a naive `month + 1` gets wrong by producing month 13."""
    assert month_expire_at(datetime(2026, 12, 31, 23, 59, 59, tzinfo=timezone.utc)) == int(
        datetime(2027, 1, 1, tzinfo=timezone.utc).timestamp()
    )


def test_month_expire_at_handles_uneven_month_lengths():
    """The bug `timedelta(days=31)` produces, asserted from both directions.

    From 31 January, +31 days lands on 3 March — skipping February's key entirely, so February's
    monthly counter would expire before the month it belongs to even started. From a 31-day month
    into a 30-day one, +31 days lands on the 2nd rather than the 1st. Months are not arithmetic on
    days.
    """
    assert month_expire_at(datetime(2026, 1, 31, 23, 0, tzinfo=timezone.utc)) == int(
        datetime(2026, 2, 1, tzinfo=timezone.utc).timestamp()
    )
    assert month_expire_at(datetime(2026, 3, 31, 23, 0, tzinfo=timezone.utc)) == int(
        datetime(2026, 4, 1, tzinfo=timezone.utc).timestamp()
    )
    # Leap February -> March.
    assert month_expire_at(datetime(2028, 2, 29, 23, 0, tzinfo=timezone.utc)) == int(
        datetime(2028, 3, 1, tzinfo=timezone.utc).timestamp()
    )


def test_month_expire_at_is_always_strictly_in_the_future():
    for moment in [
        datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        datetime(2026, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
    ]:
        assert month_expire_at(moment) > int(moment.timestamp())


def test_the_quota_key_and_its_expiry_describe_the_same_period():
    """The key names a period and the EXPIREAT ends it — they must not disagree.

    A `quota:daily:{alice}:2026-08-10` key whose TTL lands on the 12th would keep a spent counter
    alive across the 11th's fresh key, and a caller's daily allowance would silently never reset.
    """
    moment = datetime(2026, 8, 10, 23, 59, 59, tzinfo=timezone.utc)
    expires = datetime.fromtimestamp(day_expire_at(moment), tz=timezone.utc)

    assert daily_quota_key("alice", moment).endswith(day_string(moment))
    assert expires.date() == moment.date() + timedelta(days=1)
    assert (expires.hour, expires.minute, expires.second) == (0, 0, 0)


# --------------------------------------------------------------------------------------------
# Endpoint classification
# --------------------------------------------------------------------------------------------


def test_known_routes_classify_to_their_documented_label_and_category():
    assert classify("GET", "/api/v1/logs/query") == ("GET:/api/v1/logs/query", "logs_query")
    assert classify("POST", "/api/v1/logs/ingest") == ("POST:/api/v1/logs/ingest", "logs_ingest")
    assert classify("GET", "/api/v1/whoami") == ("GET:/api/v1/whoami", "default")


def test_a_parameterised_route_classifies_to_its_TEMPLATE_not_its_path():
    assert classify("GET", "/api/v1/logs/42") == ("GET:/api/v1/logs/{id}", "default")


def test_two_different_log_ids_collapse_to_the_same_label():
    """The unbounded-key-generator guard, stated as one equality.

    The endpoint label is part of `rate_limit:{user}:<label>`. If the label were the raw path, then
    `/logs/1`, `/logs/2`, `/logs/3`, ... would each mint a brand-new Redis hash — with its own TTL
    and its own FULL bucket — on every request. Two consequences, both bad: unbounded Redis growth
    that any client can trigger (a denial of service against the component every request depends
    on), and an unlimited allowance for anyone willing to vary the path.
    """
    assert classify("GET", "/api/v1/logs/1") == classify("GET", "/api/v1/logs/456")
    assert bucket_key("alice", classify("GET", "/api/v1/logs/1")[0]) == bucket_key(
        "alice", classify("GET", "/api/v1/logs/456")[0]
    )


def test_exact_routes_win_over_the_parameterised_template():
    """`query` is a perfectly good path segment, so table ORDER is what keeps this right.

    If `/logs/{id}` were tried first, the project's most expensive endpoint would be relabelled and
    RE-PRICED from 5 tokens down to 1.
    """
    label, category = classify("GET", "/api/v1/logs/query")
    assert label == "GET:/api/v1/logs/query"
    assert category == "logs_query"


def test_unknown_paths_collapse_to_a_single_other_label():
    for path in ["/", "/nope", "/api/v1/nope", "/api/v2/logs/query", "/api/v1/logs/1/extra"]:
        assert classify("GET", path) == (UNKNOWN_ENDPOINT_LABEL, DEFAULT_ENDPOINT_CATEGORY)
    assert UNKNOWN_ENDPOINT_LABEL == "other"


def test_an_unbounded_stream_of_novel_paths_yields_exactly_one_label():
    """The bound restated as a measurement rather than as an example."""
    labels = {classify("GET", f"/attack/{n}")[0] for n in range(500)}
    assert labels == {"other"}


def test_the_method_is_part_of_the_classification():
    """A POST to a GET route is not that route — it must not inherit its cheap or costly price."""
    assert classify("POST", "/api/v1/logs/query") == ("other", "default")
    assert classify("GET", "/api/v1/logs/ingest") == ("GET:/api/v1/logs/{id}", "default")


def test_the_method_is_case_insensitive():
    assert classify("get", "/api/v1/whoami") == classify("GET", "/api/v1/whoami")


def test_a_trailing_slash_does_not_create_a_second_endpoint():
    """Starlette redirects `/x/` -> `/x`, but the limiter runs ABOVE the router and sees the raw
    path — so without normalisation the same endpoint would carry two buckets and two allowances a
    caller could alternate between."""
    assert classify("GET", "/api/v1/whoami/") == classify("GET", "/api/v1/whoami")
    assert classify("GET", "/api/v1/logs/query/") == ("GET:/api/v1/logs/query", "logs_query")
    # The root is not stripped to the empty string.
    assert classify("GET", "/") == ("other", "default")
    assert classify("GET", "//") == classify("GET", "/")


#: Every trailing byte a caller can append to a path without changing which handler Starlette runs.
#: All of them are reachable percent-encoded: `%0A`, `%0D%0A`, `%09`, `%20`, `%00`.
PADDING = ["\n", "\r\n", "\r", "\t", " ", "\x00", " \n", "\x7f"]


def test_a_trailing_newline_does_not_dodge_the_expensive_endpoint_cost():
    """**A pricing bypass, and the reason the classifier normalises before it matches.**

    Starlette compiles each route to a `$`-anchored regex and matches it with `re.match`, and
    Python's `$` also matches immediately before a trailing newline — so `GET /api/v1/logs/query%0A`
    (the ASGI scope carries the percent-DECODED path) is routed to the real query handler. If the
    classifier disagreed and called that request `{id}`/`default`, the caller would be *served* the
    project's most expensive endpoint and *charged* 1 token instead of 5: the whole weighted-cost
    feature defeated by one URL-encoded character, available to anyone who can reach the service.

    Being stricter on one side of that boundary is what opens the hole, so the path is normalised
    first and matched with `fullmatch` afterwards.
    """
    label, category = classify("GET", "/api/v1/logs/query\n")

    assert (label, category) == ("GET:/api/v1/logs/query", "logs_query")
    assert category != DEFAULT_ENDPOINT_CATEGORY  # i.e. still the 5-token price, not the 1-token one


@pytest.mark.parametrize("pad", PADDING, ids=lambda p: repr(p))
def test_trailing_control_characters_never_change_what_a_path_costs(pad: str):
    """The same property across every trailing byte, not just the newline that motivated it.

    `\\r\\n`, a tab and a space are the same class of input as `%0A` and get the same answer; pinning
    one and not the others would leave the rule looking like a special case for one character.
    """
    assert classify("GET", f"/api/v1/logs/query{pad}") == ("GET:/api/v1/logs/query", "logs_query")
    assert classify("POST", f"/api/v1/logs/ingest{pad}") == (
        "POST:/api/v1/logs/ingest",
        "logs_ingest",
    )
    assert classify("GET", f"/api/v1/whoami{pad}") == ("GET:/api/v1/whoami", "default")
    assert classify("GET", f"/api/v1/logs/42{pad}") == ("GET:/api/v1/logs/{id}", "default")
    # Padding does not rescue an unknown path into a known one either.
    assert classify("GET", f"/nope{pad}") == (UNKNOWN_ENDPOINT_LABEL, DEFAULT_ENDPOINT_CATEGORY)


@pytest.mark.parametrize("pad", PADDING, ids=lambda p: repr(p))
@pytest.mark.parametrize(
    "path",
    ["/api/v1/logs/query", "/api/v1/logs/ingest", "/api/v1/whoami", "/api/v1/logs/42", "/nope", "/"],
)
def test_padding_cannot_split_the_classifier_from_the_router(path: str, pad: str):
    """**The invariant, asserted as an equality.**

    This function decides what a request COSTS; Starlette decides what it DOES. Any input the two
    classify differently is a pricing bypass — served endpoint X, charged for endpoint Y. Making the
    classification invariant under padding closes that gap for every padding byte at once, without
    the classifier having to reimplement the router's quirks.

    Measured against the installed Starlette, per padding byte appended to `/api/v1/logs/query`:

    * `\\n` — its `$`-anchored `.match()` accepts it, so the request is served by the REAL query
      handler. This is the live bypass: without normalisation the classifier would price that
      request as `{id}`/`default`, i.e. 1 token for the 5-token endpoint. Here the two agree.
    * `\\r\\n`, `\\r`, `\\t`, space, `\\x00`, `\\x7f` — the router falls through to its own `{id}`
      route (or 404s, for a non-parameterised path). The classifier charges the clean route's price,
      which is the same or MORE than what was served. Conservative, and conservative is the only
      safe direction: over-charging a padded request is a bug report, under-charging it is a bypass
      anyone can use.

    Written as `classify(m, padded) == classify(m, clean)` rather than against hard-coded labels on
    purpose: it keeps holding when the route table changes, and it is the assertion that would catch
    a future tightening on one side of the classifier/router boundary but not the other.
    """
    assert classify("GET", path + pad) == classify("GET", path)


@pytest.mark.parametrize("pad", PADDING, ids=lambda p: repr(p))
def test_padding_a_path_does_not_mint_a_second_key(pad: str):
    """Normalisation must not reintroduce the unbounded-key-generator this module exists to prevent.

    The label is still the TEMPLATE, so a padded `/logs/1` and a plain `/logs/2` share one label and
    therefore one Redis key — otherwise every `%0A`-suffixed variant would be a fresh hash with a
    fresh full bucket, which is both the memory bug and the allowance bug in one.
    """
    assert classify("GET", f"/api/v1/logs/1{pad}") == classify("GET", "/api/v1/logs/2")
    assert bucket_key("alice", classify("GET", f"/api/v1/logs/1{pad}")[0]) == bucket_key(
        "alice", classify("GET", "/api/v1/logs/2")[0]
    )
    labels = {classify("GET", f"/api/v1/logs/{n}{pad}")[0] for n in range(200)}
    assert labels == {"GET:/api/v1/logs/{id}"}


def test_an_interior_control_character_is_not_absorbed_into_a_path_parameter():
    """A control byte inside a segment must not be swallowed by `{id}`'s character class.

    It collapses to `other` instead — which carries the same `default` category the router's own
    `{id}` route would charge, so the two still agree on price while the classifier refuses to
    pretend a control byte is an ordinary log id.
    """
    assert classify("GET", "/api/v1/logs/4\n2") == (
        UNKNOWN_ENDPOINT_LABEL,
        DEFAULT_ENDPOINT_CATEGORY,
    )
    assert classify("GET", "/api/v1/logs/4\n2")[1] == classify("GET", "/api/v1/logs/42")[1]


def test_a_path_of_nothing_but_padding_is_classified_without_raising():
    """The degenerate input still has to produce a label rather than an IndexError."""
    assert classify("GET", "\n") == (UNKNOWN_ENDPOINT_LABEL, DEFAULT_ENDPOINT_CATEGORY)
    assert classify("GET", "") == (UNKNOWN_ENDPOINT_LABEL, DEFAULT_ENDPOINT_CATEGORY)


def test_classify_is_memoised_and_bounded():
    """Bounded is the load-bearing half: the cache key is caller-chosen input.

    An unbounded memo table over `(method, path)` is the very same unbounded-growth bug the `other`
    collapse fixes, merely relocated from Redis into this process's heap. Two bounds, two resources.
    """
    classify.cache_clear()

    first = classify("GET", "/api/v1/whoami")
    second = classify("GET", "/api/v1/whoami")

    info = classify.cache_info()
    assert first == second
    assert info.hits >= 1
    assert info.misses == 1
    assert info.maxsize == CLASSIFY_CACHE_SIZE
    assert CLASSIFY_CACHE_SIZE > 0

    # And the bound actually holds under a flood of novel paths.
    for n in range(CLASSIFY_CACHE_SIZE * 2):
        classify("GET", f"/flood/{n}")
    assert classify.cache_info().currsize <= CLASSIFY_CACHE_SIZE


def test_every_route_table_label_is_bucket_key_safe():
    """A label is spliced straight into a Redis key, so it must carry no brace of its own."""
    for route in ROUTE_TABLE:
        assert "{" not in route.label.split(":", 1)[0]
        built = bucket_key("alice", route.label)
        assert built.startswith("rate_limit:{alice}:")
        # The template's own `{id}` braces come AFTER the tag, so the tag is still just the user.
        assert _tag_of(built) == "alice"


def test_the_default_category_agrees_with_the_config_modules_declaration():
    """Two declarations of one contract, asserted equal instead of imported.

    `keys.py` stays dependency-free (it is the module a human reads to answer "what is in Redis?"),
    and `config.py` requires an ENDPOINT_COSTS entry for this category — if the two names ever
    drift, the cost lookup for an unclassified request becomes a KeyError raised inside the
    middleware, on the hot path, for a route nobody thought about.
    """
    assert DEFAULT_ENDPOINT_CATEGORY == DEFAULT_COST_CATEGORY


def test_every_route_category_is_priceable():
    """Every category the classifier can emit must exist in the default ENDPOINT_COSTS table."""
    from src.config import DEFAULT_ENDPOINT_COSTS_SPEC, parse_endpoint_costs

    costs = parse_endpoint_costs(DEFAULT_ENDPOINT_COSTS_SPEC)
    emitted = {route.category for route in ROUTE_TABLE} | {DEFAULT_ENDPOINT_CATEGORY}

    assert emitted <= set(costs)


#: Modules `src/keys.py` must never import. The first four can do I/O; the last two can read a
#: clock. Either would mean this module could no longer be reasoned about — or unit tested — from
#: its arguments alone.
FORBIDDEN_IMPORTS = frozenset({"redis", "os", "socket", "asyncio", "httpx", "time", "random"})


def test_keys_module_is_pure():
    """`keys.py` is pure by contract; the cheapest way to keep it that way is to assert it.

    Checked against the module's own namespace rather than its source text, so a mention in a
    docstring cannot fail the test and a real import cannot pass it.
    """
    assert FORBIDDEN_IMPORTS.isdisjoint(vars(keys))


def test_every_key_builder_is_a_total_function_of_its_arguments():
    """Same inputs, same outputs — no hidden clock anywhere in the module.

    A builder that read the clock internally would still look correct in a single assertion and
    would only misbehave across a period boundary, in production, at midnight UTC.
    """
    moment = datetime(2026, 8, 10, 23, 59, 59, tzinfo=timezone.utc)

    def snapshot() -> tuple[object, ...]:
        return (
            bucket_key("alice", "GET:/api/v1/whoami"),
            sliding_window_key("alice", 29_775_511),
            daily_quota_key("alice", moment),
            monthly_quota_key("alice", moment),
            user_key("alice"),
            day_expire_at(moment),
            month_expire_at(moment),
            minute_index(1_786_530_660_000),
            hour_index(1_786_530_660_000),
            tuple(recent_minute_indices(29_775_511, 5)),
            classify("GET", "/api/v1/logs/query"),
        )

    assert snapshot() == snapshot()
