"""Every Redis key this service touches, plus the arithmetic behind the ones that carry time.

.. rubric:: This module is PURE

No I/O, no ``redis`` import, no ``datetime.now()``, no ``time.time()``. Every function is a total
function of its arguments, and every function that needs the current time takes it as a parameter.

That is not stylistic tidiness. It is the reason this module is the one a human can read to answer
"what will actually be in Redis?" without booting anything, and the reason the unit suite can
hammer it — a key builder that reads the clock internally can only be tested by mocking the clock,
and a key builder that opens a socket cannot be tested at all. The decision script's *behaviour* is
proved against real Redis in ``tests/integration/``; the *names of the keys it operates on* are
proved here, in microseconds, for every edge of the calendar.

.. rubric:: The braces are not a placeholder — they are a Redis Cluster hash tag

The spec writes its keys as ``rate_limit:{user_id}:{endpoint}``, using braces the way documentation
usually does: "substitute the user id here". Redis Cluster reads the *same characters* as a **hash
tag** — when a key contains a ``{...}`` pair, the slot is computed from the bytes *between* the
first ``{`` and the first following ``}`` rather than from the whole key.

So the literal braces are kept. They are simultaneously the spec's format and the property that
makes this shardable: ``rate_limit:{alice}:GET:/x``, ``sw:{alice}:29775511``,
``quota:daily:{alice}:2026-08-10``, ``quota:monthly:{alice}:2026-08`` and ``user:{alice}`` all hash
to the identical slot, so the four-gate decision script can touch all of them in one ``EVALSHA``.
Drop the braces "because they look like a placeholder" and the script becomes ``CROSSSLOT`` the day
this is sharded — which is exactly the kind of change that is free to make and impossible to undo.

:func:`hash_tag` is the single definition of that invariant; every user-scoped builder below goes
through it. ``config:*`` and ``stats:*`` are deliberately **untagged**: they are global, they are
never touched by the decision script, and tagging them would pin every replica's analytics writes
to one slot.

.. rubric:: Everything is UTC

Quota periods roll over at UTC midnight, the daily key is a UTC date, the monthly key is a UTC
month. UTC is the only defensible default for a multi-replica system: the replicas do not share a
timezone, a caller does not share one with them either, and "the daily quota resets at midnight"
has to mean one instant that every process agrees on. A local-time boundary would give a caller in
UTC+13 a different reset moment than the replica that serves them, and DST would hand out one
25-hour day and one 23-hour day per year — a free 4% and a silent 4% overcharge.

Naive datetimes are read as UTC rather than rejected, because that is what every internal caller
produces (the limiter's clock is ``redis.call('TIME')``, which is UTC epoch seconds).
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from types import MappingProxyType

# --------------------------------------------------------------------------------------------
# Global (untagged) keys
# --------------------------------------------------------------------------------------------

#: HASH — field = tier name, value = ``rpm|burst|daily|monthly``. The live tier table, seeded with
#: ``HSETNX`` (C3) so an operator's runtime change survives a replica restart.
CONFIG_TIERS_KEY = "config:tiers"

#: STRING — integer, ``INCR``-ed on every write to :data:`CONFIG_TIERS_KEY`. Lets a replica notice
#: its cached snapshot is stale with one cheap read instead of re-fetching the whole table.
CONFIG_VERSION_KEY = "config:version"

# --------------------------------------------------------------------------------------------
# Time-bucket constants
# --------------------------------------------------------------------------------------------

#: Milliseconds per minute / hour. Named because the same two literals appear in the Lua script
#: (C4/C9) and a mismatch between the two would put Python's stats reads and Lua's stats writes in
#: different buckets — a dashboard that is silently always empty.
MS_PER_MINUTE = 60_000
MS_PER_HOUR = 3_600_000

# --------------------------------------------------------------------------------------------
# Endpoint classification
# --------------------------------------------------------------------------------------------

#: Label given to any request the route table does not recognise. See :func:`classify` for the
#: (load-bearing) reason this is a single constant rather than the request's real path.
UNKNOWN_ENDPOINT_LABEL = "other"

#: Cost category charged for anything without a more specific one. Must agree with
#: ``src.config.DEFAULT_COST_CATEGORY`` — the two are asserted equal in the unit suite rather than
#: imported from one another, so this module stays dependency-free and the drift is still caught.
DEFAULT_ENDPOINT_CATEGORY = "default"

#: Bound on :func:`classify`'s memo table. See the function docstring: this bound protects *process
#: memory*, and is a different bound from the one protecting *Redis memory*.
CLASSIFY_CACHE_SIZE = 1024

#: Methods that are classified **as another method**, applied before :data:`ROUTE_TABLE` is
#: consulted. Exactly one entry today, and it is a pricing rule rather than a convenience — see the
#: "HEAD is classified as GET" rubric on :func:`classify` for the bypass it closes.
#:
#: A mapping rather than an ``if`` so that the set of aliases is one readable list. Nothing else
#: belongs here on current evidence: ``OPTIONS`` never reaches the classifier (CORS answers a
#: preflight above the limiter, and an unlisted ``OPTIONS`` 405s), and ``TRACE`` / ``CONNECT`` are
#: not served at all. A future alias must clear the same bar this one does: *the router dispatches
#: it to the aliased method's handler*, so charging it the aliased method's price is what makes the
#: classifier and the router agree.
METHOD_ALIASES: Mapping[str, str] = MappingProxyType({"HEAD": "GET"})

#: Length and alphabet of an ``apikey:v1:`` digest — SHA-256 hex, lower case. Enforced by
#: :func:`apikey_key`; see there for why this validation is a security control and not a nicety.
APIKEY_DIGEST_LEN = 64
_HEX_DIGEST_RE = re.compile(r"[0-9a-f]+")


@dataclass(frozen=True, slots=True)
class RouteTemplate:
    """One row of the route table: how to recognise a path, and what to call it.

    ``label`` is the *template*, never the matched path — ``GET:/api/v1/logs/{id}``, not
    ``GET:/api/v1/logs/42``. That distinction is the whole point of the table (see
    :func:`classify`).
    """

    method: str
    pattern: re.Pattern[str]
    label: str
    category: str


#: The compiled route table, **in priority order — first match wins**.
#:
#: Order is load-bearing, not incidental: ``/api/v1/logs/query`` also matches the ``{id}`` template
#: (``query`` is a perfectly good path segment), so the exact routes must be tried first. Reordering
#: these entries would relabel and, worse, *re-price* the project's most expensive endpoint — the
#: ``logs_query`` category costs 5 tokens per request and ``default`` costs 1.
ROUTE_TABLE: tuple[RouteTemplate, ...] = (
    RouteTemplate(
        method="GET",
        pattern=re.compile(r"^/api/v1/logs/query$"),
        label="GET:/api/v1/logs/query",
        category="logs_query",
    ),
    RouteTemplate(
        method="POST",
        pattern=re.compile(r"^/api/v1/logs/ingest$"),
        label="POST:/api/v1/logs/ingest",
        category="logs_ingest",
    ),
    RouteTemplate(
        method="GET",
        pattern=re.compile(r"^/api/v1/whoami$"),
        label="GET:/api/v1/whoami",
        category=DEFAULT_ENDPOINT_CATEGORY,
    ),
    # Parameterised, and therefore the reason the table exists at all. Every distinct log id must
    # collapse onto this ONE label.
    #
    # The character class excludes control characters as well as `/`. A plain `[^/]+` matches a
    # newline, so `/api/v1/logs/query\n` would be absorbed as an {id} — quietly re-pricing the
    # project's most expensive endpoint at the `default` rate. A control byte is never part of a
    # legitimate log id, so it must not be silently swallowed by a path parameter. (A trailing one
    # is already removed by `_strip_trailing_noise`; this covers the interior case, which lands on
    # `other` — the same `default` category the router's own {id} route would charge, so the two
    # still agree on price.)
    RouteTemplate(
        method="GET",
        pattern=re.compile(r"^/api/v1/logs/[^/\x00-\x1f\x7f]+$"),
        label="GET:/api/v1/logs/{id}",
        category=DEFAULT_ENDPOINT_CATEGORY,
    ),
)


# --------------------------------------------------------------------------------------------
# User-id validation and the hash tag
# --------------------------------------------------------------------------------------------


def sanitise_user_id(user_id: str) -> str:
    """Return ``user_id`` unchanged, or raise :class:`ValueError` if it cannot be safely tagged.

    Two rules, and the second one is a real vulnerability rather than hygiene.

    **Empty ids are refused.** ``rate_limit:{}:GET:/x`` is a syntactically fine key with an empty
    hash tag, and every principal with a missing id would share one bucket — either a global rate
    limit nobody asked for, or (with a generous tier) an unmetered pool.

    **Braces are refused.** Redis Cluster computes the slot from the bytes between the **first**
    ``{`` and the first ``}`` that follows it. A user id of ``a}x{b`` inside ``user:{a}x{b}``
    produces the tag ``a`` — so a caller who can choose their own id can choose their own slot, and
    more importantly can *collide with someone else's*. ``alice`` and ``alice}:x:{alice`` would tag
    identically, which means one caller's requests can be made to drain another caller's bucket, or
    to land on a key another caller's script also writes. The braces are structure here, not data,
    so data containing them is refused rather than escaped.

    Raising rather than rewriting is deliberate. Silently normalising ``a}b`` to ``a_b`` changes
    *which principal* is being metered, and a rate limiter that quietly meters the wrong account is
    worse than one that returns an error naming the problem. Ids reaching this function come from
    the API-key record or a JWT ``sub`` (C5), so a rejected id is a data defect worth surfacing.
    """
    if not user_id:
        raise ValueError("user_id must be a non-empty string — an empty Redis hash tag would put "
                         "every unidentified principal in one shared bucket")
    if "{" in user_id or "}" in user_id:
        raise ValueError(
            f"user_id {user_id!r} contains a brace — braces delimit the Redis Cluster hash tag, "
            "so an id containing one can forge or collide with another principal's key slot"
        )
    return user_id


def hash_tag(user_id: str) -> str:
    """Return the braced hash tag for ``user_id`` — ``alice`` -> ``{alice}``.

    THE single definition of the invariant described in this module's docstring. Every user-scoped
    key below is built from this, so "everything for one user lands in one slot" is one line of
    code that can be changed once, rather than a convention six f-strings are each expected to
    remember.
    """
    return "{" + sanitise_user_id(user_id) + "}"


# --------------------------------------------------------------------------------------------
# Key builders
# --------------------------------------------------------------------------------------------


def bucket_key(user_id: str, endpoint_label: str) -> str:
    """Token-bucket key for one ``(user, endpoint)`` pair.

    ``bucket_key("alice", "GET:/api/v1/logs/query")`` -> ``rate_limit:{alice}:GET:/api/v1/logs/query``

    ``endpoint_label`` must come from :func:`classify`, never from a raw request path — that is what
    keeps the number of bucket keys bounded by the size of :data:`ROUTE_TABLE`.
    """
    return f"rate_limit:{hash_tag(user_id)}:{endpoint_label}"


def sliding_window_prefix(user_id: str) -> str:
    """Account-wide sliding-window key **without** its window index — ``alice`` -> ``sw:{alice}``.

    The decision script appends ``:<index>`` itself, for both the current and the previous window,
    because the index is derived from ``redis.call('TIME')`` *inside* the script. Passing a
    Python-computed index as a KEY would reintroduce the client clock the design removed: a replica
    whose clock is 40 s fast would write into a window the other replicas are not reading yet, and
    the account-wide gate would silently become per-replica.
    """
    return f"sw:{hash_tag(user_id)}"


def sliding_window_key(user_id: str, window_index: int) -> str:
    """One concrete sliding-window counter — ``sw:{alice}:29775511``.

    Used by tests and by the admin usage read-out. The hot path uses
    :func:`sliding_window_prefix` and lets the script do the arithmetic.
    """
    return f"{sliding_window_prefix(user_id)}:{window_index}"


def daily_quota_key(user_id: str, day: date) -> str:
    """Daily quota counter — ``quota:daily:{alice}:2026-08-10``.

    ``day`` may be a :class:`~datetime.date` or a :class:`~datetime.datetime` (a ``datetime`` *is* a
    ``date``); a ``datetime`` is converted to its UTC date first, so an aware non-UTC value cannot
    land in the wrong day's key.
    """
    return f"quota:daily:{hash_tag(user_id)}:{day_string(day)}"


def monthly_quota_key(user_id: str, day: date) -> str:
    """Monthly quota counter — ``quota:monthly:{alice}:2026-08``. Same ``day`` rules as above."""
    return f"quota:monthly:{hash_tag(user_id)}:{month_string(day)}"


def user_key(user_id: str) -> str:
    """Principal record — ``user:{alice}`` — HASH of ``tier``, ``status``, ``created_at``.

    Read *inside* the decision script, which is what makes a tier reassignment take effect on the
    very next request on every replica while the meaning of a tier is allowed a 5 s cache.
    """
    return f"user:{hash_tag(user_id)}"


def apikey_key(digest_hex: str) -> str:
    """API-key record — ``apikey:v1:<hmac_sha256_hex>``.

    ``v1`` is a scheme version, not decoration: it is what lets the pepper or the digest algorithm
    be rotated by writing ``apikey:v2:`` records alongside the old ones instead of by a migration
    that must be atomic with a deploy.

    The digest is validated as exactly 64 lower-case hex characters, and that check is a **security
    control**. The one catastrophic mistake available at this call site is passing the caller's raw
    API key where the digest belongs — which would write the plaintext secret into a Redis key name,
    where it lands in ``MONITOR`` output, in slowlog entries, in an RDB/AOF file, and in every
    backup of them. A raw key is not 64 lower-case hex characters, so this refuses instead of
    silently persisting it. Case matters too: ``hexdigest()`` is lower case, so an upper-case value
    came from somewhere else and is not the digest this scheme means.
    """
    if len(digest_hex) != APIKEY_DIGEST_LEN or not _HEX_DIGEST_RE.fullmatch(digest_hex):
        raise ValueError(
            f"api key digest must be {APIKEY_DIGEST_LEN} lower-case hex characters "
            f"(got {len(digest_hex)} chars) — passing a RAW api key here would write the "
            "plaintext secret into a Redis key name"
        )
    return f"apikey:v1:{digest_hex}"


def stats_minute_key(minute_index: int) -> str:
    """Per-minute analytics bucket — ``stats:min:29775511`` (HASH, ``EXPIRE 3600 NX``)."""
    return f"stats:min:{minute_index}"


def stats_hour_key(hour_index: int) -> str:
    """Per-hour analytics bucket — ``stats:hour:487459`` (HASH, ``EXPIRE 604800 NX``)."""
    return f"stats:hour:{hour_index}"


def stats_top_key(minute_index: int) -> str:
    """Per-minute top-consumers ZSET — ``stats:top:min:29775511`` (member=user, score=cost).

    A ZSET rather than a sort over the minute HASH: ``ZREVRANGE 0 9`` is ``O(log N + 10)`` on
    Redis's single thread, where reading the whole hash back and sorting it in Python is ``O(N)``
    transfer plus ``O(N log N)`` — on the endpoint the dashboard polls every 5 seconds.
    """
    return f"stats:top:min:{minute_index}"


# --------------------------------------------------------------------------------------------
# Time-bucket arithmetic
# --------------------------------------------------------------------------------------------


def minute_index(epoch_ms: int) -> int:
    """Minute bucket containing ``epoch_ms``: ``epoch_ms // 60_000``.

    Floor division, and Python's flooring semantics are what make it correct: ``//`` rounds toward
    negative infinity, so the buckets stay contiguous and equally sized across the epoch rather
    than doubling in width at zero the way C-style truncation would.
    """
    return epoch_ms // MS_PER_MINUTE


def hour_index(epoch_ms: int) -> int:
    """Hour bucket containing ``epoch_ms``: ``epoch_ms // 3_600_000``."""
    return epoch_ms // MS_PER_HOUR


def window_index(epoch_ms: int, window_ms: int) -> int:
    """Sliding-window bucket containing ``epoch_ms`` for a window of ``window_ms`` milliseconds.

    The Cloudflare weighted-counter algorithm needs this index and the one before it; it weights
    the previous window by how much of it still overlaps the current instant, which is why the
    boundary does not admit a double-sized burst the way a fixed window does.

    ``window_ms`` must be positive: a zero would otherwise surface as a bare ``ZeroDivisionError``
    from inside the limiter, which names neither the setting nor the caller.
    """
    if window_ms <= 0:
        raise ValueError(f"window_ms must be > 0, got {window_ms}")
    return epoch_ms // window_ms


def recent_minute_indices(latest_index: int, count: int) -> list[int]:
    """Return up to ``count`` contiguous minute indices ending at ``latest_index``, **descending**.

    ``recent_minute_indices(100, 3)`` -> ``[100, 99, 98]``.

    Descending — newest first — because that is the order every consumer wants element zero to be:
    "the minute happening right now". A chart that needs oldest-first reverses a list it already
    has; a caller that needs "now" out of an ascending list has to know the length first.

    **This function is why the dashboard never scans.** The alternative way to answer "which minute
    buckets exist?" is ``SCAN MATCH stats:min:*``, which walks the entire keyspace — including
    every ``rate_limit:*``, ``sw:*`` and ``quota:*`` key in the system — on a single-threaded server,
    on the endpoint that is polled every 5 seconds. Bucket names are pure arithmetic, so they can be
    *computed* and then pipelined; a missing bucket comes back as an empty hash, which is the same
    answer a scan would have given and costs one pipelined ``HGETALL`` instead of a keyspace walk.

    ``count`` is clamped so the list never runs below index 0 (the epoch), and a non-positive
    ``count`` yields an empty list rather than raising — asking for zero buckets is a legitimate
    thing for a caller with a zeroed ``ANALYTICS_MAX_BUCKETS`` to do.
    """
    if latest_index < 0:
        raise ValueError(f"latest_index must be >= 0, got {latest_index}")
    if count <= 0:
        return []
    span = min(count, latest_index + 1)
    return [latest_index - offset for offset in range(span)]


def _as_utc_datetime(value: datetime) -> datetime:
    """Return ``value`` as an aware UTC datetime; a naive value is *read as* UTC.

    Reading naive as UTC rather than as local time (Python's own ``astimezone()`` default) is the
    conservative choice for this service: every internal producer of a timestamp here is already
    UTC, and interpreting one as local time would shift a quota boundary by the container's
    ``TZ`` — a bug that is invisible on a UTC laptop and wrong everywhere else.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _as_utc_date(value: date) -> date:
    """Return the UTC calendar date of ``value``, accepting a ``date`` or a ``datetime``."""
    if isinstance(value, datetime):
        return _as_utc_datetime(value).date()
    return value


def day_string(now: date) -> str:
    """UTC day stamp for the daily quota key — ``2026-08-10``."""
    return _as_utc_date(now).isoformat()


def month_string(now: date) -> str:
    """UTC month stamp for the monthly quota key — ``2026-08``.

    Built from the components rather than by slicing ``isoformat()``, so it stays correct for years
    outside the four-digit range instead of silently truncating.
    """
    stamp = _as_utc_date(now)
    return f"{stamp.year:04d}-{stamp.month:02d}"


def day_expire_at(now: datetime) -> int:
    """Unix seconds of the next UTC midnight — the daily quota key's ``EXPIREAT`` argument.

    ``EXPIREAT`` rather than ``EXPIRE <seconds>``: the daily counter must die exactly when its day
    ends, not 86 400 seconds after whichever request happened to create it. A relative TTL applied
    on first write would keep a counter created at 18:00 alive until 18:00 the *next* day, so it
    would still be there — half-spent — when the new day's key is created, and the old day's
    allowance would never actually reset for anyone whose usage straddles the boundary.

    Always strictly in the future, including when called at exactly 00:00:00 (the answer is
    tomorrow's midnight, not this instant). An ``EXPIREAT`` in the past deletes the key immediately,
    which would hand the caller a fresh allowance on the spot.
    """
    aware = _as_utc_datetime(now)
    tomorrow = aware.date() + timedelta(days=1)
    return int(
        datetime(tomorrow.year, tomorrow.month, tomorrow.day, tzinfo=timezone.utc).timestamp()
    )


def month_expire_at(now: datetime) -> int:
    """Unix seconds of 00:00 UTC on the 1st of next month — the monthly key's ``EXPIREAT``.

    Computed by incrementing the month with an explicit December -> January rollover rather than by
    adding ``timedelta(days=31)``. The timedelta version is the classic bug: from 31 January it
    lands on 3 March, skipping February's key entirely, and from any 30-day month it lands on the
    2nd rather than the 1st. Months are not a fixed number of days, so they are not arithmetic on
    days.
    """
    aware = _as_utc_datetime(now)
    year, month = aware.year, aware.month
    if month == 12:
        year, month = year + 1, 1
    else:
        month += 1
    return int(datetime(year, month, 1, tzinfo=timezone.utc).timestamp())


# --------------------------------------------------------------------------------------------
# Endpoint classification
# --------------------------------------------------------------------------------------------


#: Every ASCII character that must not be allowed to change what a path means: the C0 control range
#: ``\x00``-``\x1f`` (which includes ``\t``, ``\n`` and ``\r``), the space at ``\x20``, and ``DEL``
#: at ``\x7f``. Stripped from the END of a path by :func:`_strip_trailing_noise`.
#:
#: These are reachable, not theoretical: the ASGI scope carries the percent-DECODED path, so
#: ``GET /api/v1/logs/query%0A`` arrives as a path with a real newline on the end.
_TRAILING_NOISE = "".join(map(chr, range(0x21))) + "\x7f"


def _strip_trailing_noise(path: str) -> str:
    """Remove trailing ASCII control characters and spaces. THE one definition of that rule.

    Anchored regexes are not enough on their own, and this is where the two halves meet. Python's
    ``$`` matches before a trailing newline, so ``re.match`` accepts ``"/x\\n"`` as ``/x``;
    ``re.fullmatch`` correctly does not. But Starlette's router *does* use ``$``-anchored
    ``.match()``, so it happily routes ``/api/v1/logs/query%0A`` to the real query handler. Tighten
    only the classifier and the two disagree — see :func:`classify` for why that disagreement is a
    pricing bypass rather than a cosmetic difference.

    Normalising first and matching strictly afterwards is what makes both true at once: the padded
    path and the clean path become the same string, so no padding byte can change what a request
    costs, and ``fullmatch`` still refuses to be sloppy about what is left.

    Precisely: for ``\\n`` — the one padding byte the router's ``$`` accepts on an exact route — the
    classifier and the router now agree. For ``\\r\\n``, ``\\t``, space, ``\\x00`` and ``\\x7f`` the
    router falls through to its own ``{id}`` route or 404s, so this charges the same or *more* than
    what was served. That asymmetry is the safe one: over-charging a padded request is a bug report,
    under-charging it is a bypass available to anyone.
    """
    return path.rstrip(_TRAILING_NOISE)


def _normalise_path(path: str) -> str:
    """Strip trailing noise, then a trailing slash (except from the root).

    ``/x`` and ``/x/`` are one endpoint. Starlette's router redirects ``/x/`` to ``/x`` with a 307,
    but the limiter middleware runs *above* the router and sees the raw path — so without this, the
    same endpoint would carry two labels, two buckets, and two independent allowances a caller could
    alternate between.

    Noise is stripped BEFORE the slash, so ``/x/\\n`` collapses onto ``/x`` like every other spelling
    of it rather than surviving as a third one.
    """
    path = _strip_trailing_noise(path)
    if len(path) > 1 and path.endswith("/"):
        return path.rstrip("/") or "/"
    return path


@lru_cache(maxsize=CLASSIFY_CACHE_SIZE)
def classify(method: str, path: str) -> tuple[str, str]:
    """Map an HTTP ``(method, path)`` onto ``(endpoint_label, cost_category)``.

    ``classify("GET", "/api/v1/logs/query")`` -> ``("GET:/api/v1/logs/query", "logs_query")``
    ``classify("GET", "/api/v1/logs/42")``    -> ``("GET:/api/v1/logs/{id}", "default")``
    ``classify("GET", "/anything/else")``     -> ``("other", "default")``

    .. rubric:: Why every unknown path collapses to ONE label

    This is the most important line in the module, and it is a memory-exhaustion fix rather than a
    tidiness preference.

    The endpoint label is a **component of a Redis key** — ``rate_limit:{alice}:<label>``. Use the
    request's raw path as the label and the set of possible keys becomes the set of possible URLs,
    which is unbounded and, critically, *chosen by the caller*. Anyone able to reach the service can
    then issue ``GET /a``, ``GET /b``, ``GET /c``, ... and mint a brand-new hash — each with a
    ~1 hour TTL and its own token allowance — on every single request. Two consequences, both bad:
    Redis's memory grows without limit until the store is evicting or OOM-killed (an unauthenticated
    denial of service against the component every other request depends on), and each fresh key
    arrives with a *full* bucket, so per-path buckets are themselves an unlimited allowance for a
    caller willing to vary the path.

    Collapsing unknowns to ``"other"`` bounds the key space at ``len(ROUTE_TABLE) + 1`` labels per
    user, permanently, regardless of what any caller sends. The parameterised template does the same
    job for routes that *are* known: ``/logs/1`` and ``/logs/2`` are one label, ``GET:/api/v1/logs/{id}``,
    not two keys.

    .. rubric:: Agreeing with the router is a SECURITY property

    This function decides what a request **costs** — ``logs_query`` is 5 tokens, ``default`` is 1.
    Starlette decides what a request **does**. Any input those two classify differently is a pricing
    bypass: the caller is served endpoint X and charged for endpoint Y.

    That is not hypothetical. Starlette compiles each route to a ``$``-anchored regex and matches it
    with ``re.match``, and Python's ``$`` also matches immediately before a trailing newline — so
    ``GET /api/v1/logs/query%0A`` (the ASGI scope carries the percent-DECODED path) is routed to the
    real, expensive query handler. A classifier that used bare ``fullmatch`` would call that same
    request ``GET:/api/v1/logs/{id}`` and charge 1 token instead of 5: the whole weighted-cost
    feature defeated by one URL-encoded character, available to anyone.

    So the path is normalised BEFORE it is matched (:func:`_strip_trailing_noise`) and matched with
    ``fullmatch`` afterwards. Normalisation is what keeps the two in agreement; ``fullmatch`` is what
    keeps the agreement from being an accident of ``$`` semantics. Any future tightening on one side
    of that boundary has to be checked against the other side, and the test that pins it asserts the
    invariant directly: ``classify(m, padded) == classify(m, clean)``.

    .. rubric:: HEAD is classified as GET, because the ROUTER dispatches it as GET

    The same divergence, reached through the **method** instead of the path, and it is worth
    spelling out separately because the route table looks like it already handles the method.

    Starlette's ``Route(methods=["GET"])`` **auto-adds HEAD** — literally
    ``self.methods.add("HEAD")`` in its constructor — so a plain route on a priced path serves
    ``HEAD /api/v1/logs/query`` from the very same 5-token handler as the ``GET``. A table keyed on
    the exact method would call that request ``("other", "default")``: **1 token, on a different
    bucket key than the endpoint it was actually served from.** The caller gets the expensive
    endpoint at the cheap price, and the accounting lands somewhere else entirely, so the
    overspend is invisible in the metering for the endpoint that was served.

    It is latent rather than live in this repository today — every current route is a FastAPI
    ``APIRoute``, which 405s an unlisted method, and the only ``StaticFiles`` mount sits under an
    exempt prefix — and it goes live the moment anyone adds a plain ``Route`` or a ``Mount`` on a
    priced path (C15's dashboard is the obvious candidate). Applying the alias here rather than
    adding ``HEAD`` rows to :data:`ROUTE_TABLE` means the rule holds for **every** row, including
    the ones a later commit adds without having read this docstring.

    It is also simply correct on HTTP's own terms: RFC 9110 §9.3.2 defines HEAD as GET with the
    response body omitted, and the server does the identical work to produce it. Charging it the
    same is the honest price, not a concession.

    .. rubric:: Two different bounds, for two different resources

    ``lru_cache(maxsize=1024)`` bounds the **memo table in this process**. It has to be bounded for
    the same reason the label does: the cache is keyed on ``(method, path)``, and an unbounded cache
    over caller-chosen input is the identical unbounded-growth bug relocated from Redis into the
    API's own heap. The label collapse protects Redis; the LRU bound protects the pod. Neither
    substitutes for the other.

    The cache is worth having because the regex scan runs on every metered request, the working set
    of real paths is tiny (four templates plus whatever probes are in flight), and the result is an
    immutable tuple — so a hit is a dict lookup on the hot path, and a flood of novel paths costs at
    most 1024 entries before it starts evicting itself.
    """
    # Upper-cased first, so the alias table is consulted with the one canonical spelling rather
    # than needing an entry per casing a client might send.
    normalised_method = method.upper()
    normalised_method = METHOD_ALIASES.get(normalised_method, normalised_method)
    normalised_path = _normalise_path(path)
    for route in ROUTE_TABLE:
        # `fullmatch`, not `match`. The patterns are already `$`-anchored, but `$` in Python also
        # matches immediately BEFORE a trailing newline — so `re.match` accepts
        # "/api/v1/logs/query\n" (reachable as `GET /api/v1/logs/query%0A`, since the ASGI scope
        # carries the percent-decoded path) as the real, 5-token route. `fullmatch` requires the
        # pattern to consume the whole string and is simply the correct idiom for "is this path
        # exactly this route".
        if route.method == normalised_method and route.pattern.fullmatch(normalised_path):
            return route.label, route.category
    return UNKNOWN_ENDPOINT_LABEL, DEFAULT_ENDPOINT_CATEGORY
