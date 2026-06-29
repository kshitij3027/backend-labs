"""Synthetic historical-incident corpus generator.

Produces a realistic, deterministic corpus of resolved incidents for the
recommendation engine to match against. Incidents are drawn from a fixed set of
incident **families** (DB connection-pool exhaustion, OOM/memory leak, high
latency, deploy regression, TLS/cert expiry, disk full, auth/token failures,
cache stampede, queue/consumer lag, upstream 5xx spike). Each family owns:

* a small pool of plausible ``service`` names,
* representative ``tags``,
* several **title** and **description** phrasings (deliberately varied wording so
  later semantic retrieval must match *meaning*, not exact text),
* a matching pool of ``resolution`` phrasings,
* a ``severity`` distribution weighted toward what that class of incident tends
  to be in practice.

Because incidents within a family share meaning but not surface wording, the
generated corpus forms natural semantic clusters — exactly the signal the
embedding-based retrieval (C5+) and its tests rely on.

Output is a list of plain ``dict`` rows (keys: ``title``, ``description``,
``service``, ``severity``, ``tags``, ``resolution``, ``created_at``) — the exact
shape :func:`src.db.repository.add_incidents_bulk` consumes. ``created_at`` is
spread across the last ``days_back`` days so later recency scoring has signal.
No embeddings are produced here (that is C5) and this module touches no database
— it is pure.

Generation is fully deterministic for a given ``seed`` (stdlib
:class:`random.Random`), so tests can assert stable output.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from random import Random

from src.schemas import SEVERITIES


@dataclass(frozen=True)
class _Family:
    """A template for one class of semantically-related incidents."""

    key: str                       # short stable id (used only for debugging)
    services: list[str]            # candidate services this family hits
    tags: list[str]                # representative tags (all applied)
    titles: list[str]              # title phrasings (vary the wording)
    descriptions: list[str]        # description phrasings (vary the wording)
    resolutions: list[str]         # resolution phrasings
    # Severity weights, parallel to :data:`src.schemas.SEVERITIES`
    # (critical, high, medium, low). Need not sum to 1 — used as relative weights.
    severity_weights: list[float] = field(
        default_factory=lambda: [0.1, 0.4, 0.4, 0.1]
    )


# --------------------------------------------------------------------------- #
# The incident families. Wording within each family is varied on purpose so that
# semantic search has to recognise meaning rather than match exact strings.
# --------------------------------------------------------------------------- #
_FAMILIES: list[_Family] = [
    _Family(
        key="db_pool_exhaustion",
        services=["orders-api", "payments-api", "database", "users-api"],
        tags=["db", "connection-pool", "timeout"],
        titles=[
            "Database connection pool exhausted",
            "Orders API timing out acquiring DB connections",
            "Postgres connection pool saturated under load",
            "Cannot obtain database connection — pool at limit",
        ],
        descriptions=[
            "Requests began failing with connection-timeout errors; the pool was "
            "fully checked out and new queries queued until they timed out.",
            "Under peak traffic every connection in the pool was in use, so "
            "incoming requests blocked waiting for a free connection and 500'd.",
            "A slow query held connections open long enough to drain the pool, "
            "causing cascading timeouts across dependent endpoints.",
            "The service exhausted its Postgres connection pool; latency spiked "
            "and health checks failed as no connections were available.",
        ],
        resolutions=[
            "Raised the max pool size and added a statement timeout so slow "
            "queries release connections instead of pinning the pool.",
            "Fixed a connection leak where sessions were not closed on the error "
            "path; pool utilisation returned to normal.",
            "Tuned pool_size / max_overflow and added pgbouncer in transaction "
            "mode to absorb bursts.",
        ],
        severity_weights=[0.25, 0.5, 0.2, 0.05],
    ),
    _Family(
        key="oom_memory_leak",
        services=["ingest-worker", "recommend-api", "image-service", "search-api"],
        tags=["memory", "oom", "gc"],
        titles=[
            "Service OOM-killed repeatedly",
            "Memory leak causing pods to be OOMKilled",
            "Out-of-memory crashes in the ingest worker",
            "Heap grows unbounded until the container is killed",
        ],
        descriptions=[
            "Resident memory climbed steadily over several hours until the kernel "
            "OOM-killer terminated the process, then the cycle repeated.",
            "A cache without an eviction bound retained objects indefinitely, so "
            "heap usage grew until the container hit its memory limit.",
            "GC pauses lengthened as live-set size grew; eventually the pod was "
            "OOMKilled and restarted, dropping in-flight work.",
            "Memory usage ratcheted up after each deploy and never came back down, "
            "pointing at a leak in the request-handling path.",
        ],
        resolutions=[
            "Patched the leak (unbounded cache) by adding an LRU eviction and TTL; "
            "steady-state memory flattened.",
            "Bumped the container memory limit and fixed the retained-reference "
            "leak in the worker's batch loop.",
            "Released large buffers explicitly and enabled a smaller GC heap "
            "target; OOM kills stopped.",
        ],
        severity_weights=[0.3, 0.45, 0.2, 0.05],
    ),
    _Family(
        key="high_latency",
        services=["search-api", "recommend-api", "gateway", "catalog-api"],
        tags=["latency", "p99", "slow"],
        titles=[
            "p99 latency spike on the search endpoint",
            "Slow responses on /recommend",
            "Endpoint latency degraded well beyond SLO",
            "Tail latency regression after traffic increase",
        ],
        descriptions=[
            "p99 response time jumped from ~120ms to over a second while p50 "
            "stayed flat, indicating a tail-latency problem rather than overload.",
            "A hot endpoint slowed dramatically because a frequently-hit query was "
            "missing an index and fell back to a sequential scan.",
            "Response times crept up as the dataset grew; the read path recomputed "
            "results per request with no caching.",
            "Latency degraded under normal load after a change added a synchronous "
            "call to a slow downstream on the request path.",
        ],
        resolutions=[
            "Added a covering index for the hot query, cutting p99 back under the "
            "SLO.",
            "Introduced a short-TTL cache in front of the expensive computation so "
            "repeated requests are served from cache.",
            "Moved the slow downstream call off the hot path and made it async; "
            "tail latency recovered.",
        ],
        severity_weights=[0.1, 0.4, 0.4, 0.1],
    ),
    _Family(
        key="deploy_regression",
        services=["gateway", "orders-api", "web", "checkout-api"],
        tags=["deploy", "regression", "rollback"],
        titles=[
            "Bad rollout causing elevated error rate",
            "Regression introduced by the latest release",
            "Error spike immediately after deploy",
            "New release broke checkout flow",
        ],
        descriptions=[
            "Error rate jumped the moment the new version rolled out; the previous "
            "version had been healthy, pointing squarely at the release.",
            "A code change in the release path introduced a null-handling bug that "
            "500'd a subset of requests.",
            "The deploy shipped a bad config default that disabled a required "
            "feature flag, breaking a downstream call.",
            "Canary metrics regressed after the rollout reached full traffic, with "
            "errors concentrated in the changed service.",
        ],
        resolutions=[
            "Rolled back to the last known-good release; error rate returned to "
            "baseline within minutes.",
            "Reverted the offending commit and added a regression test covering the "
            "null-handling case.",
            "Restored the correct config default and re-deployed behind a canary to "
            "verify before full rollout.",
        ],
        severity_weights=[0.25, 0.45, 0.25, 0.05],
    ),
    _Family(
        key="tls_cert_expiry",
        services=["gateway", "web", "auth-service", "edge-proxy"],
        tags=["tls", "cert", "expiry"],
        titles=[
            "TLS certificate expired",
            "Clients failing on certificate validation",
            "Expired cert breaking HTTPS handshakes",
            "Cert renewal missed — handshake failures",
        ],
        descriptions=[
            "Clients began rejecting connections with certificate-expired errors "
            "the instant the leaf certificate passed its notAfter date.",
            "The automated renewal did not run, so the serving certificate lapsed "
            "and TLS handshakes started failing.",
            "An intermediate certificate in the chain expired, causing validation "
            "failures for some clients but not others.",
            "HTTPS requests failed cluster-wide after the wildcard certificate "
            "reached its expiry.",
        ],
        resolutions=[
            "Renewed and rotated the certificate, then fixed the cron that was "
            "supposed to auto-renew it.",
            "Issued a fresh certificate via ACME and reloaded the proxy; added an "
            "expiry alert 30 days out.",
            "Replaced the expired intermediate in the chain and redeployed the "
            "bundle to every edge node.",
        ],
        severity_weights=[0.4, 0.4, 0.15, 0.05],
    ),
    _Family(
        key="disk_full",
        services=["log-collector", "database", "ci-runner", "metrics-store"],
        tags=["disk", "storage", "full"],
        titles=[
            "Disk full on the log collector",
            "No space left on device",
            "Volume reached 100% utilisation",
            "Writes failing due to full disk",
        ],
        descriptions=[
            "The data volume filled up and writes started failing with "
            "'no space left on device', taking the service down.",
            "Unrotated logs accumulated until the partition was full, blocking new "
            "writes and crashing the process.",
            "A runaway debug log filled the disk overnight; the database could no "
            "longer flush WAL segments.",
            "Storage utilisation hit 100% because old artifacts were never pruned, "
            "so the runner could not write build output.",
        ],
        resolutions=[
            "Expanded the volume and enabled log rotation with a size cap so it "
            "cannot fill the disk again.",
            "Truncated and rotated the offending logs, then added a disk-usage "
            "alert at 80%.",
            "Pruned stale artifacts and moved retention to object storage to keep "
            "the local disk small.",
        ],
        severity_weights=[0.3, 0.45, 0.2, 0.05],
    ),
    _Family(
        key="auth_token_failures",
        services=["auth-service", "gateway", "users-api", "identity"],
        tags=["auth", "token", "401"],
        titles=[
            "Widespread 401s after token change",
            "Authentication failures across services",
            "JWT validation failing for valid tokens",
            "Login broken — tokens rejected",
        ],
        descriptions=[
            "Valid requests started returning 401 because the signing key was "
            "rotated without publishing the new public key to verifiers.",
            "Token validation failed for everyone after a clock-skew between the "
            "issuer and verifier pushed tokens outside their validity window.",
            "A misconfigured audience claim caused the gateway to reject otherwise "
            "valid JWTs with 401 Unauthorized.",
            "Users could not log in; the identity service returned invalid-token "
            "errors for freshly issued credentials.",
        ],
        resolutions=[
            "Rotated the signing key correctly and distributed the new JWKS to all "
            "verifiers; 401s cleared.",
            "Synchronised clocks via NTP across issuer and verifiers and widened "
            "the allowed skew slightly.",
            "Fixed the expected audience/issuer configuration on the gateway and "
            "reloaded it.",
        ],
        severity_weights=[0.3, 0.5, 0.15, 0.05],
    ),
    _Family(
        key="cache_stampede",
        services=["recommend-api", "catalog-api", "gateway", "session-store"],
        tags=["cache", "redis", "stampede"],
        titles=[
            "Cache stampede overwhelmed the backend",
            "Thundering herd after cache expiry",
            "Redis miss storm hammering the database",
            "Simultaneous cache rebuilds saturated the DB",
        ],
        descriptions=[
            "When a hot key expired, thousands of requests missed the cache at "
            "once and stampeded the database, spiking load.",
            "A cache flush caused every instance to recompute the same expensive "
            "value simultaneously, overwhelming the backend.",
            "Synchronised TTLs meant many keys expired together, producing a miss "
            "storm that the origin could not absorb.",
            "A Redis restart cleared the cache and the resulting thundering herd "
            "drove the database to saturation.",
        ],
        resolutions=[
            "Added jitter to cache TTLs and a per-key lock so only one request "
            "rebuilds a value while others wait.",
            "Introduced request coalescing (single-flight) in front of the cache "
            "so concurrent misses share one recompute.",
            "Enabled stale-while-revalidate so expired entries are served briefly "
            "while a single background refresh runs.",
        ],
        severity_weights=[0.15, 0.45, 0.35, 0.05],
    ),
    _Family(
        key="queue_consumer_lag",
        services=["ingest-worker", "events-consumer", "kafka", "billing-worker"],
        tags=["kafka", "consumer-lag", "backpressure"],
        titles=[
            "Kafka consumer lag growing unbounded",
            "Event processing falling behind",
            "Consumer group lag alert firing",
            "Backpressure building on the events topic",
        ],
        descriptions=[
            "Consumer lag climbed steadily as the producer rate outpaced the "
            "consumers, delaying downstream processing by minutes.",
            "A slow handler reduced per-partition throughput, so the consumer "
            "group could not keep up and lag accumulated.",
            "After a traffic surge the number of consumers was insufficient for "
            "the partition count, and lag grew without bound.",
            "Backpressure from a downstream store slowed the consumer, causing the "
            "topic's committed offset to fall further behind head.",
        ],
        resolutions=[
            "Scaled out the consumer group and increased partitions so throughput "
            "matched the producer rate; lag drained.",
            "Optimised the slow handler and batched writes to the downstream store, "
            "restoring per-partition throughput.",
            "Added autoscaling on consumer lag so the group grows during surges "
            "and shrinks afterward.",
        ],
        severity_weights=[0.1, 0.4, 0.4, 0.1],
    ),
    _Family(
        key="upstream_5xx_spike",
        services=["gateway", "checkout-api", "payments-api", "orders-api"],
        tags=["5xx", "upstream", "circuit-breaker"],
        titles=[
            "5xx spike from an upstream dependency",
            "Downstream outage cascading into our service",
            "Upstream errors driving our error rate up",
            "Dependency failures propagating to clients",
        ],
        descriptions=[
            "A dependency started returning 5xx en masse; without a breaker those "
            "errors propagated straight through to our clients.",
            "An upstream payment provider degraded, and retries against it "
            "amplified load and stretched our latency.",
            "The service kept calling a failing downstream, exhausting threads on "
            "calls that were doomed to time out.",
            "A partial outage in a shared dependency surfaced as intermittent 5xx "
            "responses across several of our endpoints.",
        ],
        resolutions=[
            "Enabled a circuit breaker with a fallback so failing-upstream calls "
            "fail fast instead of cascading.",
            "Failed over to the secondary provider and capped retries with "
            "exponential backoff and jitter.",
            "Added timeouts and a bulkhead around the downstream call so its "
            "failure can no longer exhaust our threads.",
        ],
        severity_weights=[0.25, 0.5, 0.2, 0.05],
    ),
]


def _weighted_severity(rng: Random, weights: list[float]) -> str:
    """Pick a severity from :data:`SEVERITIES` using ``weights`` (parallel list)."""
    return rng.choices(SEVERITIES, weights=weights, k=1)[0]


def generate_incidents(
    count: int,
    seed: int = 42,
    *,
    days_back: int = 180,
    end: datetime | None = None,
) -> list[dict]:
    """Generate ``count`` synthetic incidents drawn from the incident families.

    Incidents are distributed round-robin across the families (so every family is
    represented and similar incidents cluster). Within a family, the title,
    description and resolution phrasings are chosen independently at random, so
    two incidents of the same class share meaning but rarely share exact wording.

    Args:
        count: Number of incidents to produce (``<= 0`` yields an empty list).
        seed: RNG seed. The chosen family, text, severity and tags — and each
            incident's *age offset* within the window — are fully deterministic
            for a given seed. Absolute ``created_at`` values are relative to
            ``end`` (which defaults to ``now``), so pass a fixed ``end`` too when
            byte-identical timestamps are required (e.g. in tests).
        days_back: Spread ``created_at`` uniformly across the last this-many days
            (so later recency scoring has signal). Must be positive.
        end: Upper bound for ``created_at`` (default ``now`` UTC). The window is
            ``[end - days_back, end]``.

    Returns:
        A list of ``dict`` rows with keys ``title``, ``description``, ``service``,
        ``severity``, ``tags``, ``resolution``, ``created_at`` — the shape
        :func:`src.db.repository.add_incidents_bulk` consumes. ``embedding`` is
        intentionally absent (computed later, in C5).

    Raises:
        ValueError: If ``days_back`` is not positive.
    """
    if count <= 0:
        return []
    if days_back <= 0:
        raise ValueError("days_back must be positive")

    rng = Random(seed)
    end = end or datetime.now(timezone.utc)
    window_seconds = days_back * 24 * 60 * 60

    incidents: list[dict] = []
    for i in range(count):
        family = _FAMILIES[i % len(_FAMILIES)]
        # created_at spread uniformly across the window (older <- .. -> newer).
        age_seconds = rng.uniform(0.0, float(window_seconds))
        created_at = end - timedelta(seconds=age_seconds)
        incidents.append(
            {
                "title": rng.choice(family.titles),
                "description": rng.choice(family.descriptions),
                "service": rng.choice(family.services),
                "severity": _weighted_severity(rng, family.severity_weights),
                "tags": list(family.tags),
                "resolution": rng.choice(family.resolutions),
                "created_at": created_at,
            }
        )
    return incidents


def generate_default_corpus(seed: int = 42, *, days_back: int = 180) -> list[dict]:
    """Generate a sensible default corpus (~120 incidents across all families).

    A convenience wrapper over :func:`generate_incidents` sized so every family
    is well represented (12 incidents per family × the family count). Deterministic
    for a given ``seed``.
    """
    per_family = 12
    count = per_family * len(_FAMILIES)
    return generate_incidents(count, seed=seed, days_back=days_back)


#: Number of incident families (exposed for tests / seeders that want to size a
#: run so every family is represented).
FAMILY_COUNT = len(_FAMILIES)

#: Stable family keys, in generation (round-robin) order.
FAMILY_KEYS = [f.key for f in _FAMILIES]
