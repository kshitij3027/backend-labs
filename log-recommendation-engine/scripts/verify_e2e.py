"""Black-box end-to-end verifier for the Log Recommendation Engine (C18).

Runs **inside Docker** (the profile-gated ``e2e`` service) against the *live* API
over HTTP — no in-process imports of the app, no direct DB/Redis access. It seeds its
own controlled corpus through the public API and then proves the whole promise of the
system end to end:

    seed a coherent incident family (+ distractors) via POST /incidents
        -> POST /recommend a paraphrase of that family and assert the top suggestion
           is from the family, is fully scored, and carries a real resolution
        -> POST /feedback (helpful on a lower-ranked family incident X, not-helpful on
           the current #1 Y) several times each
        -> POST /recommend the *identical* query again and assert the response was
           **re-computed** (cached=False, because each vote bumps the pattern's feedback
           epoch and invalidates the cache) and that **X now ranks above Y** — the core
           "improves over time via a feedback loop" proof
        -> confirm GET /stats reflects the corpus + votes and GET /metrics exposes the
           Prometheus ``recommend_requests_total`` counter.

Determinism
-----------
The ranker normally does ε-greedy exploration (``epsilon_explore`` defaults to ``0.1``),
which can perturb the ordering and make the re-rank assertion flaky. So the **first**
thing the verifier does after health is ``PUT /config {"epsilon_explore": 0}``. That
disables exploration fleet-wide on the *live* api (no compose change needed) and also
bumps the global config version, giving every ``/recommend`` a clean, deterministic,
un-cached starting point. This keeps the verifier fully self-contained and black-box.

Configuration (all via env, with sensible defaults):

* ``E2E_BASE_URL``       base URL of the live API (default ``http://api:8000``).
* ``E2E_READY_TIMEOUT``  seconds to wait for ``/health`` to come up (default 90).

Exit code: ``0`` only when **every** assertion passes; non-zero with a loud ``FAIL:``
message on the *first* failed assertion — so ``make e2e`` fails immediately and clearly.
"""

from __future__ import annotations

import os
import sys
import time
from typing import Any

import httpx

# --------------------------------------------------------------------------- #
# Configuration (env-driven; documented in the module docstring)
# --------------------------------------------------------------------------- #
BASE_URL = os.environ.get("E2E_BASE_URL", "http://api:8000").rstrip("/")
READY_TIMEOUT = float(os.environ.get("E2E_READY_TIMEOUT", "90"))

# How many votes to cast. X (a lower-ranked family incident) gets a lopsided pile of
# HELPFUL votes; Y (the current #1) gets a pile of NOT-helpful votes. With the default
# blend (weight_feedback=0.2) and Laplace smoothing (2.0), these tallies produce a
# feedback swing large enough to flip two same-family neighbours whose base
# (semantic+contextual) scores are close.
HELPFUL_VOTES_ON_X = 6
UNHELPFUL_VOTES_ON_Y = 4


# --------------------------------------------------------------------------- #
# Assertion + logging helpers
# --------------------------------------------------------------------------- #
class CheckError(AssertionError):
    """Raised to fail the verifier with a clear, single-line message."""


def check(cond: bool, msg: str) -> None:
    """Assert ``cond``; raise :class:`CheckError` with ``msg`` when it is falsy."""
    if not cond:
        raise CheckError(msg)


def info(msg: str) -> None:
    """Print a progress line (flushed so Docker shows it live)."""
    print(f"[e2e] {msg}", flush=True)


# --------------------------------------------------------------------------- #
# The controlled corpus this verifier seeds via the API.
#
# ONE coherent family (DB connection-pool timeouts) with varied phrasings but DISTINCT
# resolutions and identical service/severity/tags, plus unrelated-family distractors so
# retrieval has to actually discriminate. Keeping the family's facets identical means
# every family incident lands in the SAME feedback query-pattern bucket, so votes on X
# and Y are read back on the re-recommend.
# --------------------------------------------------------------------------- #
FAMILY_SERVICE = "orders-api"
FAMILY_SEVERITY = "high"
FAMILY_TAGS = ["db", "connection-pool", "timeout"]

FAMILY_INCIDENTS: list[dict[str, Any]] = [
    {
        "title": "Database connection pool exhausted under load",
        "description": (
            "Requests began failing with connection-timeout errors; the pool was "
            "fully checked out and new queries queued until they timed out."
        ),
        "service": FAMILY_SERVICE,
        "severity": FAMILY_SEVERITY,
        "tags": FAMILY_TAGS,
        "resolution": (
            "Raised the max pool size and added a statement timeout so slow queries "
            "release connections instead of pinning the pool."
        ),
    },
    {
        "title": "Orders API timing out acquiring DB connections",
        "description": (
            "Under peak traffic every connection in the pool was in use, so incoming "
            "requests blocked waiting for a free connection and returned 500s."
        ),
        "service": FAMILY_SERVICE,
        "severity": FAMILY_SEVERITY,
        "tags": FAMILY_TAGS,
        "resolution": (
            "Fixed a connection leak where sessions were not closed on the error "
            "path; pool utilisation returned to normal."
        ),
    },
    {
        "title": "Postgres connection pool saturated, cascading timeouts",
        "description": (
            "A slow query held connections open long enough to drain the pool, "
            "causing cascading timeouts across dependent endpoints."
        ),
        "service": FAMILY_SERVICE,
        "severity": FAMILY_SEVERITY,
        "tags": FAMILY_TAGS,
        "resolution": (
            "Tuned pool_size and max_overflow and put pgbouncer in transaction mode "
            "to absorb connection bursts."
        ),
    },
    {
        "title": "Cannot obtain database connection — pool at its limit",
        "description": (
            "The service exhausted its Postgres connection pool; latency spiked and "
            "health checks failed as no connections were available to serve queries."
        ),
        "service": FAMILY_SERVICE,
        "severity": FAMILY_SEVERITY,
        "tags": FAMILY_TAGS,
        "resolution": (
            "Added a bounded wait-for-connection timeout and shed load with a "
            "circuit breaker while the pool recovered."
        ),
    },
]

# Unrelated distractors (different families entirely) so the retrieval has to separate
# the DB-pool family from clearly different incidents.
DISTRACTOR_INCIDENTS: list[dict[str, Any]] = [
    {
        "title": "TLS certificate expired on the edge proxy",
        "description": (
            "Clients began rejecting connections with certificate-expired errors the "
            "instant the leaf certificate passed its notAfter date."
        ),
        "service": "edge-proxy",
        "severity": "critical",
        "tags": ["tls", "cert", "expiry"],
        "resolution": (
            "Renewed and rotated the certificate, then fixed the cron that was "
            "supposed to auto-renew it."
        ),
    },
    {
        "title": "Kafka consumer lag growing unbounded",
        "description": (
            "Consumer lag climbed steadily as the producer rate outpaced the "
            "consumers, delaying downstream processing by minutes."
        ),
        "service": "events-consumer",
        "severity": "medium",
        "tags": ["kafka", "consumer-lag", "backpressure"],
        "resolution": (
            "Scaled out the consumer group and increased partitions so throughput "
            "matched the producer rate; lag drained."
        ),
    },
    {
        "title": "Service OOM-killed repeatedly due to a memory leak",
        "description": (
            "Resident memory climbed steadily over several hours until the kernel "
            "OOM-killer terminated the process, then the cycle repeated."
        ),
        "service": "ingest-worker",
        "severity": "high",
        "tags": ["memory", "oom", "gc"],
        "resolution": (
            "Patched the leak (an unbounded cache) by adding LRU eviction and a TTL; "
            "steady-state memory flattened."
        ),
    },
    {
        "title": "Disk full on the log collector",
        "description": (
            "The data volume filled up and writes started failing with 'no space "
            "left on device', taking the service down."
        ),
        "service": "log-collector",
        "severity": "high",
        "tags": ["disk", "storage", "full"],
        "resolution": (
            "Expanded the volume and enabled log rotation with a size cap so it "
            "cannot fill the disk again."
        ),
    },
]

# The query — a paraphrase of the DB-pool family that shares NONE of the seeded
# incidents' exact wording, with the family's contextual facets so the contextual
# signals and the feedback bucket both engage.
QUERY = {
    "title": "DB pool timeouts — clients cannot get a database connection",
    "description": (
        "Our service is intermittently failing because the database connection pool "
        "runs out; callers block waiting for a connection and then time out."
    ),
    "service": FAMILY_SERVICE,
    "severity": FAMILY_SEVERITY,
    "tags": FAMILY_TAGS,
}

#: The set of resolutions we seed for the family — used to assert a suggestion's
#: resolution really is one of ours (i.e. the match is from the seeded family).
FAMILY_RESOLUTIONS = {inc["resolution"] for inc in FAMILY_INCIDENTS}


# --------------------------------------------------------------------------- #
# Steps
# --------------------------------------------------------------------------- #
def wait_for_health(client: httpx.Client) -> None:
    """Poll ``GET /health`` until it returns a status (or HTTP 200), within the timeout.

    ``/health`` always answers 200 while the process is alive, so either a 200 or a body
    carrying a ``status`` field means the API is up. We tolerate connection errors while
    the container is still starting.
    """
    deadline = time.time() + READY_TIMEOUT
    last = "no response"
    attempt = 0
    while time.time() < deadline:
        attempt += 1
        try:
            resp = client.get("/health", timeout=5.0)
            if resp.status_code == 200:
                body = resp.json()
                check(
                    "status" in body,
                    f"/health 200 but no 'status' field: {body!r}",
                )
                info(
                    f"health ready after {attempt} attempt(s): "
                    f"status={body.get('status')} corpus_size={body.get('corpus_size')} "
                    f"components={body.get('components')}"
                )
                return
            last = f"HTTP {resp.status_code}"
        except Exception as exc:  # noqa: BLE001 - service may still be starting
            last = type(exc).__name__
        time.sleep(2.0)
    raise CheckError(f"/health not ready after {READY_TIMEOUT:.0f}s (last: {last})")


def force_deterministic(client: httpx.Client) -> None:
    """PUT /config {"epsilon_explore": 0} so the re-rank is deterministic.

    This is the FIRST mutating action. Disabling ε-exploration removes the only source
    of ranking randomness, so the ordering assertions below are stable. It also bumps
    the global config version, invalidating any pre-existing recommendation cache — the
    verifier therefore always starts from a clean, freshly-computed baseline.
    """
    resp = client.put("/config", json={"epsilon_explore": 0}, timeout=15.0)
    check(
        resp.status_code == 200,
        f"PUT /config epsilon_explore=0 -> {resp.status_code}: {resp.text[:200]}",
    )
    body = resp.json()
    eps = body.get("config", {}).get("epsilon_explore")
    check(
        eps == 0 or eps == 0.0,
        f"epsilon_explore not applied (effective config shows {eps!r})",
    )
    info(f"determinism: epsilon_explore=0 applied (config version={body.get('version')})")


def seed_incident(client: httpx.Client, inc: dict[str, Any]) -> int:
    """POST one incident; assert 201 + has_embedding=true; return its id."""
    resp = client.post("/incidents", json=inc, timeout=30.0)
    check(
        resp.status_code == 201,
        f"POST /incidents {inc['title']!r} -> {resp.status_code}: {resp.text[:200]}",
    )
    body = resp.json()
    check(
        bool(body.get("has_embedding")),
        f"incident {inc['title']!r} was persisted without an embedding "
        f"(has_embedding={body.get('has_embedding')}) — it would be unsearchable",
    )
    check(
        isinstance(body.get("id"), int) and body["id"] >= 1,
        f"incident {inc['title']!r} returned no valid id: {body.get('id')!r}",
    )
    return int(body["id"])


def seed_corpus(client: httpx.Client) -> set[int]:
    """Seed the family + distractors via the API. Return the set of family incident ids."""
    family_ids: set[int] = set()
    for inc in FAMILY_INCIDENTS:
        family_ids.add(seed_incident(client, inc))
    for inc in DISTRACTOR_INCIDENTS:
        seed_incident(client, inc)
    info(
        f"seeded {len(FAMILY_INCIDENTS)} family + {len(DISTRACTOR_INCIDENTS)} distractor "
        f"incidents (family ids={sorted(family_ids)})"
    )
    return family_ids


def _assert_suggestion_shape(sug: dict[str, Any], where: str) -> None:
    """Assert a single suggestion carries a resolution and a full score breakdown."""
    check(
        isinstance(sug.get("resolution"), str) and sug["resolution"].strip() != "",
        f"{where}: suggestion incident_id={sug.get('incident_id')} has an empty resolution",
    )
    bd = sug.get("breakdown")
    check(isinstance(bd, dict) and bd, f"{where}: suggestion has no breakdown dict")
    for key in ("semantic", "contextual", "feedback"):
        check(
            key in bd,
            f"{where}: breakdown missing {key!r} (keys={sorted(bd)})",
        )
    for key in ("score", "semantic", "contextual", "feedback"):
        check(
            isinstance(sug.get(key), (int, float)),
            f"{where}: suggestion field {key!r} is not numeric ({sug.get(key)!r})",
        )


def recommend(client: httpx.Client, *, label: str) -> dict[str, Any]:
    """POST /recommend the fixed QUERY; assert 200 + count>0 + well-formed suggestions."""
    resp = client.post("/recommend", json=QUERY, timeout=60.0)
    check(
        resp.status_code == 200,
        f"POST /recommend ({label}) -> {resp.status_code}: {resp.text[:200]}",
    )
    body = resp.json()
    check(
        int(body.get("count", 0)) > 0 and body.get("suggestions"),
        f"POST /recommend ({label}) returned no suggestions (count={body.get('count')})",
    )
    for sug in body["suggestions"]:
        _assert_suggestion_shape(sug, where=f"/recommend ({label})")
    return body


def rank_index(suggestions: list[dict[str, Any]], incident_id: int) -> int:
    """Return the 0-based rank of ``incident_id`` in ``suggestions`` (-1 if absent)."""
    for i, sug in enumerate(suggestions):
        if sug.get("incident_id") == incident_id:
            return i
    return -1


def vote(client: httpx.Client, recommendation_id: int, incident_id: int, helpful: bool) -> None:
    """POST /feedback one vote; assert 201."""
    resp = client.post(
        "/feedback",
        json={
            "recommendation_id": recommendation_id,
            "incident_id": incident_id,
            "helpful": helpful,
        },
        timeout=15.0,
    )
    check(
        resp.status_code == 201,
        f"POST /feedback (rec={recommendation_id} inc={incident_id} helpful={helpful}) "
        f"-> {resp.status_code}: {resp.text[:200]}",
    )


def verify_stats(client: httpx.Client, *, expected_corpus: int, expected_votes: int) -> None:
    """GET /stats and assert the corpus + feedback tallies match what we drove."""
    resp = client.get("/stats", timeout=15.0)
    check(resp.status_code == 200, f"GET /stats -> {resp.status_code}: {resp.text[:200]}")
    body = resp.json()
    check(
        int(body.get("corpus_size", 0)) >= expected_corpus,
        f"/stats corpus_size {body.get('corpus_size')} < seeded {expected_corpus}",
    )
    total = int(body.get("feedback_total", -1))
    helpful = int(body.get("feedback_helpful", 0))
    unhelpful = int(body.get("feedback_unhelpful", 0))
    check(
        total == helpful + unhelpful,
        f"/stats feedback_total {total} != helpful {helpful} + unhelpful {unhelpful}",
    )
    check(
        total >= expected_votes,
        f"/stats feedback_total {total} < votes we cast {expected_votes}",
    )
    check(
        int(body.get("recommendations_served", 0)) >= 2,
        f"/stats recommendations_served {body.get('recommendations_served')} < 2",
    )
    info(
        f"stats OK: corpus_size={body.get('corpus_size')} "
        f"embedded={body.get('embedded_count')} feedback_total={total} "
        f"(helpful={helpful}/unhelpful={unhelpful}) "
        f"recommendations_served={body.get('recommendations_served')}"
    )


def verify_metrics(client: httpx.Client) -> None:
    """GET /metrics and assert it returns 200 text exposition with recommend_requests_total."""
    resp = client.get("/metrics", timeout=15.0)
    check(resp.status_code == 200, f"GET /metrics -> {resp.status_code}")
    text = resp.text
    check(
        "recommend_requests_total" in text,
        "/metrics exposition does not contain 'recommend_requests_total'",
    )
    info("metrics OK: /metrics exposes recommend_requests_total")


# --------------------------------------------------------------------------- #
# The full flow
# --------------------------------------------------------------------------- #
def run() -> None:
    info(f"== E2E verifier against {BASE_URL} ==")
    with httpx.Client(base_url=BASE_URL) as client:
        # 1. Wait for the live API.
        wait_for_health(client)

        # 2. Force determinism BEFORE anything else (epsilon_explore -> 0). This also
        #    invalidates any pre-existing recommendation cache (config version bump).
        force_deterministic(client)

        # 3. Seed a controlled corpus (one coherent family + distractors).
        family_ids = seed_corpus(client)

        # 4. Recommend a paraphrase of the family; validate shape + that #1 is family.
        first = recommend(client, label="before feedback")
        suggestions = first["suggestions"]
        ordered_ids = [s["incident_id"] for s in suggestions]
        recommendation_id = first["recommendation_id"]
        info(f"recommend #1 (before): ordered incident_ids={ordered_ids} cached={first.get('cached')}")

        top_id = ordered_ids[0]
        check(
            top_id in family_ids,
            f"top suggestion incident_id={top_id} is NOT from the seeded family "
            f"{sorted(family_ids)} — retrieval failed to surface the right family",
        )

        # Pick Y = current #1 (a family incident) and X = a LOWER-ranked family incident.
        Y = top_id
        lower_family = [i for i in ordered_ids[1:] if i in family_ids]
        check(
            bool(lower_family),
            "need a second family incident ranked below #1 to prove a re-rank shift, "
            f"but only these family ids appeared in order: "
            f"{[i for i in ordered_ids if i in family_ids]}",
        )
        X = lower_family[0]
        before_x = rank_index(suggestions, X)
        before_y = rank_index(suggestions, Y)
        info(f"chose Y=#1 (incident {Y}, rank {before_y}) and X (incident {X}, rank {before_x})")
        check(
            before_x > before_y,
            f"expected X (rank {before_x}) to start BELOW Y (rank {before_y})",
        )

        # 5. Feedback: pile HELPFUL votes on X, NOT-helpful votes on Y. Each vote bumps
        #    this pattern's feedback epoch, which invalidates the recommendation cache.
        for _ in range(HELPFUL_VOTES_ON_X):
            vote(client, recommendation_id, X, helpful=True)
        for _ in range(UNHELPFUL_VOTES_ON_Y):
            vote(client, recommendation_id, Y, helpful=False)
        total_votes = HELPFUL_VOTES_ON_X + UNHELPFUL_VOTES_ON_Y
        info(
            f"cast {HELPFUL_VOTES_ON_X} helpful votes on X={X} and "
            f"{UNHELPFUL_VOTES_ON_Y} not-helpful votes on Y={Y} ({total_votes} total)"
        )

        # 6. Re-recommend the IDENTICAL query. The core proof of the feedback loop.
        second = recommend(client, label="after feedback")
        suggestions2 = second["suggestions"]
        ordered_ids2 = [s["incident_id"] for s in suggestions2]
        info(f"recommend #2 (after): ordered incident_ids={ordered_ids2} cached={second.get('cached')}")

        # 6a. The vote bumped the epoch, so this identical query must be RE-COMPUTED.
        check(
            second.get("cached") is False,
            f"expected cached=False after feedback (epoch bump should invalidate the "
            f"cache), got cached={second.get('cached')!r}",
        )

        # 6b. Both X and Y must still be present so we can compare their new ranks.
        after_x = rank_index(suggestions2, X)
        after_y = rank_index(suggestions2, Y)
        check(after_x >= 0, f"X={X} dropped out of the re-ranked results {ordered_ids2}")
        check(after_y >= 0, f"Y={Y} dropped out of the re-ranked results {ordered_ids2}")

        # 6c. The learned feedback signal must be reflected on the suggestions.
        sug_x = suggestions2[after_x]
        sug_y = suggestions2[after_y]
        check(
            sug_x.get("feedback", 0) > 0,
            f"X={X} feedback signal should be positive after {HELPFUL_VOTES_ON_X} "
            f"helpful votes, got {sug_x.get('feedback')!r}",
        )
        check(
            sug_y.get("feedback", 0) < 0,
            f"Y={Y} feedback signal should be negative after {UNHELPFUL_VOTES_ON_Y} "
            f"not-helpful votes, got {sug_y.get('feedback')!r}",
        )

        # 6d. THE re-rank assertion: X now ranks ABOVE Y (lower index == higher rank).
        check(
            after_x < after_y,
            f"re-rank did NOT improve: X={X} is at rank {after_x} and Y={Y} at rank "
            f"{after_y} — expected X to overtake Y after the feedback",
        )
        info(
            f"RE-RANK PROVEN: X={X} moved rank {before_x} -> {after_x}; "
            f"Y={Y} moved rank {before_y} -> {after_y} (X now above Y)"
        )

        # 7. Stats + metrics reflect what we drove.
        expected_corpus = len(FAMILY_INCIDENTS) + len(DISTRACTOR_INCIDENTS)
        verify_stats(client, expected_corpus=expected_corpus, expected_votes=total_votes)
        verify_metrics(client)

        # 8. Loud success summary.
        print("", flush=True)
        print("=" * 68, flush=True)
        print("E2E PASSED ✅", flush=True)
        print(f"  corpus seeded          : {expected_corpus} incidents "
              f"({len(FAMILY_INCIDENTS)} family + {len(DISTRACTOR_INCIDENTS)} distractors)", flush=True)
        print(f"  top suggestion (before): incident {top_id} (from the seeded family)", flush=True)
        print(f"  votes cast             : {HELPFUL_VOTES_ON_X} helpful on X={X}, "
              f"{UNHELPFUL_VOTES_ON_Y} not-helpful on Y={Y}", flush=True)
        print(f"  order before feedback  : {ordered_ids}", flush=True)
        print(f"  order after  feedback  : {ordered_ids2}  (re-computed, cached=False)", flush=True)
        print(f"  re-rank shift          : X rank {before_x}->{after_x}, "
              f"Y rank {before_y}->{after_y}  => X now above Y", flush=True)
        print("=" * 68, flush=True)


def main() -> int:
    try:
        run()
    except CheckError as exc:
        print("", flush=True)
        print("!" * 68, file=sys.stderr, flush=True)
        print(f"FAIL: {exc}", file=sys.stderr, flush=True)
        print("E2E FAILED ❌", file=sys.stderr, flush=True)
        print("!" * 68, file=sys.stderr, flush=True)
        return 1
    except Exception as exc:  # noqa: BLE001 - any unexpected error is a hard failure
        print("", flush=True)
        print("!" * 68, file=sys.stderr, flush=True)
        print(f"FAIL: unexpected {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        print("E2E FAILED ❌", file=sys.stderr, flush=True)
        print("!" * 68, file=sys.stderr, flush=True)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
