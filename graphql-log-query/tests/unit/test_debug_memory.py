"""``GET /debug/memory`` — the server-side RSS probe the ``MAX_BACKEND_MEM_MB`` gate reads.

.. rubric:: What is actually at stake here, and why "it returned 200" is not a test

This route exists for one reason: ``scripts/verify_e2e.py`` (C12) and ``scripts/load_test.py`` (C14)
run in **their own container**, so a harness that called ``psutil`` on itself would be grading the
footprint of an httpx client. The route is what makes the gate measure the right process — and a
gate that cannot measure is worse than no gate, because it prints PASS forever.

Three properties therefore have to be pinned, and each of them can regress on its own:

1. **The wire keys.** The gate reads ``probe["memoryMb"]``. A model that serialises ``memory_mb``
   makes that read ``None``, and the check fails every run with a message blaming ``psutil``. See
   :func:`test_the_wire_keys_are_the_ones_the_verifier_reads`.
2. **The number is this process's resident set size**, not a constant and not a different quantity
   wearing the same name. Pinned by *moving* it: :func:`test_the_reading_tracks_real_resident_memory`
   allocates a large buffer and asserts the reported figure follows.
3. **A missing reading is ``null``, never ``0.0``.** A zero passes every ceiling forever; a null
   fails the check with a message naming ``psutil``, which is the outcome that gets fixed.

.. rubric:: No lifespan, and a stub broker

The app under test is a bare :class:`~fastapi.FastAPI` with only this router mounted. That is
deliberate: the route's contract is "process facts plus whatever ``app.state.broker`` reports", and
building the real application would drag a database and a Redis client into a test about a number.
The broker is stubbed to the one method the route calls, including a stub that *raises* — a probe
that 500s because a diagnostic it merely mentions is unreadable would fail an E2E run for a reason
that has nothing to do with the API being correct.
"""

from __future__ import annotations

import inspect
import os
from typing import Any, Optional

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from src.api.debug import (
    BYTES_PER_MIB,
    PSUTIL_AVAILABLE,
    MemoryResponse,
    _resident_bytes,
    memory,
    router as debug_router,
)

#: The keys ``scripts/verify_e2e.py``'s ``check_perf_and_memory`` reads off the response body. Kept
#: as a literal here rather than derived from the model, because deriving them from the thing under
#: test would make any renaming invisible — which is the exact failure this list guards.
VERIFIER_KEYS = ("available", "memoryMb", "pid", "subscribers")

#: How much memory :func:`test_the_reading_tracks_real_resident_memory` allocates, and how much of
#: it the reading must reflect. ``bytearray(n)`` is zero-filled, so every page is touched and
#: therefore resident; the assertion threshold is an eighth of the allocation so that page
#: accounting, allocator behaviour and a busy machine all have room to be imprecise without the
#: test becoming a coin flip.
ALLOCATION_BYTES = 64 * BYTES_PER_MIB
MINIMUM_OBSERVED_GROWTH = 8 * BYTES_PER_MIB


class StubBroker:
    """The one method :func:`src.api.debug.memory` calls on a broker."""

    def __init__(self, count: int) -> None:
        self._count = count
        self.reads = 0

    def subscriber_count(self) -> int:
        self.reads += 1
        return self._count


class ExplodingBroker:
    """A broker whose counter cannot be read. The route must answer anyway."""

    def subscriber_count(self) -> int:
        raise RuntimeError("simulated: the registry lock is wedged")


_NO_BROKER = object()


def make_app(broker: Any = _NO_BROKER) -> FastAPI:
    """A bare app with only ``/debug/memory`` mounted, optionally carrying a broker."""
    app = FastAPI()
    app.include_router(debug_router)
    if broker is not _NO_BROKER:
        app.state.broker = broker
    return app


def probe(broker: Any = _NO_BROKER) -> dict[str, Any]:
    """``GET /debug/memory`` against a fresh app; the parsed body, with the status asserted."""
    with TestClient(make_app(broker)) as client:
        response = client.get("/debug/memory")
    assert response.status_code == 200, response.text
    return response.json()


# =================================================================================================
# The wire contract — the half that has already silently broken once
# =================================================================================================


def test_the_wire_keys_are_the_ones_the_verifier_reads() -> None:
    """``check_perf_and_memory`` reads ``probe["memoryMb"]``; the body must actually contain it.

    The Python attribute is ``memory_mb`` and the published key is ``memoryMb``, which is the same
    split every other surface in this project uses (Strawberry's ``auto_camel_case``). Without the
    alias the model serialises the attribute name, ``probe.get("memoryMb")`` returns ``None``, and
    the verifier's memory gate raises "the server's memory probe reported ..." on **every** run —
    a failure that looks like a broken image and is actually a renamed key.
    """
    body = probe()

    missing = [key for key in VERIFIER_KEYS if key not in body]
    assert missing == [], (
        f"GET /debug/memory does not publish {missing}, which scripts/verify_e2e.py reads by name. "
        f"The body was {body!r}"
    )
    assert "rssBytes" in body, "the unrounded reading is published under its camelCase key too"


def test_no_snake_case_key_is_published_beside_its_camel_case_alias() -> None:
    """One spelling per field, so a consumer cannot accidentally depend on the dead one.

    Publishing both would make the bug above unfalsifiable: the verifier would work, and so would a
    client reading ``memory_mb``, until somebody removed the duplicate.
    """
    body = probe()

    assert "memory_mb" not in body
    assert "rss_bytes" not in body
    assert set(body) == {"pid", "memoryMb", "rssBytes", "subscribers", "available"}


def test_every_published_key_is_camel_case() -> None:
    """A field added later in snake_case would be a second instance of the same bug.

    Walked over the served body rather than a hand-written list of keys, so this fails when the
    *next* multi-word field is added without an alias rather than when somebody notices.
    """
    body = probe()

    snake = [key for key in body if "_" in key]
    assert snake == [], (
        f"these response keys are snake_case: {snake}. Every published surface in this project is "
        "camelCase; add an alias to src.api.debug.MemoryResponse."
    )


def test_the_model_is_still_constructible_by_field_name() -> None:
    """``populate_by_name`` — the route builds the response with Python names, not with aliases."""
    built = MemoryResponse(
        pid=4242, memory_mb=12.5, rss_bytes=13_107_200, subscribers=3, available=True
    )

    assert built.memory_mb == 12.5
    assert built.rss_bytes == 13_107_200
    assert built.model_dump(by_alias=True)["memoryMb"] == 12.5
    assert built.model_dump(by_alias=True)["rssBytes"] == 13_107_200


# =================================================================================================
# The number is this process's RSS
# =================================================================================================


def test_the_probe_answers_for_the_process_that_served_the_request() -> None:
    """``pid`` is the answering process's own, which is what makes the reading attributable.

    Under ``uvicorn --workers N`` this is how a harness knows the number belongs to one worker
    rather than to the deployment. Here the app runs in-process, so the pid it reports is this
    interpreter's — a route that echoed a client-supplied or hard-coded value would not match.
    """
    body = probe()

    assert body["pid"] == os.getpid()


@pytest.mark.skipif(not PSUTIL_AVAILABLE, reason="psutil is pinned; this asserts the live reading")
def test_the_reading_is_present_plausible_and_self_consistent() -> None:
    """MiB and bytes describe one reading, and the rounding is to one decimal.

    The consistency check is the point: two independently computed numbers would let the rounded
    one drift (a megabyte instead of a mebibyte, say — a 600 MB gate quietly becoming 629 MB) while
    both stayed individually plausible.
    """
    body = probe()

    assert body["available"] is True
    assert body["rssBytes"] is not None and body["memoryMb"] is not None
    assert body["rssBytes"] > 0
    assert body["memoryMb"] == round(body["rssBytes"] / BYTES_PER_MIB, 1)
    assert BYTES_PER_MIB == 1024 * 1024, "mebibytes, not megabytes — the gate is written in MiB"
    # A Python process running FastAPI is comfortably over a mebibyte and comfortably under 100 GiB.
    # Wide on purpose: this asserts "a real reading", not a footprint budget.
    assert BYTES_PER_MIB < body["rssBytes"] < 100 * 1024 * BYTES_PER_MIB


@pytest.mark.skipif(not PSUTIL_AVAILABLE, reason="psutil is pinned; this asserts the live reading")
def test_the_reading_tracks_real_resident_memory() -> None:
    """Allocate, re-read, and require the number to have moved. **The load-bearing test here.**

    Every other assertion in this file would stay green against a route that returned a constant,
    a peak-since-boot figure, or the RSS of some other process. This one will not: a 64 MiB
    zero-filled buffer is genuinely resident, so a reading that is really *this* process's current
    RSS has to grow with it.

    Growth rather than an exact figure, and an eighth of the allocation rather than all of it,
    because the interpreter is doing other things at the same time and page accounting is not
    obliged to be prompt. Shrinkage after the free is deliberately **not** asserted: an allocator
    is entitled to keep freed pages mapped, so requiring the number to come back down would be
    testing glibc rather than this route.
    """
    before = probe()["rssBytes"]

    ballast = bytearray(ALLOCATION_BYTES)
    try:
        # Touch it after the read, so a compiler-, allocator- or interpreter-level optimisation
        # cannot decide the buffer is dead before the probe runs.
        ballast[0] = 1
        ballast[-1] = 1
        during = probe()["rssBytes"]
    finally:
        del ballast

    growth = during - before
    assert growth >= MINIMUM_OBSERVED_GROWTH, (
        f"allocating {ALLOCATION_BYTES / BYTES_PER_MIB:.0f} MiB moved the reported RSS by only "
        f"{growth / BYTES_PER_MIB:.1f} MiB ({before} -> {during} bytes). The probe is not "
        "reporting this process's current resident set size."
    )


@pytest.mark.skipif(not PSUTIL_AVAILABLE, reason="psutil is pinned; this asserts the live reading")
def test_the_helper_and_the_route_read_the_same_quantity() -> None:
    """``_resident_bytes()`` is what the route serves, so the two must agree to within noise.

    Not exact equality: the two readings are taken microseconds apart and the process really is
    allocating in between. A 25% band is far tighter than any plausible confusion between RSS and
    VMS (which differ by an order of magnitude) and far looser than ordinary churn.
    """
    direct = _resident_bytes()
    assert direct is not None

    served = probe()["rssBytes"]
    assert served is not None
    assert abs(served - direct) < 0.25 * direct, (
        f"the route served {served} bytes while the helper read {direct}"
    )


def test_an_unreadable_reading_is_null_rather_than_zero() -> None:
    """A zero passes every ceiling forever; a null fails the check with a diagnosable message.

    Driven through the model rather than by breaking ``psutil``, because the branch that produces
    it is unreachable in a correctly built image and the *contract* — ``available: false`` with
    nulls, never a fabricated number — is what the verifier keys on.
    """
    unavailable = MemoryResponse(
        pid=1, memory_mb=None, rss_bytes=None, subscribers=0, available=False
    ).model_dump(by_alias=True)

    assert unavailable["available"] is False
    assert unavailable["memoryMb"] is None
    assert unavailable["rssBytes"] is None
    assert unavailable["memoryMb"] != 0.0


# =================================================================================================
# The subscriber gauge, read from the application instance
# =================================================================================================


def test_the_subscriber_count_comes_from_this_applications_broker() -> None:
    """Two applications in one test process must report independently.

    The count is read off ``request.app.state.broker`` rather than a module global for exactly that
    reason, and a stub returning a distinctive number is how a global would be caught.
    """
    stub = StubBroker(7)

    body = probe(stub)

    assert body["subscribers"] == 7
    assert stub.reads == 1, "read once, at the moment the rest of the snapshot was taken"


def test_two_applications_report_their_own_subscriber_counts() -> None:
    """The isolation claim, stated as two apps alive at once rather than one after the other."""
    quiet = make_app(StubBroker(0))
    busy = make_app(StubBroker(4))

    with TestClient(quiet) as quiet_client, TestClient(busy) as busy_client:
        assert quiet_client.get("/debug/memory").json()["subscribers"] == 0
        assert busy_client.get("/debug/memory").json()["subscribers"] == 4


def test_an_application_with_no_broker_reports_zero_rather_than_failing() -> None:
    """No broker means no lifespan, in which case zero subscriptions is the literal truth."""
    body = probe()

    assert body["subscribers"] == 0
    assert (body["memoryMb"] is not None) is body["available"], (
        "the memory half answers independently of the broker half, and `available` still describes "
        "whether there is a reading"
    )


def test_a_broker_whose_counter_raises_does_not_fail_the_probe() -> None:
    """A probe that 500s would fail an E2E run for a reason unrelated to the API being correct.

    The memory reading — the thing the gate actually needs — must still come back, which is why the
    assertion is about ``memoryMb`` and not only about the status code.
    """
    body = probe(ExplodingBroker())

    assert body["subscribers"] == 0
    if PSUTIL_AVAILABLE:
        assert body["memoryMb"] is not None, "the reading the gate needs survived the bad counter"


# =================================================================================================
# Registration and threading
# =================================================================================================


def test_the_handler_is_synchronous_so_the_proc_read_leaves_the_event_loop() -> None:
    """Reading ``/proc`` is a short blocking syscall; on the loop it lands in front of a query.

    Starlette runs a plain ``def`` endpoint on the threadpool and an ``async def`` one directly on
    the event loop, so this one-line property is the whole difference between the probe being free
    and it being a latency spike in whatever request was in flight.
    """
    assert not inspect.iscoroutinefunction(memory)


def test_the_route_is_registered_at_the_path_the_harnesses_call() -> None:
    """``/debug/memory``, exactly — both harnesses hard-code it against a running container."""
    paths = {route.path for route in make_app().routes if hasattr(route, "path")}

    assert "/debug/memory" in paths


def test_the_router_publishes_nothing_but_the_probe() -> None:
    """Process facts only. This route is unauthenticated, so its surface is the security model.

    A second route added to this router would be unauthenticated too, and would inherit an argument
    ("a pid, an RSS and a subscriber count are not worth an auth scheme") that was made about
    exactly those three facts.
    """
    routes = [route for route in debug_router.routes if hasattr(route, "path")]

    assert [route.path for route in routes] == ["/debug/memory"]
    assert sorted(routes[0].methods) == ["GET"]  # type: ignore[attr-defined]


def test_the_response_model_filters_extra_keys_out() -> None:
    """``response_model`` is what stops a later commit leaking configuration through this route."""
    fields = set(MemoryResponse.model_fields)

    assert fields == {"pid", "memory_mb", "rss_bytes", "subscribers", "available"}


def test_resident_bytes_returns_an_int_or_none_and_never_raises() -> None:
    """The helper's whole contract, and the one every caller above depends on."""
    reading: Optional[int] = _resident_bytes()

    if PSUTIL_AVAILABLE:
        assert isinstance(reading, int) and reading > 0
    else:  # pragma: no cover - psutil is pinned in requirements.txt
        assert reading is None
