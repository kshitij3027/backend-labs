"""Process introspection — ``GET /debug/memory``.

.. rubric:: WHY THIS EXISTS: a harness measuring itself measures nothing

``MAX_BACKEND_MEM_MB`` gates ``scripts/verify_e2e.py`` (C12) and ``scripts/load_test.py`` (C14), and
both of them run **in their own container**, as their own process, on the other side of the compose
network from the thing they are grading. A client that called ``psutil.Process()`` on itself would
be reporting the memory footprint of an httpx client and a few lists — a number that is real, stable,
and about entirely the wrong process. ``.env.example`` says as much next to the gate ("Taken from
the SERVER's own psutil probe, never the client's"); this route is what makes that sentence true
rather than aspirational.

It is therefore **not a debugging convenience**. Without it the memory gate cannot bite, and a gate
that cannot bite is worse than no gate: it reports PASS forever and nobody notices that the number
behind it was never checked.

.. rubric:: Why a separate route and not one more series on ``/metrics``

``/metrics`` is registered **only when** ``METRICS_ENABLED`` is set (see
:func:`src.main.create_app`), and it is a perfectly ordinary thing for an operator to turn off. If
the RSS reading lived there, ``METRICS_ENABLED=false`` would silently disable the memory gate —
turning an unrelated observability toggle into a switch that makes a verification check unenforceable
while it still prints PASS. So this route stands on its own and is always registered.

The two are also read by different things in different formats: Prometheus scrapes text exposition
on a schedule, while a harness wants one number, once, as JSON, at the end of a run.

.. rubric:: Why it is not gated behind auth

This project has no authentication layer at all — it is a GraphQL demonstration over a log store,
and adding one route's worth of RBAC would be inventing a security model to protect a number. What
is exposed is deliberately kept to the level of "facts about this process": a pid, a resident-set
size, and the count of live subscriptions that ``/metrics`` already publishes as a gauge. **No
stored data, no configuration, no connection strings, no environment.** Anything richer belongs
behind an auth scheme this project does not have.

.. rubric:: A missing ``psutil`` answers honestly rather than guessing

:mod:`psutil` is pinned in ``requirements.txt`` for exactly this route, so the import failing means
a broken image rather than an unsupported platform. It is still guarded, and the guarded answer is
``available: false`` with a **null** ``memoryMb`` — not a zero, and not a fabricated reading from
``resource.getrusage`` (whose ``ru_maxrss`` is a *peak*, in KiB on Linux and bytes on macOS, i.e. a
different quantity in different units wearing the same name). A harness that receives ``null`` fails
its check with a message naming ``psutil``, which is the outcome that gets fixed. A harness that
receives ``0.0`` passes every ceiling forever.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

from fastapi import APIRouter, Request
from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)

router = APIRouter(tags=["debug"])

try:  # pragma: no cover - the failure branch needs a deliberately broken image to reach
    import psutil

    PSUTIL_AVAILABLE = True
except Exception:  # noqa: BLE001 - a missing optional dependency must not stop the app importing
    psutil = None  # type: ignore[assignment]
    PSUTIL_AVAILABLE = False


#: Bytes per mebibyte. Named because ``/ 1024 / 1024`` inline is where a mebibyte quietly becomes a
#: megabyte and a 600 MB gate becomes a 629 MB one.
BYTES_PER_MIB = 1024 * 1024


class MemoryResponse(BaseModel):
    """What ``GET /debug/memory`` reports about **this** process.

    Attributes:
        pid: The process id. Under ``uvicorn --workers N`` each worker answers for itself, so a
            harness reading a number knows which process it belongs to — and knows that the reading
            is one worker's rather than the deployment's.
        memory_mb: Resident set size in **mebibytes**, rounded to one decimal, or ``None`` when
            ``psutil`` is unavailable. RSS rather than VMS: it is the number that corresponds to
            what the container is actually charged for and what an OOM killer counts.
        rss_bytes: The same reading unrounded, for a caller that wants to do its own arithmetic.
        subscribers: Live subscription operations held by this process — the same gauge
            ``/metrics`` publishes as ``gql_active_subscriptions``. Included because the C14 load
            harness's memory phase asserts two things about one instant ("RSS is bounded" and
            "the subscribers were released"), and reading them from two endpoints at two moments
            would let a leak hide in the gap between the reads.
        available: ``False`` only when ``psutil`` could not be imported, in which case ``memory_mb``
            and ``rss_bytes`` are ``None``. See the module docstring for why this is reported rather
            than papered over with a zero.

    .. rubric:: THE WIRE KEYS ARE camelCase; the Python attributes are snake_case

    Every other published surface in this project camel-cases (Strawberry's ``auto_camel_case`` is
    on, and the module docstring above names the null field ``memoryMb``), and the only consumer of
    this route — ``check_perf_and_memory`` in ``scripts/verify_e2e.py`` — reads ``probe["memoryMb"]``.
    Without the aliases below the model serialises ``memory_mb``, that read returns ``None``, and
    the ``MAX_BACKEND_MEM_MB`` gate fails *every* run with "the server's memory probe reported ..." —
    a gate that cannot measure, which is precisely the failure this route exists to prevent.

    ``populate_by_name`` keeps ``MemoryResponse(memory_mb=...)`` constructible by field name;
    FastAPI serialises response models with ``by_alias=True``, so the wire keys are the aliases.
    ``tests/unit/test_debug_memory.py`` pins both spellings.
    """

    model_config = ConfigDict(populate_by_name=True)

    pid: int = Field(description="PID of the process that answered this request.")
    memory_mb: Optional[float] = Field(
        alias="memoryMb",
        description="Resident set size in MiB, or null when psutil is unavailable.",
    )
    rss_bytes: Optional[int] = Field(
        alias="rssBytes",
        description="Resident set size in bytes, or null when psutil is unavailable.",
    )
    subscribers: int = Field(description="Live subscription operations held by this process.")
    available: bool = Field(description="False when psutil could not be imported.")


def _resident_bytes() -> Optional[int]:
    """This process's RSS in bytes, or ``None`` if it cannot be read. **Never raises.**

    ``psutil.Process()`` with no argument is the current process, and ``memory_info().rss`` is the
    portable spelling of resident set size across Linux (``/proc/self/statm``) and macOS
    (``task_info``) — which matters because the container runs Linux and a developer running the
    app on a host does not.

    A failure is swallowed and reported as ``None``. The one realistic way this raises is a
    permissions or ``/proc`` restriction in a hardened runtime, and a probe endpoint that answered
    500 would fail an E2E run for a reason that has nothing to do with the API being correct.
    """
    if not PSUTIL_AVAILABLE:
        return None
    try:
        return int(psutil.Process().memory_info().rss)
    except Exception:  # noqa: BLE001 - see the docstring: an unreadable probe is None, not a 500
        logger.debug("could not read this process's resident set size", exc_info=True)
        return None


@router.get(
    "/debug/memory",
    response_model=MemoryResponse,
    summary="This process's resident memory and live subscription count",
    description=(
        "The server's own psutil reading, for the MAX_BACKEND_MEM_MB gate in the E2E verifier and "
        "the load harness — a harness measuring its own process would be measuring the wrong one. "
        "Reports process-level facts only: no stored data, no configuration."
    ),
)
def memory(request: Request) -> MemoryResponse:
    """Report this process's RSS and live subscriber count.

    Takes the ``Request`` — unlike :func:`src.api.health.health`, deliberately — because the
    subscriber count belongs to the application instance's broker rather than to a module global,
    which is the same arrangement ``/metrics`` uses and for the same reason: two applications in one
    test process must report independently.

    Synchronous (a plain ``def``) so Starlette runs it on the threadpool: reading ``/proc`` is a
    blocking syscall, short but real, and doing it on the event loop would put it in front of an
    in-flight query.

    A missing broker reports ``subscribers: 0`` rather than failing. The broker is created in
    :func:`src.main.lifespan`, so its absence means an application assembled without one — in which
    case zero subscriptions is the literal truth.
    """
    broker = getattr(request.app.state, "broker", None)
    subscribers = 0
    if broker is not None:
        try:
            subscribers = int(broker.subscriber_count())
        except Exception:  # noqa: BLE001 - a probe must not fail on a diagnostic it cannot read
            logger.debug("could not read the broker's subscriber count", exc_info=True)

    rss = _resident_bytes()
    return MemoryResponse(
        pid=os.getpid(),
        memory_mb=round(rss / BYTES_PER_MIB, 1) if rss is not None else None,
        rss_bytes=rss,
        subscribers=subscribers,
        available=rss is not None,
    )
