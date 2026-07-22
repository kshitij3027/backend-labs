"""Integration tests for ``GET /api/v1/logs/stream`` — the SSE tail, over a real socket.

.. rubric:: Why this file starts its own server

Every other integration suite here drives ``starlette.testclient.TestClient``. This one cannot,
and the reason is worth stating plainly because it is the single most expensive thing to
rediscover:

**``TestClient`` and ``httpx.ASGITransport`` both buffer the entire response body.**
``ASGITransport.handle_async_request`` awaits the ASGI app to *completion* before returning a
response; ``TestClient`` does the same through a portal. Neither can hand control back to the
test while a response is still open. An SSE stream is open-ended by definition, so
"connect, then POST, then assert the POST arrived" does not merely run slowly under them — it
**deadlocks**, because the POST can never be issued until the stream that is waiting for it ends.

So these tests run the app on an **in-process uvicorn server bound to an ephemeral port**
(``port=0``, with the OS-assigned port read back off the listening socket) and drive it with
``httpx.AsyncClient`` + ``httpx_sse.aconnect_sse`` over real HTTP. Non-streaming assertions —
``401``, ``403``, the ``429`` past the stream cap — are plain ``client.get`` calls against the
same server, because those responses complete.

.. rubric:: Why every test has a timeout

A bug in the fan-out, the sentinel, or the exit paths does not usually produce a wrong value; it
produces a coroutine parked forever on a queue nothing will write to. Without a hard
``asyncio.timeout`` that is a CI job that hangs until the runner kills it, with no failing test
to point at. :data:`TEST_TIMEOUT` converts every such regression into a loud, located failure.

.. rubric:: No sleeps

The ``ready`` frame is what makes that possible. A stream emits it immediately after subscribing
and before any log frame, so a test can block until the subscription provably exists, *then*
write, and assert on what comes back. Without it, "the stream delivers entries appended after I
connected" could only be written as a sleep-and-hope. ``?max_events=N`` closes the other end of
the same problem: the stream terminates on its own after N frames, so a test can drain an
iterator to completion instead of guessing when to stop reading.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Iterator
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Any

import httpx
import pytest
import uvicorn
from fastapi import FastAPI
from httpx_sse import ServerSentEvent, aconnect_sse

# `AppStatus` is sse-starlette's module-level shutdown latch. It is reset per test below; see
# `_reset_sse_app_status` for why that matters more than it looks like it should.
from sse_starlette.sse import AppStatus

from src.api.v1 import MAX_STREAMS_DETAIL
from src.auth import DEV_PASSWORDS
from src.config import Settings
from src.main import API_V1_PREFIX, Runtime, create_app

#: Hard ceiling on any single test. Every streaming assertion runs inside one of these, so a
#: regression that would park a reader forever fails in seconds and names itself.
TEST_TIMEOUT = 15.0

#: How long to wait for uvicorn to bind its ephemeral port before giving up on the fixture.
SERVER_START_TIMEOUT = 10.0

STREAM_URL = f"{API_V1_PREFIX}/logs/stream"
LOGS_URL = f"{API_V1_PREFIX}/logs"
SEARCH_URL = f"{API_V1_PREFIX}/logs/search"
TOKEN_URL = f"{API_V1_PREFIX}/auth/token"


# ---------------------------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_sse_app_status() -> Iterator[None]:
    """Clear sse-starlette's cross-test shutdown latch between tests.

    ``sse_starlette.sse.AppStatus`` holds a **module-level** ``anyio.Event`` that every
    ``EventSourceResponse`` waits on so a SIGTERM can end all live streams at once. It is created
    lazily by the first stream in the process and then reused forever — but an ``asyncio.Event``
    binds to the loop that first awaits it, and pytest-asyncio gives each test a *new* loop. The
    second test in this module would therefore await an event bound to a dead loop and die with
    "attached to a different event loop", nowhere near the code that caused it.

    Resetting the singleton is the fix sse-starlette's own suite uses. It is autouse because
    forgetting it in one new test would break the *next* one, which is the worst kind of failure
    to debug.
    """
    AppStatus.should_exit = False
    AppStatus.should_exit_event = None
    yield
    AppStatus.should_exit = False
    AppStatus.should_exit_event = None


@asynccontextmanager
async def serve(app: FastAPI) -> AsyncIterator[str]:
    """Run ``app`` on a real socket at an OS-assigned port; yield its base URL.

    ``port=0`` rather than a fixed port so a sibling project already holding ``:8000`` — which
    happens constantly in this repo — cannot make this suite fail for an unrelated reason, and so
    two of these can run concurrently. The bound port is read back off the listening socket
    because that is the only place the OS's choice is recorded.

    ``lifespan="off"``: the app is built with an **injected** Runtime, which by construction has
    no lifespan (see :func:`src.main.create_app`). Saying so explicitly keeps uvicorn from
    negotiating a protocol there is nothing behind, and keeps the store under test exactly the
    one the fixture put there.
    """
    config = uvicorn.Config(
        app,
        host="127.0.0.1",
        port=0,
        log_level="warning",
        lifespan="off",
        access_log=False,
        # A stream that is somehow still open at teardown must not wedge the suite. Tests close
        # their own connections; this is the backstop for the case where one does not.
        timeout_graceful_shutdown=2,
    )
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    try:
        async with asyncio.timeout(SERVER_START_TIMEOUT):
            while not server.started:
                if task.done():
                    # Surfaces a bind/startup failure as itself instead of as a timeout.
                    task.result()
                    raise RuntimeError("uvicorn exited before it started serving")
                await asyncio.sleep(0.005)
        host, port = server.servers[0].sockets[0].getsockname()[:2]
        yield f"http://{host}:{port}"
    finally:
        server.should_exit = True
        try:
            async with asyncio.timeout(SERVER_START_TIMEOUT):
                await task
        except TimeoutError:  # pragma: no cover - only reachable if a stream leaked
            server.force_exit = True
            await task


class Stream:
    """One open SSE connection, read through a **single** frame iterator.

    The iterator is created once and held, rather than calling ``aiter_sse()`` per read: two
    iterators over one response would each try to consume the same byte stream, and the frames a
    test is waiting for would go to whichever one happened to be scheduled.
    """

    def __init__(self, source: Any) -> None:
        self.response: httpx.Response = source.response
        self._frames = source.aiter_sse()

    async def start(self) -> dict[str, Any]:
        """Block until the `ready` frame arrives and return its payload.

        This is the synchronisation primitive the whole file is built on: when this returns, the
        server has provably registered the subscription, so anything appended from here on must
        reach this stream. It replaces every ``sleep()`` a naive version of these tests would
        need.
        """
        first = await anext(self._frames)
        assert first.event == "ready", (
            f"the first frame must be `ready`, got {first.event!r} — without it no test can know "
            "when the subscription exists"
        )
        return json.loads(first.data)

    async def next_frame(self) -> ServerSentEvent:
        """The next frame, whatever it is. Raises ``StopAsyncIteration`` at end of stream."""
        return await anext(self._frames)

    async def collect(self) -> list[ServerSentEvent]:
        """Every remaining frame, to end of stream. Only safe with ``max_events`` set."""
        return [frame async for frame in self._frames]


@asynccontextmanager
async def open_stream(
    client: httpx.AsyncClient,
    *,
    token: str | None = None,
    headers: dict[str, str] | None = None,
    **params: Any,
) -> AsyncIterator[Stream]:
    """Open ``GET /logs/stream`` and yield a :class:`Stream`. Closes the connection on exit."""
    request_headers = dict(headers or {})
    if token is not None:
        request_headers.setdefault("Authorization", f"Bearer {token}")
    async with aconnect_sse(
        client, "GET", STREAM_URL, params=params, headers=request_headers
    ) as source:
        # Bound to a local **on purpose** — do not collapse this into `yield Stream(source)`.
        #
        # `Stream` owns the `aiter_sse()` async generator. If the only reference to the `Stream`
        # is the caller's variable, then a caller that rebinds it — `for _ in range(n): held =
        # await stack.enter_async_context(open_stream(...))`, which is exactly how the cap tests
        # hold several streams at once — drops the previous one at the next iteration. CPython
        # frees it immediately on refcount, the event loop's async-generator finalizer then calls
        # `aclose()` on the abandoned iterator, and that `GeneratorExit` propagates down through
        # `aiter_lines` -> `aiter_text` -> `aiter_bytes` -> `aiter_raw` and **closes the httpx
        # response and its TCP connection**. The server does exactly the right thing with that —
        # sees `http.disconnect`, unsubscribes, frees the slot — and the test then measures a
        # server that is behaving correctly against a connection the test did not mean to close.
        #
        # Keeping the reference here makes this context manager's contract ("the connection is
        # open until you leave the block") true by construction, instead of true only while the
        # caller happens to keep the returned value bound.
        stream = Stream(source)
        yield stream


class Harness:
    """The live server plus the helpers every test needs against it."""

    def __init__(self, client: httpx.AsyncClient, app: FastAPI, settings: Settings) -> None:
        self.client = client
        self.app = app
        self.settings = settings
        self._tokens: dict[str, str] = {}

    async def token(self, username: str) -> str:
        """A JWT for a demo account, minted through the real ``POST /auth/token``.

        Cached per username: a token is a pure function of the account here, and re-minting one
        costs a bcrypt verify and a rate-limit token for nothing.
        """
        if username not in self._tokens:
            response = await self.client.post(
                TOKEN_URL,
                data={"username": username, "password": DEV_PASSWORDS[username]},
            )
            assert response.status_code == 200, response.text
            self._tokens[username] = response.json()["access_token"]
        return self._tokens[username]

    async def auth(self, username: str) -> dict[str, str]:
        """``Authorization`` header for a demo account."""
        return {"Authorization": f"Bearer {await self.token(username)}"}

    async def post_log(self, **overrides: Any) -> dict[str, Any]:
        """Append one entry as ``writer`` and return the ``201`` body (a full ``LogEntry``)."""
        body = {
            "level": "ERROR",
            "service": "auth-svc",
            "host": "node-1",
            "message": "streamed marker",
        } | overrides
        response = await self.client.post(
            LOGS_URL, json=body, headers=await self.auth("writer")
        )
        assert response.status_code == 201, response.text
        return response.json()

    async def next_seq(self) -> int:
        """The seq the next append will take, read from a throwaway stream's `ready` frame.

        The `ready` frame is the only place the server publishes this, and it is what lets a
        resume test compute the exact seq of an entry it just posted instead of assuming the
        store started empty.
        """
        async with open_stream(self.client, token=await self.token("analyst")) as probe:
            return (await probe.start())["next_seq"]


@pytest.fixture()
async def api(settings: Settings) -> AsyncIterator[Harness]:
    """A live server over a **fresh, empty** store, plus an HTTP client aimed at it.

    ``Runtime.build`` rather than ``build_seeded``: an empty ring makes every seq in these tests
    a small number a human can read, and a stream test's subject is what happens *after* it
    connects, so a pre-existing corpus is noise the resume assertions would have to work around.
    """
    app = create_app(runtime=Runtime.build(settings))
    async with serve(app) as base_url:
        async with httpx.AsyncClient(
            base_url=base_url,
            # No read timeout: a stream is *supposed* to sit idle. `asyncio.timeout` in each
            # test is the real bound, and it produces a far better failure message than a
            # transport-level ReadTimeout would.
            timeout=httpx.Timeout(10.0, read=None),
        ) as client:
            yield Harness(client, app, settings)


# ---------------------------------------------------------------------------------------------
# Routing and the gates
# ---------------------------------------------------------------------------------------------


async def test_stream_returns_event_stream_not_404(api: Harness) -> None:
    """**Pins route ordering.** ``/logs/{entry_id}`` is a wildcard and would swallow this path.

    Declared after it, ``GET /logs/stream`` matches the path-param route and comes back as a
    ``404`` for an entry whose id happens to be the string ``"stream"`` — a failure that looks
    like a missing entry rather than a misordered router. This assertion is the only thing
    keeping the two declarations in the right order.
    """
    async with asyncio.timeout(TEST_TIMEOUT):
        async with open_stream(api.client, token=await api.token("analyst")) as stream:
            assert stream.response.status_code == 200, await stream.response.aread()
            assert stream.response.headers["content-type"].startswith("text/event-stream")
            # Reading a real frame proves it reached the stream handler, not merely something
            # that answered 200.
            assert "next_seq" in await stream.start()


async def test_stream_sets_proxy_and_referrer_headers(api: Harness) -> None:
    """The response headers are load-bearing, not decoration.

    ``X-Accel-Buffering: no`` stops nginx and friends from buffering the response, which would
    turn a live tail into a batch that arrives whenever the proxy's buffer fills — the feature
    still "works" and is completely useless. ``Referrer-Policy: no-referrer`` exists because of
    ``?access_token=``: without it any URL the page later navigates to receives the full stream
    URL, token included, in the ``Referer`` header.
    """
    async with asyncio.timeout(TEST_TIMEOUT):
        async with open_stream(api.client, token=await api.token("analyst")) as stream:
            headers = stream.response.headers
            assert headers["cache-control"] == "no-cache"
            assert headers["x-accel-buffering"] == "no"
            assert headers["referrer-policy"] == "no-referrer"


async def test_stream_without_token_is_401(api: Harness) -> None:
    """No credential at all is ``401`` **with** the challenge — "I don't know who you are"."""
    async with asyncio.timeout(TEST_TIMEOUT):
        response = await api.client.get(STREAM_URL)

    assert response.status_code == 401, response.text
    assert response.headers["WWW-Authenticate"] == "Bearer"


async def test_stream_requires_analyst_403_for_viewer(api: Harness) -> None:
    """A ``viewer``'s token is perfectly valid and simply does not out-rank this route.

    ``403``, never ``401``: refreshing the credential would not help, and the *absence* of
    ``WWW-Authenticate`` is the machine-readable half of "do not retry".
    """
    async with asyncio.timeout(TEST_TIMEOUT):
        response = await api.client.get(STREAM_URL, headers=await api.auth("viewer"))

    assert response.status_code == 403, response.text
    assert "WWW-Authenticate" not in response.headers
    assert "analyst" in response.json()["detail"]


# ---------------------------------------------------------------------------------------------
# Delivery
# ---------------------------------------------------------------------------------------------


async def test_stream_delivers_entry_appended_after_connect(api: Harness) -> None:
    """**The point of the whole feature**, asserted without a single sleep.

    ``start()`` returns only once the server has emitted `ready`, which it does immediately after
    registering the subscription. So the ``POST`` below provably happens *after* this connection
    is live, and the frame that comes back provably belongs to it. ``max_events=1`` then ends the
    stream on its own, so nothing has to guess when to stop reading.
    """
    async with asyncio.timeout(TEST_TIMEOUT):
        async with open_stream(
            api.client, token=await api.token("analyst"), max_events=1
        ) as stream:
            ready = await stream.start()
            assert ready["resumed_from"] is None
            assert ready["replayed"] == 0
            assert ready["truncated"] is False

            posted = await api.post_log(message="appeared after connect")

            frame = await stream.next_frame()
            assert frame.event == "log"
            assert json.loads(frame.data)["id"] == posted["id"]
            assert int(frame.id) == ready["next_seq"], (
                "`ready.next_seq` promises where the live window starts; the first entry "
                "appended after connect must land exactly there"
            )


async def test_stream_frame_matches_paginated_entry_schema(api: Harness) -> None:
    """One schema, two delivery modes: a streamed frame is byte-identical to the fetched entry.

    This is what lets a client hand a streamed frame to whatever already parses a paginated page.
    If the two ever diverged, every consumer would need a second parser and the divergence would
    show up as a field quietly missing from the live view.
    """
    async with asyncio.timeout(TEST_TIMEOUT):
        async with open_stream(
            api.client, token=await api.token("analyst"), max_events=1
        ) as stream:
            await stream.start()
            posted = await api.post_log(message="one schema two modes", attrs={"k": "v"})
            streamed = json.loads((await stream.next_frame()).data)

        fetched = await api.client.get(
            f"{LOGS_URL}/{posted['id']}", headers=await api.auth("analyst")
        )
        assert fetched.status_code == 200, fetched.text

    assert streamed == posted, "the stream frame must equal the write route's 201 body"
    assert streamed == fetched.json(), "…and the read route's representation of the same entry"


async def test_stream_respects_level_and_service_filters(api: Harness) -> None:
    """The tail uses the same filter vocabulary as ``GET /logs``, applied at fan-out.

    Two non-matching entries are appended *before* the matching one, so passing this test
    requires the filter to actually exclude them rather than merely to deliver things in order.
    ``max_events=1`` then proves the stream saw exactly one frame: had a non-matching entry been
    delivered, it — not the third entry — would be the frame under assertion.
    """
    async with asyncio.timeout(TEST_TIMEOUT):
        async with open_stream(
            api.client,
            token=await api.token("analyst"),
            level="ERROR",
            service="payments-svc",
            max_events=1,
        ) as stream:
            await stream.start()

            await api.post_log(level="INFO", service="payments-svc", message="wrong level")
            await api.post_log(level="ERROR", service="auth-svc", message="wrong service")
            wanted = await api.post_log(
                level="ERROR", service="payments-svc", message="the only match"
            )

            frames = await stream.collect()

    assert [json.loads(frame.data)["id"] for frame in frames] == [wanted["id"]]
    assert json.loads(frames[0].data)["message"] == "the only match"


async def test_max_events_terminates_stream(api: Harness) -> None:
    """``?max_events=N`` closes the stream after N log frames, so demos and tests terminate.

    Without it a ``curl -N`` demo hangs until the operator interrupts it and every test here
    would need an out-of-band way to decide it had seen enough. Three entries are appended
    against a limit of two, so the cut is observable rather than incidental.
    """
    async with asyncio.timeout(TEST_TIMEOUT):
        async with open_stream(
            api.client, token=await api.token("analyst"), max_events=2
        ) as stream:
            await stream.start()
            posted = [await api.post_log(message=f"entry {i}") for i in range(3)]
            frames = await stream.collect()

    assert [json.loads(frame.data)["id"] for frame in frames] == [
        posted[0]["id"],
        posted[1]["id"],
    ]
    assert all(frame.event == "log" for frame in frames)


async def test_stream_ids_are_seqs_and_advance(api: Harness) -> None:
    """``id`` is the entry's seq — which is what makes ``Last-Event-ID`` resume work at all.

    A separate identifier would need its own mapping back to a store position; the seq already
    *is* one, so the reconnect protocol comes for free.
    """
    async with asyncio.timeout(TEST_TIMEOUT):
        async with open_stream(
            api.client, token=await api.token("analyst"), max_events=3
        ) as stream:
            ready = await stream.start()
            for i in range(3):
                await api.post_log(message=f"entry {i}")
            frames = await stream.collect()

    base = ready["next_seq"]
    assert [int(frame.id) for frame in frames] == [base, base + 1, base + 2]


# ---------------------------------------------------------------------------------------------
# `Last-Event-ID` resume
# ---------------------------------------------------------------------------------------------


async def test_last_event_id_header_resumes(api: Harness) -> None:
    """A reconnect replays strictly *after* the acknowledged id — the browser's own protocol.

    ``EventSource`` sends this header automatically on every reconnect, so honouring it is what
    turns a dropped connection into a gap-free resume rather than a hole in the client's
    timeline.
    """
    async with asyncio.timeout(TEST_TIMEOUT):
        posted = [await api.post_log(message=f"entry {i}") for i in range(3)]
        # Read the seq boundary off the server rather than assuming the store began empty.
        first_seq = await api.next_seq() - len(posted)

        async with open_stream(
            api.client,
            token=await api.token("analyst"),
            max_events=2,
            headers={"Last-Event-ID": str(first_seq)},
        ) as stream:
            ready = await stream.start()
            frames = await stream.collect()

    assert ready["resumed_from"] == first_seq
    assert ready["replayed"] == 2, "strictly after the acknowledged id — never re-sending it"
    assert ready["truncated"] is False
    assert [int(frame.id) for frame in frames] == [first_seq + 1, first_seq + 2]
    assert [json.loads(frame.data)["id"] for frame in frames] == [
        posted[1]["id"],
        posted[2]["id"],
    ]


async def test_last_event_id_query_param_resumes(api: Harness) -> None:
    """The query-param spelling exists because ``EventSource`` cannot set the header *first*.

    A browser only sends ``Last-Event-ID`` on a *reconnect* of a connection it already had. After
    a page reload the checkpoint lives in the page's storage and the very first connection is the
    one that needs to carry it — and that connection cannot set a header. Hence the parameter.
    """
    async with asyncio.timeout(TEST_TIMEOUT):
        posted = [await api.post_log(message=f"entry {i}") for i in range(3)]
        first_seq = await api.next_seq() - len(posted)

        async with open_stream(
            api.client,
            token=await api.token("analyst"),
            max_events=2,
            last_event_id=str(first_seq),
        ) as stream:
            ready = await stream.start()
            frames = await stream.collect()

    assert ready["resumed_from"] == first_seq
    assert ready["replayed"] == 2
    assert [json.loads(frame.data)["id"] for frame in frames] == [
        posted[1]["id"],
        posted[2]["id"],
    ]


async def test_last_event_id_header_wins_over_the_query_param(api: Harness) -> None:
    """Both present: the header wins, because a reconnect's header is fresher than a stale URL.

    The URL was minted once, when the page first opened the stream; the header is re-sent by the
    browser with the id of the last frame it actually received. Preferring the URL would rewind
    a long-lived stream to its starting checkpoint on every reconnect.
    """
    async with asyncio.timeout(TEST_TIMEOUT):
        await api.post_log(message="entry 0")
        await api.post_log(message="entry 1")
        first_seq = await api.next_seq() - 2

        async with open_stream(
            api.client,
            token=await api.token("analyst"),
            max_events=1,
            last_event_id=str(first_seq - 1),  # the stale URL checkpoint: would replay 2
            headers={"Last-Event-ID": str(first_seq)},  # the live one: replays 1
        ) as stream:
            ready = await stream.start()

    assert ready["resumed_from"] == first_seq
    assert ready["replayed"] == 1


@pytest.mark.parametrize("garbage", ["", "null", "not-a-number", "12.5", "-5", " "])
async def test_garbage_last_event_id_is_ignored_not_an_error(
    api: Harness, garbage: str
) -> None:
    """Unparseable checkpoints degrade to "resume from now" — they never fail the connection.

    This value round-trips through proxies, browser reconnect logic and third-party SSE clients,
    several of which have historically sent an empty string, a quoted value or the literal
    ``"null"``. A ``400`` there would break a reconnect for a client that did nothing wrong and
    cannot fix it. Negative values are ignored on the same grounds: seqs start at 0, so a
    negative id did not come from this server.
    """
    async with asyncio.timeout(TEST_TIMEOUT):
        await api.post_log(message="pre-existing entry")

        async with open_stream(
            api.client, token=await api.token("analyst"), last_event_id=garbage
        ) as stream:
            assert stream.response.status_code == 200
            ready = await stream.start()

    assert ready["resumed_from"] is None, f"{garbage!r} must be ignored, not parsed"
    assert ready["replayed"] == 0, "an ignored checkpoint replays nothing — it is not `-1`"
    assert ready["truncated"] is False


# ---------------------------------------------------------------------------------------------
# The concurrent-stream cap
# ---------------------------------------------------------------------------------------------


async def test_exceeding_max_streams_is_429(api: Harness) -> None:
    """One long-lived connection and one thousand quick requests are not the same kind of load.

    Hence a separate ceiling with a **distinct** ``detail``: both limits answer ``429``, and an
    operator reading a log line — or this test reading a body — has to be able to tell which one
    bit without counting requests.
    """
    cap = api.settings.max_streams_per_principal
    token = await api.token("analyst")

    async with asyncio.timeout(TEST_TIMEOUT):
        async with AsyncExitStack() as stack:
            for _ in range(cap):
                held = await stack.enter_async_context(open_stream(api.client, token=token))
                # Blocking on `ready` guarantees the subscription is registered server-side
                # before the next connection is opened, so the cap is measured against `cap`
                # live streams rather than against however many happened to have been accepted.
                await held.start()

            refused = await api.client.get(
                STREAM_URL, headers={"Authorization": f"Bearer {token}"}
            )

    assert refused.status_code == 429, refused.text
    detail = refused.json()["detail"]
    assert MAX_STREAMS_DETAIL in detail
    assert str(cap) in detail
    assert "rate limit" not in detail.lower(), (
        "the stream cap and the request bucket both answer 429 and must stay distinguishable"
    )


async def test_stream_cap_is_per_principal(api: Harness) -> None:
    """A principal at its ceiling must not lock anyone else out — the counter is keyed by subject."""
    cap = api.settings.max_streams_per_principal

    async with asyncio.timeout(TEST_TIMEOUT):
        async with AsyncExitStack() as stack:
            for _ in range(cap):
                held = await stack.enter_async_context(
                    open_stream(api.client, token=await api.token("analyst"))
                )
                await held.start()

            # `admin` outranks analyst and holds zero streams, so its own cap is untouched.
            other = await stack.enter_async_context(
                open_stream(api.client, token=await api.token("admin"))
            )
            assert other.response.status_code == 200
            assert "next_seq" in await other.start()


async def test_stream_slot_released_after_disconnect(api: Harness) -> None:
    """**The idempotent-decrement pin.** A closed stream gives its slot back exactly once.

    The release is made observable without a sleep by ending one stream *deterministically*: it
    is opened with ``max_events=1``, so a single append exhausts it and the generator's
    ``finally`` — one of the six exit paths that unsubscribe — runs before the response body is
    closed. By the time the client sees end-of-stream, the server has already released the slot.

    A decrement that ran per *call* rather than per *subscription* would over-release here (the
    ``finally`` and the response's ``BackgroundTask`` both fire for the same connection) and the
    cap would quietly stop counting the two streams still open — which the final assertion
    catches.
    """
    cap = api.settings.max_streams_per_principal
    assert cap >= 2, "this test needs room to hold a stream open while another one ends"
    token = await api.token("analyst")

    async with asyncio.timeout(TEST_TIMEOUT):
        async with AsyncExitStack() as stack:
            for _ in range(cap - 1):
                held = await stack.enter_async_context(open_stream(api.client, token=token))
                await held.start()

            # The one that will end on its own.
            async with open_stream(api.client, token=token, max_events=1) as ending:
                await ending.start()
                await api.post_log(message="ends the short stream")
                assert len(await ending.collect()) == 1, "max_events=1 must end the stream"

            # The slot is free again — this connect would be a 429 if it were not.
            reopened = await stack.enter_async_context(open_stream(api.client, token=token))
            assert reopened.response.status_code == 200, await reopened.response.aread()
            await reopened.start()

            # And exactly one slot was returned, not two: the cap still bites at `cap`.
            refused = await api.client.get(
                STREAM_URL, headers={"Authorization": f"Bearer {token}"}
            )
            assert refused.status_code == 429, (
                "the counter over-released — a `finally` and a BackgroundTask both ran for the "
                "same subscription and each took a decrement"
            )


# ---------------------------------------------------------------------------------------------
# The `?access_token=` escape hatch, and its boundary
# ---------------------------------------------------------------------------------------------


async def test_access_token_query_param_authenticates(api: Harness) -> None:
    """The documented escape hatch: ``EventSource`` cannot send an ``Authorization`` header.

    It is the *only* way a browser consumes SSE without shipping a polyfill, so the alternative
    to this parameter is "the dashboard cannot have a live tail". The cost — a token in one
    access log line — is mitigated by a 30-minute TTL, the analyst-only gate, and the
    ``Referrer-Policy: no-referrer`` asserted above.
    """
    token = await api.token("analyst")

    async with asyncio.timeout(TEST_TIMEOUT):
        async with open_stream(api.client, access_token=token, max_events=1) as stream:
            assert stream.response.status_code == 200
            assert "Authorization" not in stream.response.request.headers, (
                "this test is only meaningful if no header was sent"
            )
            await stream.start()
            posted = await api.post_log(message="authenticated by query param")
            assert json.loads((await stream.next_frame()).data)["id"] == posted["id"]


async def test_access_token_query_param_rejected_on_search_route(api: Harness) -> None:
    """**The boundary.** Search's entire rationale is keeping search terms out of access logs.

    ``POST /logs/search`` uses a body precisely so a nested filter never reaches a proxy log.
    Accepting a token in *its* query string would put the credential in the same log the body was
    moved out of, and would do it on the one route that exists to avoid that. The hatch is
    ``/logs/stream``-only, and this test is what keeps it that way.
    """
    token = await api.token("analyst")

    async with asyncio.timeout(TEST_TIMEOUT):
        response = await api.client.post(
            SEARCH_URL, params={"access_token": token}, json={}
        )
        # And the same body with the header is fine — proving the 401 is about *where* the
        # credential was, not about the request being malformed.
        with_header = await api.client.post(
            SEARCH_URL, json={}, headers={"Authorization": f"Bearer {token}"}
        )

    assert response.status_code == 401, response.text
    assert response.headers["WWW-Authenticate"] == "Bearer"
    assert with_header.status_code == 200, with_header.text


async def test_authorization_header_wins_over_query_param(api: Harness) -> None:
    """When both are present the header decides — including when the header is the broken one.

    Precedence is only meaningful if it holds in the direction that *costs* something. A valid
    query token beside a garbage header must still be a ``401``: if the query param could rescue
    a rejected header, "the header takes precedence" would be a comment rather than behaviour.
    """
    good = await api.token("analyst")

    async with asyncio.timeout(TEST_TIMEOUT):
        # Good header, garbage query param -> the header is used, so this succeeds.
        async with open_stream(
            api.client, token=good, access_token="not-a-jwt-at-all"
        ) as stream:
            assert stream.response.status_code == 200
            assert "next_seq" in await stream.start()

        # Garbage header, good query param -> the header is still used, so this fails.
        refused = await api.client.get(
            STREAM_URL,
            params={"access_token": good},
            headers={"Authorization": "Bearer not-a-jwt-at-all"},
        )

    assert refused.status_code == 401, refused.text
    assert refused.headers["WWW-Authenticate"] == "Bearer"
