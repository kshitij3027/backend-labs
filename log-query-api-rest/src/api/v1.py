"""The versioned data router — every route under the hard ``/api/v1`` prefix.

``/api/v1`` is a **prefix on the router**, not a string repeated on each route, so no handler in
this file can accidentally ship at the wrong version. The point of the prefix is that a breaking
change gets a new namespace instead of a changelog entry: a future v2 mounts a **second** router
beside this one in :func:`src.main.create_app` and leaves v1 byte-identical for the clients that
still speak it. Nobody edits the shapes in here to "upgrade" them — that is precisely the failure
mode versioning exists to prevent. (``/health``, ``/docs``, ``/redoc`` and ``/openapi.json`` stay
unversioned; see ``src/api/health.py`` for why a liveness probe must not move with the API.)

.. rubric:: Reading shared state

Handlers reach the store and the settings through ``request.app.state.runtime`` rather than by
importing :mod:`src.main` — ``src.main`` imports *this* module, so importing back would be a
cycle at startup. Every read is defensive (:func:`_runtime_parts`) and a missing collaborator
degrades to a documented empty answer rather than a ``500``: an API whose read path can be
crashed by half-wired process state is not a read path worth having.

.. rubric:: Where the two informational headers come from

``X-Page-Limit-Clamped`` and ``X-Cursor-Truncated`` are attached through the handler's injected
``response: Response``, which is the right tool **here specifically**: both are success-path-only
facts, and every failure mode in this file raises before either could be set. C8's rate-limit
headers are the opposite kind of thing — they matter most on ``401``/``403``/``429``, and headers
set on an injected ``Response`` do **not** survive the exception path — so those are attached by
``RequestContextMiddleware`` instead. The distinction is deliberate, not an inconsistency.

.. rubric:: Error bodies

Handlers raise :class:`fastapi.HTTPException` with a plain ``detail`` string. FastAPI's default
handler renders ``{"detail": …}``, which is exactly the required half of
:class:`~src.models.ErrorBody` (``code`` and ``request_id`` are optional there), and
``RequestContextMiddleware`` already stamps ``X-Request-ID`` on **every** response including this
one — so the correlation id is not duplicated into the body. One envelope, one place that
produces it.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Path,
    Query,
    Request,
    Response,
    status,
)
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import ValidationError

from src.auth import Principal, authenticate, create_access_token
from src.config import Settings
from src.deps import WWW_AUTHENTICATE, current_principal, settings_from_request
from src.models import (
    CLAMPED_HEADER,
    CURSOR_TRUNCATED_HEADER,
    MAX_Q_LEN,
    ErrorBody,
    LogEntry,
    LogLevel,
    LogPage,
    LogQuery,
    PageInfo,
    PrincipalResponse,
    SortOrder,
    TokenResponse,
    clamp_limit,
)
from src.store import Filter, InvalidCursor, LogStore, decode_cursor, encode_cursor

#: The prefix is spelled here and documented as ``src.main.API_V1_PREFIX`` there. It is
#: deliberately **not** imported from ``src.main``: that module imports this one, so the import
#: would be a cycle. ``tests/integration/test_logs_api.py`` asserts the two strings are equal, so
#: the duplication is pinned rather than merely hoped for.
router = APIRouter(prefix="/api/v1")

#: How much of a client-supplied id is echoed back in a ``404`` message. Bounded so a caller
#: cannot use the error body as an arbitrary-length reflector; long enough that a real id (a
#: uuid4/uuid5 hex is 32 characters) is always shown in full.
_ECHO_ID_CHARS = 64

#: The **single** ``401`` detail the token endpoint ever returns.
#:
#: One string for both "no such user" and "wrong password", deliberately. ``src.auth.authenticate``
#: already goes to the trouble of making the two paths cost the same wall-clock time so the
#: endpoint is not a timing oracle; returning ``"unknown user"`` versus ``"wrong password"`` would
#: hand back through the response body exactly the enumeration signal that work removed from the
#: clock. A caller who genuinely owns the account can tell the two apart themselves; an attacker
#: mapping the user list cannot. ``tests/integration/test_auth_api.py`` asserts the two responses
#: are byte-identical.
INVALID_CREDENTIALS_DETAIL = "incorrect username or password"


# ---------------------------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------------------------


def _runtime_parts(request: Request) -> tuple[LogStore | None, Settings | None]:
    """Read the store and the settings off ``app.state.runtime``, defensively.

    Returns ``(None, None)`` for a missing or half-wired runtime instead of raising. Both
    ``getattr`` chains are deliberate: this is the convention every route in the project follows
    (see ``src/api/health.py``), and it is what lets a data route degrade to an honest empty
    answer rather than a ``500``.

    **The residency test is ``is None``, never truthiness.** :class:`~src.store.LogStore` defines
    ``__len__``, so a perfectly healthy *empty* ring is falsy — ``if not store`` would silently
    take the degraded path on a brand-new process and keep taking it until the first append.
    """
    runtime = getattr(request.app.state, "runtime", None)
    store: LogStore | None = getattr(runtime, "store", None)
    settings: Settings | None = getattr(runtime, "settings", None)
    return store, settings


def _query_error_detail(exc: ValidationError) -> str:
    """Render a :class:`~src.models.LogQuery` coherence failure as a one-line ``400`` detail.

    :class:`~src.models.LogQuery` already owns the two rules that make a query incoherent rather
    than merely empty — ``cursor`` together with ``offset``, and ``since`` after ``until`` — and
    its own docstring says C5 maps them to ``400``. Re-checking them in the handler would be a
    second definition of "incoherent" that could drift from the first, so the model stays the
    single authority and this function only translates its complaint.

    Pydantic prefixes a ``ValueError`` raised inside a validator with ``"Value error, "``; that
    prefix is an implementation detail of the validation machinery, not something a client should
    have to read past, so it is stripped.
    """
    errors = exc.errors()
    if not errors:  # pragma: no cover - pydantic never raises with an empty error list
        return "invalid query parameters"
    return str(errors[0].get("msg", "invalid query parameters")).removeprefix("Value error, ")


def _paginate(
    store: LogStore | None,
    settings: Settings | None,
    flt: Filter,
    order: SortOrder,
    *,
    limit: int | None,
    cursor: str | None,
    offset: int | None,
    response: Response,
) -> LogPage:
    """Turn an already-built filter into one :class:`~src.models.LogPage`. **The only pager.**

    Everything between "I have a predicate" and "I have an envelope" lives here: clamping, cursor
    decode/validate, the scan, the two informational headers, and ``next_cursor`` minting. C9's
    ``POST /logs/search`` calls this verbatim with a compiled boolean-tree filter, which is why
    the signature takes a **filter object** rather than raw query parameters — the two routes then
    cannot drift into producing subtly different envelopes for the same match set. Any object
    exposing ``matches`` / ``index_hint`` / ``is_empty`` / ``fingerprint`` works; see
    :class:`~src.store.Filter`'s "Extension point for C9" note.

    .. rubric:: What ``total`` means

    ``total`` is the size of the **filtered** match set, and it is computed fresh only on a
    cursorless first page. From then on it is carried inside the cursor and echoed back
    unchanged, so every page of one walk reports the same number even while the corpus grows
    underneath it. A ``total`` recomputed per page would make a paginated UI's "showing 50 of N"
    counter flicker, and worse, would let the sum of the pages disagree with the header the client
    started from. As-of-walk-start is a coherent snapshot; per-page freshness is not.

    Args:
        store: The log store, or ``None`` on a half-wired runtime (degrades to an empty page).
        settings: Settings, or ``None`` on a half-wired runtime.
        flt: The compiled predicate. Every returned entry satisfies it.
        order: Scan direction. Bound into the cursor, so a walk cannot change direction mid-way.
        limit: The client's requested page size, pre-clamp. ``None`` means "no preference".
        cursor: ``next_cursor`` from a previous page, or ``None`` to start a fresh walk.
        offset: Row offset for "jump to page N". Mutually exclusive with ``cursor`` — the caller
            has already rejected the combination before reaching here.
        response: The handler's injected response, used only to attach the two informational
            headers.

    Raises:
        HTTPException: ``400`` when the cursor is malformed, or belongs to a different filter or
            sort order. Never a wrong page where an error is available.
    """
    if store is None or settings is None:
        # Degraded, never a 500. An honest empty page beats a stack trace: the caller learns
        # there is nothing to read, /health reports the real state, and no request 500s because
        # process wiring is incomplete.
        return LogPage(
            items=[],
            page=PageInfo(
                limit=max(1, limit or 1),
                returned=0,
                next_cursor=None,
                has_more=False,
                total=0,
            ),
        )

    effective_limit, clamped = clamp_limit(limit, settings)
    if clamped:
        # The header carries what the client ASKED for; `page.limit` carries what it got. Both
        # halves are needed — one value alone cannot tell a client that it was adjusted.
        response.headers[CLAMPED_HEADER] = str(limit)

    fingerprint = flt.fingerprint()
    if cursor is not None:
        try:
            state = decode_cursor(
                cursor, expected_fingerprint=fingerprint, expected_order=order
            )
        except InvalidCursor as exc:
            # A cursor from another walk is still a well-formed integer, so serving it would
            # return a page that is internally consistent and completely wrong. 400 is the only
            # honest answer.
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
            ) from exc
        start_after_seq: int | None = state.seq
        total = state.total
    else:
        start_after_seq = None
        total = store.count(flt)

    result = store.scan(
        flt,
        order,
        limit=effective_limit,
        start_after_seq=start_after_seq,
        skip=offset or 0,
    )

    if result.truncated:
        # The anchor had already been evicted from the ring. Returning fewer rows *silently* is
        # the one behaviour that must never happen, so the shortfall is advertised.
        response.headers[CURSOR_TRUNCATED_HEADER] = "true"

    # `next_seq` is already None unless `has_more`, so the conjunction is belt-and-braces — but
    # it states the contract locally instead of making a reader go and confirm it in the store.
    # Note this fires for offset pages too: an ad-hoc offset request hands back a cursor so a
    # client can switch to the stable walk without restarting from the top.
    next_cursor = (
        encode_cursor(
            seq=result.next_seq, order=order, fingerprint=fingerprint, total=total
        )
        if result.next_seq is not None and result.has_more
        else None
    )

    items = [record.entry for record in result.items]
    return LogPage(
        items=items,
        page=PageInfo(
            limit=effective_limit,
            returned=len(items),
            next_cursor=next_cursor,
            has_more=result.has_more,
            total=total,
        ),
    )


# ---------------------------------------------------------------------------------------------
# --- auth ---
#
# Declared above the `/logs` block purely for readability: `/auth/*` shares no path prefix with
# `/logs*`, so neither of these routes participates in the wildcard-ordering hazard documented
# below, and neither may be moved into that block.
#
# These are the only two routes in the file that are not a read over the corpus, and they sit at
# opposite ends of the auth chain: `POST /auth/token` is the one route with NO principal (it is
# what mints one), and `GET /auth/me` is the smallest possible route that has one.
# ---------------------------------------------------------------------------------------------


@router.post(
    "/auth/token",
    response_model=TokenResponse,
    tags=["auth"],
    summary="Exchange credentials for a signed JWT",
    responses={
        status.HTTP_401_UNAUTHORIZED: {
            "model": ErrorBody,
            "description": "Unknown username or wrong password — the body does not say which.",
        }
    },
)
async def issue_token(
    request: Request,
    form: Annotated[OAuth2PasswordRequestForm, Depends()],
) -> TokenResponse:
    """Exchange a username and password for a short-lived HS256 access token.

    The body is **form-encoded**, not JSON, because that is what RFC 6749 §4.3 specifies for a
    password grant — and honouring it is what makes two useful things work for free: the Swagger
    UI's *Authorize* button drives this endpoint directly, and the README's
    `curl -d 'username=…&password=…'` is a literal working command rather than an approximation.

    Both failure modes — unknown user, wrong password — return the identical `401`. See
    `INVALID_CREDENTIALS_DETAIL` for why, and `src.auth.authenticate` for the matching
    constant-cost treatment of the two paths.

    .. rubric:: This route is deliberately UNMETERED — do not add a rate-limit dependency

    C8 must **not** attach `rate_limit` here, and C12's p95 gate must exclude this path. Two
    reasons, and the first is structural:

    * The limiter is keyed on `principal.subject`, and at this point in the request there **is**
      no principal — that is the entire purpose of the route. Keying a bucket on the *claimed*
      username instead would let anyone lock a real account out of logging in by spraying wrong
      passwords at it, turning the rate limiter into a denial-of-service tool aimed at the
      users it is supposed to protect.
    * bcrypt is its own brake. Every call here, success or failure, costs one full hash at
      `BCRYPT_ROUNDS` (~250 ms at the production cost of 12), so the achievable request rate
      against this endpoint is roughly four per second per core no matter what a client tries.
      That is a far better password-guessing throttle than a token bucket, and it is why this
      route's latency is an order of magnitude above every other route's — measuring it inside
      the same p95 gate would mean either a meaningless gate or a permanently red one.
    """
    # `settings_from_request`, not `_runtime_parts`. Every other handler in this file degrades to
    # an empty answer when the runtime is half-wired, because an empty page is an honest reading
    # of "there is nothing here". There is no equivalent honest fallback for signing a token: the
    # only alternatives are a real key or no key at all, and issuing an unsigned credential is
    # categorically worse than failing. `settings_from_request` therefore never returns None — it
    # falls back to the process-wide `get_settings()`, which is what production would have used.
    settings = settings_from_request(request)

    user = authenticate(form.username, form.password, rounds=settings.bcrypt_rounds)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=INVALID_CREDENTIALS_DETAIL,
            headers=dict(WWW_AUTHENTICATE),
        )

    token, expires_at = create_access_token(
        subject=user.username,
        role=user.role,
        tier=user.tier,
        settings=settings,
    )
    return TokenResponse(
        access_token=token,
        # The configured TTL, not `expires_at - now`. `exp` is derived from `iat` as exactly this
        # many seconds, so this is the token's true lifetime rather than a recomputation that
        # could round to one second less and imply the server issued a stale token.
        expires_in=settings.access_token_ttl_min * 60,
        expires_at=expires_at,
        role=user.role,
        tier=user.tier,
    )


@router.get(
    "/auth/me",
    response_model=PrincipalResponse,
    tags=["auth"],
    summary="Echo the authenticated principal",
    responses={
        status.HTTP_401_UNAUTHORIZED: {
            "model": ErrorBody,
            "description": "Missing, malformed, expired or tampered token.",
        }
    },
)
async def read_me(
    principal: Annotated[Principal, Depends(current_principal)],
) -> PrincipalResponse:
    """Return the decoded principal — the fastest way to prove a token works.

    It touches no store, no filter and no pagination, so a `200` here isolates the auth chain
    from every other moving part: if this succeeds and a data route does not, the token is not
    the problem. A `401` carries `WWW-Authenticate: Bearer`, as RFC 9110 §11.6.1 requires.

    The declared `Depends(current_principal)` is also what puts the `bearerAuth` security
    requirement on this operation in the generated OpenAPI document — the README's claim that
    the auth contract is published rather than tribal knowledge is pinned by
    `test_openapi_documents_auth_routes`.
    """
    return PrincipalResponse.from_principal(principal)


# =============================================================================================
#  ROUTE DECLARATION ORDER IS LOAD-BEARING — READ THIS BEFORE ADDING A ROUTE
# ---------------------------------------------------------------------------------------------
#  Starlette matches routes in **declaration order, first match wins**, and `/logs/{entry_id}`
#  is a wildcard: it matches `/logs/search`, `/logs/stream`, and every other single-segment path
#  under `/logs`. Any literal `/logs/<something>` route declared AFTER it is therefore
#  unreachable — the request lands in `get_log_entry` with `entry_id="search"`, misses the store,
#  and comes back as a 404 that looks like a routing bug and is actually an ordering bug. It is
#  a genuinely confusing hour of debugging, and it is entirely avoidable:
#
#      * C9's  POST /logs/search   MUST be declared ABOVE `/logs/{entry_id}`.
#      * C10's GET  /logs/stream   MUST be declared ABOVE `/logs/{entry_id}`.
#
#  `/logs/{entry_id}` is deliberately the LAST `/logs*` route in this file and must stay last.
#  Add new literal `/logs/...` routes above it, never below.
# =============================================================================================


@router.get(
    "/logs",
    response_model=LogPage,
    tags=["logs"],
    summary="List log entries (paginated + filtered)",
    responses={
        status.HTTP_400_BAD_REQUEST: {
            "model": ErrorBody,
            "description": "Incoherent query: a bad/foreign cursor, cursor+offset together, "
            "or `since` after `until`.",
        }
    },
)
async def list_logs(
    request: Request,
    response: Response,
    level: Annotated[
        list[LogLevel] | None,
        Query(description="Match any of these levels. Repeatable: `?level=ERROR&level=FATAL`."),
    ] = None,
    service: Annotated[
        list[str] | None,
        Query(description="Match any of these service names. Repeatable."),
    ] = None,
    host: Annotated[
        list[str] | None,
        Query(description="Match any of these host names. Repeatable."),
    ] = None,
    since: Annotated[
        datetime | None,
        Query(
            description="Inclusive lower bound on `ts` (RFC-3339). Naive input is read as UTC.",
        ),
    ] = None,
    until: Annotated[
        datetime | None,
        Query(
            description="Inclusive upper bound on `ts` (RFC-3339). Naive input is read as UTC.",
        ),
    ] = None,
    q: Annotated[
        str | None,
        Query(
            max_length=MAX_Q_LEN,
            description=(
                f"Case-insensitive substring match over `message`. At most {MAX_Q_LEN} "
                "characters, so a hostile client cannot make the substring scan the expensive "
                "part of a full-corpus sweep."
            ),
        ),
    ] = None,
    order: Annotated[
        SortOrder,
        Query(description="Scan direction. `desc` (newest-first) is the default."),
    ] = SortOrder.DESC,
    limit: Annotated[
        int | None,
        Query(
            description=(
                "Requested page size. **Clamped** into [1, MAX_PAGE_SIZE], never rejected — an "
                "over-large value returns the ceiling plus `X-Page-Limit-Clamped`, not a 422."
            ),
        ),
    ] = None,
    cursor: Annotated[
        str | None,
        Query(
            description=(
                "Opaque `next_cursor` from a previous page. Bound to the filter and sort order "
                "it was minted for; replaying it against a different query is a 400, never a "
                "plausible-looking wrong page. Mutually exclusive with `offset`."
            ),
        ),
    ] = None,
    offset: Annotated[
        int | None,
        Query(
            ge=0,
            description=(
                "Row offset for 'jump to page N' use. Mutually exclusive with `cursor`, and it "
                "drifts under concurrent appends — inherent to offset paging, not a bug."
            ),
        ),
    ] = None,
) -> LogPage:
    """Walk the log corpus newest-first, with the flat ANDed query-string filters.

    Filters are **ANDed across fields and ORed within a field** — `?level=ERROR&level=FATAL&
    service=auth-svc` means *(ERROR or FATAL) and auth-svc*. That is the only thing a flat query
    string can honestly express; anything more structured belongs in `POST /api/v1/logs/search`.

    Two pagination styles are offered because they answer different questions:

    * **Cursor** (`?cursor=…`) — the correct one for a growing store. The cursor anchors on the
      last entry served, so entries appended mid-walk land beyond the anchor and can neither be
      skipped nor duplicated. `page.total` is frozen as-of-walk-start and carried in the cursor.
    * **Offset** (`?offset=…`) — for ad-hoc and "jump to page N" use, with the honest caveat that
      it drifts under concurrent writes.

    Supplying both is a `400`: a cursor already encodes a position, so there is no reading of the
    pair that isn't a guess. `limit` is always clamped, never rejected.
    """
    # LogQuery owns the coherence rules (cursor XOR offset; since <= until) AND the normalisation
    # of `since`/`until` to UTC. Validating through it rather than re-checking here means the two
    # bounds are already in the same timezone when they are compared — comparing a naive
    # datetime with an aware one raises TypeError in Python, which would turn a client mistake
    # into a 500.
    try:
        query = LogQuery(
            level=level,
            service=service,
            host=host,
            since=since,
            until=until,
            q=q,
            limit=limit,
            cursor=cursor,
            offset=offset,
            order=order,
        )
    except ValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=_query_error_detail(exc)
        ) from exc

    store, settings = _runtime_parts(request)
    return _paginate(
        store,
        settings,
        Filter.from_query(query),
        query.order,
        limit=query.limit,
        cursor=query.cursor,
        offset=query.offset,
        response=response,
    )


# --- `/logs/{entry_id}` is the wildcard. Keep it LAST. See the block above. -------------------


@router.get(
    "/logs/{entry_id}",
    response_model=LogEntry,
    tags=["logs"],
    summary="Fetch one log entry by id",
    responses={
        status.HTTP_404_NOT_FOUND: {
            "model": ErrorBody,
            "description": "No entry with that id is resident in the ring.",
        }
    },
)
async def get_log_entry(
    request: Request,
    entry_id: Annotated[
        str,
        Path(description="The entry's `id`, exactly as it appears in a list response."),
    ],
) -> LogEntry:
    """Return a single entry, or `404` if it is unknown **or has been evicted**.

    The store is a fixed-capacity ring, so "never existed" and "aged out" are indistinguishable
    from the outside and both answer `404`. Inventing a `410 Gone` for the second case would
    require retaining a tombstone per evicted id — an unbounded structure behind a bounded store,
    which is the exact memory leak the ring exists to prevent.
    """
    store, _ = _runtime_parts(request)
    entry = None if store is None else store.get(entry_id)
    if entry is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"no log entry with id {entry_id[:_ECHO_ID_CHARS]!r}",
        )
    return entry
