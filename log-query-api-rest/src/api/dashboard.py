"""The browser dashboard — ``GET /``, a static page served by this same app.

**Why the dashboard is not a second service.** Everything the page needs is already published by
``/api/v1``: a token endpoint, a paginated list, an SSE tail, a stats aggregate, and the
``X-RateLimit-*`` triple on every response. A React build behind nginx would add a bundler, a
node toolchain, a second compose service and a CORS story — all to consume an API that is
already complete. So the dashboard is three flat files under ``src/static`` and this router.
The Dockerfile's ``COPY src/ ./src/`` carries them with no change, ``requirements.txt`` is
untouched (there is no template engine here — the page is static and hardcodes ``/api/v1``),
and because the page is served from the same origin as the API it makes, **the dashboard path
has no CORS in it at all**. The ``expose_headers`` list in ``src/main.py`` still matters for
third-party clients; it just is not what makes this page able to read the rate-limit headers.

.. rubric:: This route is deliberately unauthenticated and unmetered

It is a *static resource* — HTML, CSS and JS bytes with no user data in them. Authentication
happens in the browser, in ``app.js``, against ``POST /api/v1/auth/token``, and every subsequent
call the page makes is a normal, fully-gated ``/api/v1`` request carrying a bearer token. Gating
the shell itself would be theatre: there is nothing to protect in it, and it would leave a caller
with no page from which to log in. Together with ``/health`` this is one of only two data-free
routes in the app, and the same reasoning applies to both — see ``src/api/health.py``.

The file is checked for existence rather than assumed. A partially-built image (a bad
``.dockerignore`` rule, an editor mid-write) should answer ``404`` with a readable explanation,
not a ``500`` from a missing path deep inside Starlette's file response.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException, status
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from starlette.responses import Response
from starlette.types import Scope

router = APIRouter(tags=["dashboard"])

#: Sent on the dashboard HTML and on every file under ``/static``.
#:
#: ``no-cache`` is widely misread as "do not cache". It means "cache freely, but **revalidate
#: before every reuse**" — which is exactly what is wanted here and why the directive is not
#: ``no-store`` (that one forbids storage outright and would throw away a working cache to fix
#: a freshness problem).
#:
#: The reason this has to be explicit: Starlette stamps ``ETag`` and ``Last-Modified`` on a file
#: response but **no** ``Cache-Control``, and a response carrying a validator with no explicit
#: freshness directive licenses a browser to *heuristically* cache it — commonly ~10% of the age
#: implied by ``Last-Modified``, reused with no request to the server at all. These three assets
#: have unversioned filenames with no content hash in them, so their URLs do not change when
#: their bytes do. That leaves revalidation as the only thing standing between a redeploy and a
#: returning user silently running last week's ``app.js`` against this week's API — the precise
#: shape of a "works for me, broken for them" report. Revalidation is nearly free anyway: the
#: ``ETag`` is already there, so the steady state is a ``304`` with an empty body.
CACHE_CONTROL = "no-cache"


class NoCacheStaticFiles(StaticFiles):
    """:class:`~starlette.staticfiles.StaticFiles` that revalidates instead of being guessed at.

    ``get_response`` is the override point rather than ``file_response``: it is the single
    funnel :meth:`StaticFiles.__call__` goes through, so stamping here covers the ``200``, the
    ``304`` that :meth:`StaticFiles.file_response` substitutes when the client's validator still
    matches, and any redirect — one place, no case left uncovered. (Stamping the 304 is not
    redundant belt-and-braces: a ``304`` that omits ``Cache-Control`` re-opens the same
    heuristic window it was just closed on, for every reuse after the first.)

    404s are raised as ``HTTPException`` inside ``get_response`` and so never reach this header,
    which is correct — freshness advice about a file that does not exist is meaningless.
    """

    async def get_response(self, path: str, scope: Scope) -> Response:
        response = await super().get_response(path, scope)
        # Assigned, not `setdefault`ed. The whole point is that this value is guaranteed rather
        # than dependent on what the pinned Starlette does or does not already put there.
        response.headers["Cache-Control"] = CACHE_CONTROL
        return response

#: ``src/static`` — resolved from this module rather than the process working directory, so the
#: page is found identically under ``uvicorn`` from ``/app``, under ``pytest`` from the repo root,
#: and under any ``docker run -w`` an operator picks. A relative ``"src/static"`` would work in
#: exactly one of those three.
STATIC_DIR = Path(__file__).resolve().parent.parent / "static"

#: The page itself. The only file this router serves directly; the rest go through the
#: ``/static`` mount that ``src/main.py`` installs.
INDEX_FILE = STATIC_DIR / "index.html"


@router.get(
    "/",
    response_class=HTMLResponse,
    summary="Browser dashboard",
    description=(
        "The single-page dashboard. Public, unversioned, unauthenticated and unmetered — it is "
        "static markup, and the page authenticates itself against `/api/v1/auth/token` once "
        "loaded. Returns 404 if the static assets are missing from the image."
    ),
    responses={
        status.HTTP_200_OK: {
            "content": {"text/html": {"schema": {"type": "string"}}},
            "description": "The dashboard HTML.",
        },
        status.HTTP_404_NOT_FOUND: {
            "description": "The static assets were not built into this image.",
        },
    },
)
def dashboard() -> FileResponse:
    """Return ``src/static/index.html``.

    ``FileResponse`` rather than reading the bytes and wrapping them: it streams and sets
    ``Content-Length``, ``ETag`` and ``Last-Modified`` from a single ``stat``.

    Note what it does **not** do — unlike :class:`NoCacheStaticFiles` above, a bare
    ``FileResponse`` publishes those validators but never *checks* the incoming
    ``If-None-Match``; the conditional logic lives in ``StaticFiles``, not in the response
    class. So this route always returns the whole document. That is a deliberate accept rather
    than an oversight: the shell is a few kilobytes, it is the one file that must never be
    stale because it is what names the other two, and the assets that are actually worth a
    ``304`` (``app.js`` is an order of magnitude larger) go through the mount that does
    revalidate properly.
    """
    if not INDEX_FILE.is_file():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                "dashboard assets are not present in this build; the API itself is unaffected "
                "— see /docs"
            ),
        )
    return FileResponse(
        INDEX_FILE,
        media_type="text/html",
        headers={"Cache-Control": CACHE_CONTROL},
    )
