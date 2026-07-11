"""Integration tests for the C6 WebSocket surface and CORS middleware.

Drive the real ``/ws`` route and the CORS-configured HTTP surface through
:class:`~fastapi.testclient.TestClient`:

* the ``"ping"`` -> ``"pong"`` keepalive, and that other inbound text is ignored while
  the connection stays open;
* the POST -> WS broadcast: analyzing an incident pushes
  ``{"type": "incident_update", "data": <report>}`` to a connected client;
* the CORS middleware is installed — a cross-origin preflight and a simple cross-origin
  GET both carry an ``access-control-allow-origin`` header — plus the ``cors_origins``
  parsing branches on the pure ``_cors_kwargs`` helper.

**WebSocket + loop note.** A bare ``TestClient(app)`` (the ``client`` fixture) is *not*
entered as a context manager, so every ``client.<verb>()`` call spins up its own
short-lived event loop/portal. That is fine for a self-contained ``/ws`` exchange
(ping/pong lives entirely inside the one WebSocket session's loop), but the
broadcast test must issue an HTTP POST *while a WebSocket is open* — the broadcast can
only reach the socket if the POST runs on the **same** loop. So the broadcast test
enters ``TestClient(app)`` as a context manager, which pins a single shared portal for
both the WebSocket session and the POST. Keeping that detail here (rather than relying
on TestClient timing) is what makes the test deterministic instead of flaky.
"""

from fastapi.testclient import TestClient

from src.api import _cors_kwargs, create_app
from src.config import Settings
from src.generators import generate_incident
from src.main import Runtime


def _wildcard_client() -> TestClient:
    """A TestClient whose app is built with an explicit wildcard CORS config.

    Built from pure defaults (``_env_file=None``) so the CORS assertions are hermetic
    regardless of any ambient ``.env`` / ``CORS_ORIGINS`` in the environment.
    """
    app = create_app(runtime=Runtime.build(Settings(_env_file=None, cors_origins="*")))
    return TestClient(app)


# --- WebSocket keepalive ---------------------------------------------------------


def test_ws_ping_pong(client):
    with client.websocket_connect("/ws") as ws:
        ws.send_text("ping")
        assert ws.receive_text() == "pong"


def test_ws_ignores_non_ping_text(client):
    # A non-"ping" message is silently ignored; the connection stays open and still
    # answers a subsequent ping.
    with client.websocket_connect("/ws") as ws:
        ws.send_text("not-a-ping")
        ws.send_text("ping")
        assert ws.receive_text() == "pong"


# --- Broadcast on analyze (POST -> WS) -------------------------------------------


def test_analyze_incident_broadcasts_to_ws_client(app):
    payload = [event.model_dump() for event in generate_incident(seed=1).events]

    # Context-managed TestClient => one shared loop/portal for the WebSocket session
    # AND the POST, so the broadcast actually reaches the socket (see module docstring).
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as ws:
            response = client.post("/api/analyze-incident", json=payload)
            assert response.status_code == 200
            posted_id = response.json()["incident_id"]
            frame = ws.receive_json()

    assert frame["type"] == "incident_update"
    # The full report rides in `data`: same incident id, with ranked causes populated.
    assert frame["data"]["incident_id"] == posted_id
    assert frame["data"]["root_causes"], "the broadcast report carries ranked causes"


def test_analyze_incident_broadcasts_to_every_client(app):
    payload = [event.model_dump() for event in generate_incident(seed=2).events]

    with TestClient(app) as client:
        with client.websocket_connect("/ws") as ws_a:
            with client.websocket_connect("/ws") as ws_b:
                response = client.post("/api/analyze-incident", json=payload)
                assert response.status_code == 200
                frame_a = ws_a.receive_json()
                frame_b = ws_b.receive_json()

    # Both connected clients received the same incident_update frame.
    assert frame_a["type"] == frame_b["type"] == "incident_update"
    assert (
        frame_a["data"]["incident_id"]
        == frame_b["data"]["incident_id"]
        == response.json()["incident_id"]
    )


# --- CORS middleware (installed) -------------------------------------------------


def test_cors_preflight_allows_cross_origin():
    client = _wildcard_client()

    response = client.options(
        "/api/analyze-incident",
        headers={
            "Origin": "http://dashboard.example.com",
            "Access-Control-Request-Method": "POST",
        },
    )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "*"


def test_cors_header_on_simple_cross_origin_get():
    client = _wildcard_client()

    response = client.get(
        "/api/health", headers={"Origin": "http://dashboard.example.com"}
    )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "*"


# --- CORS origin parsing (_cors_kwargs branches) ---------------------------------


def test_cors_kwargs_wildcard_disables_credentials():
    kwargs = _cors_kwargs("*")
    assert kwargs["allow_origins"] == ["*"]
    assert kwargs["allow_credentials"] is False
    assert kwargs["allow_methods"] == ["*"]
    assert kwargs["allow_headers"] == ["*"]


def test_cors_kwargs_explicit_list_enables_credentials():
    kwargs = _cors_kwargs("http://a.example.com, http://b.example.com")
    # Comma-split, whitespace-trimmed, order preserved.
    assert kwargs["allow_origins"] == ["http://a.example.com", "http://b.example.com"]
    assert kwargs["allow_credentials"] is True


def test_cors_kwargs_wildcard_wins_when_mixed():
    # A "*" anywhere in the value means allow-any (and therefore no credentials).
    kwargs = _cors_kwargs("http://a.example.com,*")
    assert kwargs["allow_origins"] == ["*"]
    assert kwargs["allow_credentials"] is False
