"""Integration tests for the C7 HTTP layer.

Every test spins up the full FastAPI application (including the lifespan
handler that mints the initial DEK and starts the rotation poll task)
via ``httpx.AsyncClient`` + ``ASGITransport(lifespan="on")``. No real
sockets, no Docker, no rebuild between tests — but the application's full
startup/shutdown flow runs each time so the test environment matches
production wiring exactly.

Why ``ASGITransport(lifespan="on")``?
-------------------------------------
httpx ships an ASGI transport that can optionally drive the application's
lifespan protocol. With ``lifespan="on"`` httpx calls the startup hook on
context-manager entry and the shutdown hook on exit, so our singletons
(keystore, processor, etc.) are constructed/torn down per test. That's
slightly more expensive than constructing them once and sharing across
the suite, but it eliminates inter-test state leakage (audit ring buffer
accumulating events, stats counters drifting) which is what we want.

Grouping
--------
Tests are grouped by endpoint into ``TestClass`` blocks for readability;
pytest discovers them via the standard ``test_*`` prefix on each method.
Every test is independently runnable.

Order matters?
--------------
No. The lifespan re-runs per test so each method gets a fresh keystore,
fresh stats, fresh audit buffer. ``TestStats.test_stats_after_encrypt``
makes its own encrypt call rather than relying on a previous test.
"""
from __future__ import annotations

import base64
import copy
import json
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

# Import the FastAPI app *after* tests.conftest sets MASTER_KEY_B64.
from src.main import app


# Mark the whole module as integration so a future "-m 'not integration'"
# selector still works.
pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


_FIXTURES_DIR = (
    Path(__file__).resolve().parent.parent / "fixtures"
)
"""Repo-relative path to the JSON fixtures shared with the unit tests."""


def _load_fixture(name: str) -> dict[str, Any]:
    """Load a JSON fixture and return a deep copy.

    Deep-copy so individual tests can mutate their copy without
    contaminating other tests that import the same fixture.
    """
    with (_FIXTURES_DIR / name).open() as f:
        return copy.deepcopy(json.load(f))


@pytest_asyncio.fixture
async def client() -> AsyncClient:  # type: ignore[misc]
    """An ``AsyncClient`` bound to the live FastAPI app with lifespan run.

    ``httpx==0.27.2`` (pinned in ``requirements.txt``) does not support the
    ``ASGITransport(lifespan="on")`` kwarg — that was added in httpx 0.28.
    We wrap the client in ``asgi_lifespan.LifespanManager`` instead, which
    drives the ASGI lifespan protocol explicitly and populates
    ``app.state`` with the singletons built by our startup handler.

    Each test gets a fresh app boot — see module docstring.
    """
    async with LifespanManager(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(
            transport=transport, base_url="http://test"
        ) as ac:
            yield ac


# ---------------------------------------------------------------------------
# Tests — Health
# ---------------------------------------------------------------------------


class TestHealth:
    """``GET /api/health`` smoke + payload tests."""

    @pytest.mark.asyncio
    async def test_health_returns_200(self, client: AsyncClient) -> None:
        """Liveness probe is reachable."""
        resp = await client.get("/api/health")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_health_payload_matches_contract(
        self, client: AsyncClient
    ) -> None:
        """Liveness payload matches the documented contract.

        The body shape is contracted with the Docker healthcheck — any
        future change has to be deliberate.
        """
        resp = await client.get("/api/health")
        assert resp.json() == {
            "status": "healthy",
            "service": "field-encryption-service",
        }


# ---------------------------------------------------------------------------
# Tests — Encrypt
# ---------------------------------------------------------------------------


class TestEncrypt:
    """``POST /v1/logs/encrypt`` — single-log encrypt."""

    @pytest.mark.asyncio
    async def test_encrypt_returns_200(self, client: AsyncClient) -> None:
        """E-commerce fixture round-trips through encrypt with status 200."""
        log = _load_fixture("ecommerce_log.json")
        resp = await client.post("/v1/logs/encrypt", json={"log": log})
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_encrypt_email_becomes_encrypted_field(
        self, client: AsyncClient
    ) -> None:
        """``customer_email`` is replaced with an EncryptedField dict."""
        log = _load_fixture("ecommerce_log.json")
        resp = await client.post("/v1/logs/encrypt", json={"log": log})
        body = resp.json()
        assert isinstance(body["customer_email"], dict)
        assert "encrypted_value" in body["customer_email"]
        assert body["customer_email"]["algorithm"] == "AES-256-GCM"

    @pytest.mark.asyncio
    async def test_encrypt_order_id_unchanged(
        self, client: AsyncClient
    ) -> None:
        """Operational fields stay readable after encrypt."""
        log = _load_fixture("ecommerce_log.json")
        resp = await client.post("/v1/logs/encrypt", json={"log": log})
        body = resp.json()
        # order_id is not PII; should be untouched.
        assert body["order_id"] == log["order_id"]
        # amount is also operational — survives as-is.
        assert body["amount"] == log["amount"]

    @pytest.mark.asyncio
    async def test_encrypt_validation_error_returns_422(
        self, client: AsyncClient
    ) -> None:
        """Pydantic validation error returns 422, not 500."""
        # Missing the required ``log`` field — pydantic rejects.
        resp = await client.post("/v1/logs/encrypt", json={"foo": "bar"})
        assert resp.status_code == 422


class TestEncryptDecryptRoundTrip:
    """Verify encrypt followed by decrypt recovers the original plaintext."""

    @pytest.mark.asyncio
    async def test_round_trip_recovers_email(self, client: AsyncClient) -> None:
        """Encrypt then decrypt gives back the original email value."""
        original = _load_fixture("ecommerce_log.json")
        encrypt_resp = await client.post(
            "/v1/logs/encrypt", json={"log": original}
        )
        assert encrypt_resp.status_code == 200
        encrypted = encrypt_resp.json()

        decrypt_resp = await client.post(
            "/v1/logs/decrypt", json={"log": encrypted}
        )
        assert decrypt_resp.status_code == 200
        decrypted = decrypt_resp.json()

        # Round-trip equality: email survives intact (values come back
        # as strings — see LogProcessor module docstring).
        assert decrypted["customer_email"] == original["customer_email"]
        assert decrypted["phone"] == original["phone"]

    @pytest.mark.asyncio
    async def test_round_trip_preserves_operational_fields(
        self, client: AsyncClient
    ) -> None:
        """Non-PII fields survive a round-trip unchanged."""
        original = _load_fixture("ecommerce_log.json")
        encrypted = (
            await client.post("/v1/logs/encrypt", json={"log": original})
        ).json()
        decrypted = (
            await client.post("/v1/logs/decrypt", json={"log": encrypted})
        ).json()

        assert decrypted["order_id"] == original["order_id"]
        # amount survives as a value-equal scalar; LogProcessor decodes
        # encrypted values as strings, but unchanged scalars are untouched.
        assert decrypted["amount"] == original["amount"]


class TestEncryptBatch:
    """``POST /v1/logs/encrypt/batch`` — multi-log encrypt."""

    @pytest.mark.asyncio
    async def test_batch_returns_list_of_same_length(
        self, client: AsyncClient
    ) -> None:
        """Batch of 5 logs returns 5 encrypted logs in order."""
        log = _load_fixture("ecommerce_log.json")
        logs = [copy.deepcopy(log) for _ in range(5)]
        resp = await client.post(
            "/v1/logs/encrypt/batch", json={"logs": logs}
        )
        assert resp.status_code == 200
        body = resp.json()
        assert "encrypted_logs" in body
        assert len(body["encrypted_logs"]) == 5

    @pytest.mark.asyncio
    async def test_batch_each_entry_encrypted(
        self, client: AsyncClient
    ) -> None:
        """Every log in the batch has its PII fields encrypted."""
        log = _load_fixture("ecommerce_log.json")
        logs = [copy.deepcopy(log) for _ in range(3)]
        resp = await client.post(
            "/v1/logs/encrypt/batch", json={"logs": logs}
        )
        body = resp.json()
        for entry in body["encrypted_logs"]:
            assert isinstance(entry["customer_email"], dict)
            assert entry["customer_email"]["algorithm"] == "AES-256-GCM"

    @pytest.mark.asyncio
    async def test_batch_empty_list(self, client: AsyncClient) -> None:
        """Empty batch returns empty list (not an error)."""
        resp = await client.post("/v1/logs/encrypt/batch", json={"logs": []})
        assert resp.status_code == 200
        assert resp.json()["encrypted_logs"] == []


# ---------------------------------------------------------------------------
# Tests — Detect
# ---------------------------------------------------------------------------


class TestDetect:
    """``POST /v1/detect`` — dry-run, returns detections without encrypting."""

    @pytest.mark.asyncio
    async def test_detect_returns_detections_list(
        self, client: AsyncClient
    ) -> None:
        """Detect emits at least one detection on the e-commerce fixture."""
        log = _load_fixture("ecommerce_log.json")
        resp = await client.post("/v1/detect", json={"log": log})
        assert resp.status_code == 200
        body = resp.json()
        assert "detections" in body
        assert isinstance(body["detections"], list)
        assert len(body["detections"]) >= 2  # email + phone

    @pytest.mark.asyncio
    async def test_detect_field_paths_include_email_and_phone(
        self, client: AsyncClient
    ) -> None:
        """The detection list includes the expected field paths."""
        log = _load_fixture("ecommerce_log.json")
        body = (
            await client.post("/v1/detect", json={"log": log})
        ).json()
        paths = {d["field_path"] for d in body["detections"]}
        assert "customer_email" in paths
        assert "phone" in paths

    @pytest.mark.asyncio
    async def test_detect_does_not_encrypt_input(
        self, client: AsyncClient
    ) -> None:
        """Detect is a dry-run: the response contains no ciphertext."""
        log = _load_fixture("ecommerce_log.json")
        body = (
            await client.post("/v1/detect", json={"log": log})
        ).json()
        # The response shape only carries the detection list — no
        # encrypted log echoed back.
        assert "encrypted_value" not in json.dumps(body)
        # No ``algorithm`` key either.
        assert "AES-256-GCM" not in json.dumps(body)


# ---------------------------------------------------------------------------
# Tests — Decrypt failure modes
# ---------------------------------------------------------------------------


class TestDecryptFailures:
    """Decrypt error mapping — 404 for missing key, 422 for tampered tag."""

    @pytest.mark.asyncio
    async def test_decrypt_unknown_key_returns_404(
        self, client: AsyncClient
    ) -> None:
        """Pointing ``key_id`` at a nonexistent key produces 404."""
        log = _load_fixture("ecommerce_log.json")
        # First encrypt to get a valid envelope.
        encrypted = (
            await client.post("/v1/logs/encrypt", json={"log": log})
        ).json()
        # Mutate to an unknown key id.
        encrypted["customer_email"]["key_id"] = "key-does-not-exist"

        resp = await client.post(
            "/v1/logs/decrypt", json={"log": encrypted}
        )
        assert resp.status_code == 404
        assert "key" in resp.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_decrypt_tampered_ciphertext_returns_422(
        self, client: AsyncClient
    ) -> None:
        """Flipping one bit in ``encrypted_value`` produces 422 (auth fail)."""
        log = _load_fixture("ecommerce_log.json")
        encrypted = (
            await client.post("/v1/logs/encrypt", json={"log": log})
        ).json()

        # Flip the last byte of the ciphertext blob.
        original_b64 = encrypted["customer_email"]["encrypted_value"]
        raw = bytearray(base64.b64decode(original_b64))
        raw[-1] ^= 0x01  # flip one bit
        tampered_b64 = base64.b64encode(bytes(raw)).decode("ascii")
        encrypted["customer_email"]["encrypted_value"] = tampered_b64

        resp = await client.post(
            "/v1/logs/decrypt", json={"log": encrypted}
        )
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Tests — Keys
# ---------------------------------------------------------------------------


class TestKeys:
    """``GET /v1/keys`` — DEK lifecycle listing."""

    @pytest.mark.asyncio
    async def test_keys_returns_at_least_one_key(
        self, client: AsyncClient
    ) -> None:
        """Startup mints one active DEK — keys list is non-empty."""
        resp = await client.get("/v1/keys")
        assert resp.status_code == 200
        body = resp.json()
        assert "keys" in body
        assert len(body["keys"]) >= 1

    @pytest.mark.asyncio
    async def test_keys_first_key_is_active(self, client: AsyncClient) -> None:
        """The initial DEK is in active status."""
        body = (await client.get("/v1/keys")).json()
        # At least one of the returned keys should be active.
        statuses = {k["status"] for k in body["keys"]}
        assert "active" in statuses
        # Top-level ``active_key_id`` should also be populated.
        assert body["active_key_id"] is not None


# ---------------------------------------------------------------------------
# Tests — Metrics
# ---------------------------------------------------------------------------


class TestMetrics:
    """``GET /metrics`` — Prometheus text endpoint."""

    @pytest.mark.asyncio
    async def test_metrics_endpoint_is_reachable(
        self, client: AsyncClient
    ) -> None:
        """``/metrics`` returns 200 in Prometheus text format."""
        resp = await client.get("/metrics")
        assert resp.status_code == 200
        # prometheus_client uses ``text/plain`` with a version tag.
        assert resp.headers["content-type"].startswith("text/plain")

    @pytest.mark.asyncio
    async def test_metrics_contains_custom_counter_after_encrypt(
        self, client: AsyncClient
    ) -> None:
        """After an encrypt call, ``encryptions_total`` shows up."""
        log = _load_fixture("ecommerce_log.json")
        # Make at least one encrypt so the counter has been touched.
        await client.post("/v1/logs/encrypt", json={"log": log})

        body = (await client.get("/metrics")).text
        # Counter family name appears in the exposition text.
        assert "encryptions_total" in body

    @pytest.mark.asyncio
    async def test_metrics_contains_instrumentator_default(
        self, client: AsyncClient
    ) -> None:
        """The prometheus_fastapi_instrumentator default metrics are present."""
        # Touch any endpoint to ensure the default middleware ran at
        # least once for this app instance.
        await client.get("/api/health")
        body = (await client.get("/metrics")).text
        # The instrumentator's default counter family is named
        # ``http_requests_total`` — assert it's in the output.
        assert "http_requests_total" in body


# ---------------------------------------------------------------------------
# Tests — Stats
# ---------------------------------------------------------------------------


class TestStats:
    """``GET /api/stats`` — atomic counter snapshot."""

    @pytest.mark.asyncio
    async def test_stats_returns_counters_dict(
        self, client: AsyncClient
    ) -> None:
        """Stats endpoint returns a JSON object with a ``counters`` dict."""
        resp = await client.get("/api/stats")
        assert resp.status_code == 200
        body = resp.json()
        assert "counters" in body
        assert isinstance(body["counters"], dict)
        # Well-known counters are pre-populated.
        assert "logs_processed" in body["counters"]

    @pytest.mark.asyncio
    async def test_stats_logs_processed_increments_after_encrypt(
        self, client: AsyncClient
    ) -> None:
        """``logs_processed`` increases after an encrypt call."""
        before = (await client.get("/api/stats")).json()["counters"][
            "logs_processed"
        ]

        log = _load_fixture("ecommerce_log.json")
        await client.post("/v1/logs/encrypt", json={"log": log})

        after = (await client.get("/api/stats")).json()["counters"][
            "logs_processed"
        ]
        assert after == before + 1


# ---------------------------------------------------------------------------
# Tests — Dashboard (C8)
# ---------------------------------------------------------------------------


class TestDashboard:
    """C8 dashboard surface — ``GET /``, the HTMX stats partial, the
    static-asset mount, and the form-encoded encrypt/decrypt endpoints.

    These tests share the same ``client`` fixture as the rest of the
    file: the FastAPI lifespan runs per test, so the keystore mints a
    fresh active DEK for every dashboard call. That makes the form-
    submission round-trip self-contained — encrypt and decrypt see the
    same key in the same process.
    """

    @pytest.mark.asyncio
    async def test_dashboard_page_returns_html(
        self, client: AsyncClient
    ) -> None:
        """``GET /`` returns a 200 HTML page with the expected sections."""
        resp = await client.get("/")
        assert resp.status_code == 200
        # Jinja2Templates returns ``text/html; charset=utf-8`` by default.
        assert resp.headers["content-type"].startswith("text/html")
        body = resp.text
        # Sanity-check that the rendered template carries the page title
        # and each of the three card headings the brief specified.
        assert "Field-Level Log Encryption" in body
        assert "Encrypt" in body
        assert "Decrypt" in body
        assert "Live Stats" in body

    @pytest.mark.asyncio
    async def test_stats_partial_returns_html_with_counters(
        self, client: AsyncClient
    ) -> None:
        """``GET /api/stats/html`` returns the partial with the dt labels."""
        resp = await client.get("/api/stats/html")
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/html")
        body = resp.text
        # The well-known counter labels render as ``<dt>`` entries; the
        # poll is what powers the dashboard's auto-refresh so those
        # labels MUST be present on every response.
        assert "Logs processed" in body
        assert "Errors" in body

    @pytest.mark.asyncio
    async def test_static_htmx_js_is_served(
        self, client: AsyncClient
    ) -> None:
        """``GET /static/htmx.min.js`` returns the vendored placeholder."""
        resp = await client.get("/static/htmx.min.js")
        assert resp.status_code == 200
        # The marker check is case-insensitive so a future drop-in of
        # the full minified bundle (which may title-case the banner)
        # still satisfies the assertion.
        assert "htmx" in resp.text.lower()

    @pytest.mark.asyncio
    async def test_static_css_is_served(self, client: AsyncClient) -> None:
        """``GET /static/dashboard.css`` returns CSS with the stats class."""
        resp = await client.get("/static/dashboard.css")
        assert resp.status_code == 200
        # Starlette serves ``.css`` as ``text/css``; we just assert the
        # family rather than the exact charset suffix.
        assert resp.headers["content-type"].startswith("text/css")
        # ``stats-list`` is the class name the partial relies on for
        # the two-column layout — a regression in either side would
        # surface here.
        assert "stats-list" in resp.text

    @pytest.mark.asyncio
    async def test_dashboard_encrypt_form_returns_encrypted_html(
        self, client: AsyncClient
    ) -> None:
        """Form-encoded ``raw_log`` -> HTML containing ``encrypted_value``."""
        # ``httpx.AsyncClient.post(data=...)`` form-encodes the body and
        # sets ``Content-Type: application/x-www-form-urlencoded``, which
        # is what FastAPI's ``Form(...)`` parameter parses.
        raw = '{"customer_email":"a@b.com","order_id":"o1"}'
        resp = await client.post(
            "/api/dashboard/encrypt",
            data={"raw_log": raw},
        )
        assert resp.status_code == 200
        body = resp.text
        # The encrypted payload is JSON-pretty-printed inside a <pre>;
        # the ``encrypted_value`` key is part of the EncryptedField dump
        # and is the load-bearing assertion that detection + encrypt
        # both fired.
        assert "encrypted_value" in body
        # The ``<pre>`` shell preserves the id so subsequent submissions
        # still find their HTMX target.
        assert 'id="encrypt-output"' in body

    @pytest.mark.asyncio
    async def test_dashboard_round_trip_recovers_email(
        self, client: AsyncClient
    ) -> None:
        """Encrypt via the form, then decrypt the result, gets the email back.

        The encrypt response is HTML-escaped JSON inside a ``<pre>``. We
        unescape and parse it back out to drive the decrypt form, which
        mirrors what the browser does when a user copies one output into
        the other textarea.
        """
        original_email = "alice@example.com"
        raw = f'{{"customer_email":"{original_email}","order_id":"o1"}}'

        # 1) Encrypt via the dashboard endpoint.
        enc_resp = await client.post(
            "/api/dashboard/encrypt",
            data={"raw_log": raw},
        )
        assert enc_resp.status_code == 200

        # 2) Extract the JSON payload from the ``<pre>``. The body is
        #    HTML-escaped JSON; ``html.unescape`` reverses the escape
        #    so we can ``json.loads`` it.
        import html as _html  # local import: tests aren't on a hot path

        body = enc_resp.text
        start = body.index(">") + 1
        end = body.rindex("</pre>")
        encrypted_json = json.loads(_html.unescape(body[start:end]))

        # 3) Decrypt via the dashboard endpoint with the same JSON
        #    re-serialized into the textarea.
        dec_resp = await client.post(
            "/api/dashboard/decrypt",
            data={"raw_log": json.dumps(encrypted_json)},
        )
        assert dec_resp.status_code == 200
        # The decrypted plaintext email appears as a substring of the
        # rendered HTML <pre>. We don't need to round-trip-parse it
        # again — the substring assertion is sufficient and resilient
        # to whitespace changes in the pretty-printer.
        assert original_email in dec_resp.text

    @pytest.mark.asyncio
    async def test_dashboard_encrypt_invalid_json_returns_error_html(
        self, client: AsyncClient
    ) -> None:
        """Malformed JSON returns 200 HTML with the ``error`` class, not 5xx.

        HTMX swaps responses regardless of status code (in our default
        config), but a 5xx would block the swap on most htmx setups.
        Returning a 200 with an inline error <pre> keeps the UX
        consistent — the user sees the error in the same target slot.
        """
        resp = await client.post(
            "/api/dashboard/encrypt",
            data={"raw_log": "{not json"},
        )
        # 200, NOT 5xx — the failure is surfaced inline as HTML.
        assert resp.status_code == 200
        body = resp.text
        # The error styling hook is the ``error`` class on the <pre>;
        # the substring assertion lets us evolve the exact message
        # text without breaking the test.
        assert "error" in body.lower()
