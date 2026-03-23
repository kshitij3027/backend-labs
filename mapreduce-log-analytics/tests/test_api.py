import asyncio
import os

import pytest
from httpx import ASGITransport, AsyncClient

from src.api import app
from src.config import Config
from src.job_manager import JobManager


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(autouse=True)
def _setup_app_state():
    """Initialize app state that lifespan normally sets up."""
    config = Config.from_env()
    os.makedirs(config.upload_dir, exist_ok=True)
    os.makedirs(config.output_dir, exist_ok=True)
    app.state.config = config
    app.state.job_manager = JobManager(config)
    yield
    # Clean up state
    if hasattr(app.state, "job_manager"):
        del app.state.job_manager
    if hasattr(app.state, "config"):
        del app.state.config


@pytest.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


class TestHealthEndpoint:
    @pytest.mark.asyncio
    async def test_health(self, client):
        resp = await client.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}


class TestFunctionsEndpoint:
    @pytest.mark.asyncio
    async def test_list_functions(self, client):
        resp = await client.get("/api/functions")
        assert resp.status_code == 200
        data = resp.json()
        assert "word_count" in data
        assert "security" in data


class TestJobsEndpoint:
    @pytest.mark.asyncio
    async def test_submit_job(self, client, sample_json_logs):
        resp = await client.post("/api/jobs/submit", json={
            "analysis_type": "WORD_COUNT",
            "input_files": [sample_json_logs],
        })
        assert resp.status_code == 201
        data = resp.json()
        assert "job_id" in data
        assert data["status"] in ("PENDING", "MAPPING", "SHUFFLING", "REDUCING", "DONE")

    @pytest.mark.asyncio
    async def test_list_jobs(self, client, sample_json_logs):
        await client.post("/api/jobs/submit", json={
            "analysis_type": "WORD_COUNT",
            "input_files": [sample_json_logs],
        })
        resp = await client.get("/api/jobs")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) >= 1

    @pytest.mark.asyncio
    async def test_get_job(self, client, sample_json_logs):
        submit_resp = await client.post("/api/jobs/submit", json={
            "analysis_type": "WORD_COUNT",
            "input_files": [sample_json_logs],
        })
        job_id = submit_resp.json()["job_id"]
        resp = await client.get(f"/api/jobs/{job_id}")
        assert resp.status_code == 200
        assert resp.json()["job_id"] == job_id

    @pytest.mark.asyncio
    async def test_get_unknown_job(self, client):
        resp = await client.get("/api/jobs/nonexistent")
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_submit_with_missing_file(self, client):
        resp = await client.post("/api/jobs/submit", json={
            "analysis_type": "WORD_COUNT",
            "input_files": ["/nonexistent/path.log"],
        })
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_job_completes(self, client, sample_json_logs):
        """Submit a job and wait for it to complete."""
        submit_resp = await client.post("/api/jobs/submit", json={
            "analysis_type": "WORD_COUNT",
            "input_files": [sample_json_logs],
        })
        job_id = submit_resp.json()["job_id"]

        # Poll until completed or timeout
        for _ in range(30):
            resp = await client.get(f"/api/jobs/{job_id}")
            status = resp.json()["status"]
            if status in ("COMPLETED", "FAILED"):
                break
            await asyncio.sleep(0.5)

        final = resp.json()
        assert final["status"] == "COMPLETED"
        assert final["results"] is not None
        assert final["execution_time"] > 0

    @pytest.mark.asyncio
    async def test_dashboard_placeholder(self, client):
        resp = await client.get("/dashboard")
        assert resp.status_code == 200
