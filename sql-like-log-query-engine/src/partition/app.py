from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI

from src.shared.config import PartitionSettings
from src.shared.models import PartitionMetadata, TimeRange

from .data_generator import generate_logs
from .executor import LocalExecutor
from .routes import router
from .storage import LogStorage


def create_partition_app(settings: PartitionSettings) -> FastAPI:
    """Build and return the partition-node FastAPI app.

    The lifespan generates the synthetic log data once at startup, builds
    the :class:`LogStorage` with the configured indexes, and attaches
    ``storage``, ``executor``, and ``metadata`` to ``app.state`` so the
    route handlers can reach them without needing global state.
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        time_range = TimeRange(
            start=datetime.fromisoformat(settings.partition_time_start),
            end=datetime.fromisoformat(settings.partition_time_end),
        )

        indexed_fields = settings.indexed_fields_list()

        records = generate_logs(
            partition_id=settings.partition_id,
            time_range=time_range,
            count=settings.log_sample_count,
        )
        storage = LogStorage(records=records, indexed_fields=indexed_fields)
        executor = LocalExecutor(storage=storage)

        metadata = PartitionMetadata(
            id=settings.partition_id,
            url=f"http://{settings.partition_id}:{settings.partition_port}",
            time_range=time_range,
            indexed_fields=indexed_fields,
            healthy=True,
        )

        app.state.settings = settings
        app.state.storage = storage
        app.state.executor = executor
        app.state.metadata = metadata

        yield

    app = FastAPI(
        title=f"Partition Node ({settings.partition_id})",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.include_router(router)
    return app
