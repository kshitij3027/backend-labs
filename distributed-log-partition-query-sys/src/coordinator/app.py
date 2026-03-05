from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI

from src.config import CoordinatorConfig
from src.coordinator.cache import QueryCache
from src.coordinator.merger import ResultMerger
from src.coordinator.partition_map import PartitionMap
from src.coordinator.scatter_gather import ScatterGather
from src.models import PartitionInfo


@asynccontextmanager
async def lifespan(app: FastAPI):
    config: CoordinatorConfig = app.state.config

    # Create long-lived httpx client
    client = httpx.AsyncClient()

    # Initialize components
    partition_map = PartitionMap()
    for i, url in enumerate(config.partition_urls):
        partition_id = f"partition_{i + 1}"
        partition_map.register(
            PartitionInfo(
                partition_id=partition_id,
                url=url,
            )
        )

    scatter_gather = ScatterGather(client=client, timeout=config.query_timeout)
    merger = ResultMerger(max_merge_size=config.max_merge_size)
    cache = QueryCache(max_size=config.max_cache_size)

    app.state.client = client
    app.state.partition_map = partition_map
    app.state.scatter_gather = scatter_gather
    app.state.merger = merger
    app.state.cache = cache

    yield

    await client.aclose()


def create_coordinator_app(config: CoordinatorConfig) -> FastAPI:
    app = FastAPI(title="Query Coordinator", lifespan=lifespan)
    app.state.config = config

    from src.coordinator.routes import router

    app.include_router(router)

    return app
