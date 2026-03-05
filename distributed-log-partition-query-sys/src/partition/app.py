from contextlib import asynccontextmanager
from fastapi import FastAPI
from src.config import PartitionConfig
from src.partition.data_generator import generate_sample_logs
from src.partition.storage import LogStorage
from src.partition.search import LogSearchEngine


@asynccontextmanager
async def lifespan(app: FastAPI):
    config: PartitionConfig = app.state.config
    storage = LogStorage()
    entries = generate_sample_logs(config.log_count, config.days_back, config.partition_id)
    storage.load(entries)
    app.state.storage = storage
    app.state.search_engine = LogSearchEngine()
    yield


def create_partition_app(config: PartitionConfig) -> FastAPI:
    app = FastAPI(title=f"Partition Server - {config.partition_id}", lifespan=lifespan)
    app.state.config = config

    from src.partition.routes import router
    app.include_router(router)

    return app
