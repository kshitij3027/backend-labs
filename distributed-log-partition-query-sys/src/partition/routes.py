import time
from fastapi import APIRouter, Request
from src.models import Query, QueryResponse

router = APIRouter()


@router.get("/health")
async def health(request: Request):
    storage = request.app.state.storage
    config = request.app.state.config
    tr = storage.time_range
    return {
        "status": "healthy",
        "partition_id": config.partition_id,
        "log_count": storage.count,
        "time_range": {
            "start": tr[0].isoformat() if tr else None,
            "end": tr[1].isoformat() if tr else None,
        },
    }


@router.post("/query")
async def query_logs(query: Query, request: Request):
    start_time = time.time()
    storage = request.app.state.storage
    search_engine = request.app.state.search_engine

    results = search_engine.search(storage, query)
    elapsed_ms = (time.time() - start_time) * 1000

    return QueryResponse(
        query_id=query.query_id,
        total_results=len(results),
        partitions_queried=1,
        partitions_successful=1,
        total_execution_time_ms=round(elapsed_ms, 2),
        results=results,
    )
