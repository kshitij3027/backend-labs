import time
import uuid

from fastapi import APIRouter, Request

from src.models import Query, QueryResponse

router = APIRouter()


@router.get("/health")
async def health(request: Request):
    partition_map = request.app.state.partition_map
    cache = request.app.state.cache
    return {
        "status": "healthy",
        "role": "coordinator",
        "partitions": {
            "total": partition_map.total_count,
            "healthy": partition_map.healthy_count,
        },
        "cache": cache.stats,
    }


@router.get("/stats")
async def stats(request: Request):
    partition_map = request.app.state.partition_map
    cache = request.app.state.cache
    return {
        "partitions": {
            "total": partition_map.total_count,
            "healthy": partition_map.healthy_count,
            "details": [
                {
                    "partition_id": p.partition_id,
                    "url": p.url,
                    "healthy": p.healthy,
                }
                for p in partition_map.get_all()
            ],
        },
        "cache": cache.stats,
    }


@router.post("/query")
async def query_logs(query: Query, request: Request):
    start_time = time.time()

    cache = request.app.state.cache
    partition_map = request.app.state.partition_map
    scatter_gather = request.app.state.scatter_gather
    merger = request.app.state.merger

    # Ensure query has an ID
    if not query.query_id:
        query.query_id = str(uuid.uuid4())

    # Check cache
    cached_response = cache.get(query)
    if cached_response is not None:
        elapsed_ms = (time.time() - start_time) * 1000
        return QueryResponse(
            query_id=query.query_id,
            total_results=cached_response.total_results,
            partitions_queried=cached_response.partitions_queried,
            partitions_successful=cached_response.partitions_successful,
            total_execution_time_ms=round(elapsed_ms, 2),
            results=cached_response.results,
            cached=True,
        )

    # Smart routing -- get relevant partitions
    relevant_partitions = partition_map.get_relevant_partitions(query)

    if not relevant_partitions:
        elapsed_ms = (time.time() - start_time) * 1000
        response = QueryResponse(
            query_id=query.query_id,
            total_results=0,
            partitions_queried=0,
            partitions_successful=0,
            total_execution_time_ms=round(elapsed_ms, 2),
            results=[],
        )
        return response

    # Scatter -- fan out to partitions
    scatter_results = await scatter_gather.scatter(relevant_partitions, query)

    # Update partition health based on results
    for result in scatter_results:
        if result.success:
            partition_map.mark_healthy(result.partition_id)
        else:
            partition_map.mark_unhealthy(result.partition_id)

    # Gather successful results
    partition_results = [r.entries for r in scatter_results if r.success]
    partitions_successful = len(partition_results)

    # Merge results
    merged = merger.merge(
        partition_results,
        sort_field=query.sort_field,
        sort_order=query.sort_order,
        limit=query.limit,
    )

    elapsed_ms = (time.time() - start_time) * 1000

    response = QueryResponse(
        query_id=query.query_id,
        total_results=len(merged),
        partitions_queried=len(relevant_partitions),
        partitions_successful=partitions_successful,
        total_execution_time_ms=round(elapsed_ms, 2),
        results=merged,
    )

    # Cache the response
    cache.put(query, response)

    return response
