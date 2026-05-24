import pytest
from src.services.stats_service import compute_statistics
from src.persistence.models import (
    ErasureRequest, RequestState, RequestType, UserDataMapping,
)


@pytest.mark.asyncio
async def test_stats_empty_system(session_factory):
    async with session_factory() as s:
        result = await compute_statistics(s)
    assert result["total_mappings"] == 0
    assert result["unique_users"] == 0
    assert result["completion_rate"] == 0.0
    assert result["data_type_counts"] == {}


@pytest.mark.asyncio
async def test_stats_populated(session_factory):
    async with session_factory() as s:
        # 5 mappings across 2 users, 3 data_types
        for i, (uid, dtype) in enumerate([
            ("u-a", "system_logs"),
            ("u-a", "system_logs"),  # different storage → unique
            ("u-a", "analytics_events"),
            ("u-b", "system_logs"),
            ("u-b", "personal_profile"),
        ]):
            s.add(UserDataMapping(
                user_id=uid, data_type=dtype, storage_location=f"loc-{i}",
            ))
        # 4 requests: 2 COMPLETED, 1 FAILED, 1 PENDING
        for state in [RequestState.COMPLETED, RequestState.COMPLETED, RequestState.FAILED, RequestState.PENDING]:
            s.add(ErasureRequest(user_id="u-a", request_type=RequestType.DELETE, state=state))
        await s.commit()

        result = await compute_statistics(s)

    assert result["total_mappings"] == 5
    assert result["unique_users"] == 2
    assert result["completion_rate"] == 0.5
    assert result["data_type_counts"] == {
        "analytics_events": 1, "personal_profile": 1, "system_logs": 3,
    }
