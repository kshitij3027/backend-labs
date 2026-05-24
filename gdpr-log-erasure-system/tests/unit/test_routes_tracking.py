import datetime as dt
import pytest
from pydantic import ValidationError
from src.api.schemas import UserDataMappingCreate, UserDataMappingResponse


def test_create_payload_round_trip():
    p = UserDataMappingCreate(user_id="u-1", data_type="system_logs", storage_location="loc-a")
    assert p.user_id == "u-1"
    assert p.data_path is None
    assert p.metadata is None


def test_create_payload_empty_user_id_invalid():
    with pytest.raises(ValidationError):
        UserDataMappingCreate(user_id="", data_type="x", storage_location="y")


def test_create_payload_with_metadata_and_path():
    p = UserDataMappingCreate(
        user_id="u-2", data_type="analytics_events",
        storage_location="loc-b", data_path="/var/log/x", metadata={"k": "v"},
    )
    assert p.data_path == "/var/log/x"
    assert p.metadata == {"k": "v"}


def test_response_maps_metadata_json_alias():
    class StubRow:
        id = 1
        user_id = "u-3"
        data_type = "system_logs"
        storage_location = "loc"
        data_path = None
        metadata_json = {"a": 1}
        created_at = dt.datetime(2026, 1, 1)

    resp = UserDataMappingResponse.model_validate(StubRow())
    dumped = resp.model_dump(by_alias=True)
    assert dumped["metadata"] == {"a": 1}
    assert "metadata_json" not in dumped
