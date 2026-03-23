import pytest
from pydantic import ValidationError

from src.models import JobCreate, JobStatus


class TestJobCreate:
    def test_valid_job_create(self):
        job = JobCreate(
            input_path="/data/logs.jsonl",
            map_fn="word_count",
            reduce_fn="sum",
        )
        assert job.input_path == "/data/logs.jsonl"
        assert job.map_fn == "word_count"
        assert job.reduce_fn == "sum"
        assert job.num_mappers == 2
        assert job.num_reducers == 2

    def test_custom_mapper_reducer_counts(self):
        job = JobCreate(
            input_path="/data/logs.jsonl",
            map_fn="extract_errors",
            reduce_fn="count",
            num_mappers=4,
            num_reducers=3,
        )
        assert job.num_mappers == 4
        assert job.num_reducers == 3

    def test_missing_required_fields(self):
        with pytest.raises(ValidationError):
            JobCreate(input_path="/data/logs.jsonl")

    def test_missing_input_path(self):
        with pytest.raises(ValidationError):
            JobCreate(map_fn="word_count", reduce_fn="sum")


class TestJobStatus:
    def test_expected_values(self):
        expected = {
            "PENDING",
            "MAPPING",
            "SHUFFLE_COMPLETE",
            "REDUCING",
            "COMPLETED",
            "FAILED",
            "CANCELLED",
        }
        actual = {s.value for s in JobStatus}
        assert actual == expected

    def test_status_is_string(self):
        assert JobStatus.PENDING == "PENDING"
        assert isinstance(JobStatus.COMPLETED, str)
