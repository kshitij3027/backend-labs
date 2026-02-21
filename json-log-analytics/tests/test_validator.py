class TestLogValidator:
    def test_valid_log(self, validator, sample_valid_log):
        """Validate a correct log, assert is_valid=True, no errors."""
        is_valid, errors = validator.validate(sample_valid_log)
        assert is_valid is True
        assert errors == []

    def test_missing_required_fields(self, validator):
        """Missing timestamp and level, assert is_valid=False, errors mention missing fields."""
        log = {"message": "hello", "service": "test-svc"}
        is_valid, errors = validator.validate(log)
        assert is_valid is False
        assert len(errors) > 0
        error_text = " ".join(errors).lower()
        assert "timestamp" in error_text or "level" in error_text

    def test_invalid_level(self, validator):
        """Level='INVALID', assert is_valid=False."""
        log = {
            "timestamp": "2024-01-15T10:30:00Z",
            "level": "INVALID",
            "service": "test-svc",
            "message": "test message",
        }
        is_valid, errors = validator.validate(log)
        assert is_valid is False
        assert len(errors) > 0

    def test_additional_properties_rejected(self, validator):
        """Add extra_field, assert is_valid=False."""
        log = {
            "timestamp": "2024-01-15T10:30:00Z",
            "level": "INFO",
            "service": "test-svc",
            "message": "test message",
            "extra_field": "should be rejected",
        }
        is_valid, errors = validator.validate(log)
        assert is_valid is False
        assert len(errors) > 0

    def test_valid_log_with_metadata(self, validator, sample_log_with_metadata):
        """Log with user_id + metadata, assert is_valid=True."""
        is_valid, errors = validator.validate(sample_log_with_metadata)
        assert is_valid is True
        assert errors == []

    def test_invalid_metadata_processing_time(self, validator):
        """Negative processing_time_ms, assert is_valid=False."""
        log = {
            "timestamp": "2024-01-15T10:30:00Z",
            "level": "ERROR",
            "service": "api-gateway",
            "message": "Request failed",
            "metadata": {
                "processing_time_ms": -10,
            },
        }
        is_valid, errors = validator.validate(log)
        assert is_valid is False
        assert len(errors) > 0

    def test_stats_tracking(self, validator, sample_valid_log):
        """Validate a few logs, verify stats counts."""
        validator.validate(sample_valid_log)
        validator.validate(sample_valid_log)
        validator.validate({"bad": "log"})

        stats = validator.get_stats()
        assert stats["total"] == 3
        assert stats["valid"] == 2
        assert stats["invalid"] == 1
        assert isinstance(stats["error_types"], dict)

    def test_reset_stats(self, validator, sample_valid_log):
        """Validate, reset, verify zeroed."""
        validator.validate(sample_valid_log)
        validator.validate({"bad": "log"})

        stats = validator.get_stats()
        assert stats["total"] == 2

        validator.reset_stats()
        stats = validator.get_stats()
        assert stats["total"] == 0
        assert stats["valid"] == 0
        assert stats["invalid"] == 0
        assert stats["error_types"] == {}
