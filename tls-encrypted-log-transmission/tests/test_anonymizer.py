"""Tests for patient anonymization."""

from src.anonymizer import anonymize_patient_id, create_anonymized_healthcare_entry


class TestAnonymizePatientId:
    def test_returns_12_hex_chars(self):
        result = anonymize_patient_id("P12345")
        assert len(result) == 12
        assert all(c in "0123456789abcdef" for c in result)

    def test_deterministic(self):
        a = anonymize_patient_id("P12345")
        b = anonymize_patient_id("P12345")
        assert a == b

    def test_different_ids_different_hashes(self):
        a = anonymize_patient_id("P12345")
        b = anonymize_patient_id("P67890")
        assert a != b


class TestCreateAnonymizedEntry:
    def test_has_required_fields(self):
        entry = create_anonymized_healthcare_entry(
            "P12345", "admission", "ER", "Patient admitted"
        )
        assert "timestamp" in entry
        assert "patient_id_hash" in entry
        assert entry["hipaa_compliant"] is True
        assert entry["data_classification"] == "PHI"
        assert entry["event_type"] == "admission"
        assert entry["department"] == "ER"

    def test_patient_id_is_hashed(self):
        entry = create_anonymized_healthcare_entry(
            "P12345", "admission", "ER", "Patient admitted"
        )
        assert entry["patient_id_hash"] != "P12345"
        assert len(entry["patient_id_hash"]) == 12
