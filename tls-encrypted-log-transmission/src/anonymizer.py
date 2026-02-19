"""Patient ID anonymization for HIPAA-compliant healthcare logging."""

import hashlib
import datetime


def anonymize_patient_id(patient_id: str) -> str:
    """Hash a patient ID using SHA-256, truncated to 12 hex chars."""
    return hashlib.sha256(patient_id.encode("utf-8")).hexdigest()[:12]


def create_anonymized_healthcare_entry(
    patient_id: str, event_type: str, department: str, message: str
) -> dict:
    """Create a healthcare log entry with anonymized patient ID."""
    return {
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "level": "INFO",
        "message": message,
        "patient_id_hash": anonymize_patient_id(patient_id),
        "event_type": event_type,
        "department": department,
        "hipaa_compliant": True,
        "data_classification": "PHI",
    }
