import re
import sqlalchemy as sa
from src.persistence.models import AuditRecord, Base, IMMUTABILITY_TRIGGERS_SQL


def test_audit_record_tablename():
    assert AuditRecord.__tablename__ == "audit_records"


def test_audit_record_has_all_expected_columns():
    expected = {
        "seq",
        "timestamp_utc",
        "actor",
        "action",
        "resource",
        "success",
        "error_message",
        "processing_ms",
        "args_digest",
        "result_digest",
        "prev_hash",
        "self_hash",
        "signature",
    }
    actual = set(AuditRecord.__table__.columns.keys())
    assert actual == expected, f"missing={expected - actual} extra={actual - expected}"


def test_seq_is_primary_key_without_autoincrement():
    col = AuditRecord.__table__.columns["seq"]
    assert col.primary_key is True
    # autoincrement=False means we set the seq ourselves (genesis is 0).
    assert col.autoincrement is False


def test_self_hash_is_unique():
    col = AuditRecord.__table__.columns["self_hash"]
    assert col.unique is True


def test_indexes_declared():
    # Single-column indexes (from index=True on the column)
    # plus the three composite indexes from __table_args__.
    index_names = {ix.name for ix in AuditRecord.__table__.indexes}
    assert "ix_audit_records_actor_ts" in index_names
    assert "ix_audit_records_action_ts" in index_names
    assert "ix_audit_records_resource_ts" in index_names


def test_composite_index_columns():
    by_name = {ix.name: ix for ix in AuditRecord.__table__.indexes}
    for ix_name, expected_cols in [
        ("ix_audit_records_actor_ts", ["actor", "timestamp_utc"]),
        ("ix_audit_records_action_ts", ["action", "timestamp_utc"]),
        ("ix_audit_records_resource_ts", ["resource", "timestamp_utc"]),
    ]:
        cols = [c.name for c in by_name[ix_name].columns]
        assert cols == expected_cols, f"{ix_name}: {cols} != {expected_cols}"


def test_nullable_columns():
    cols = AuditRecord.__table__.columns
    # These two are explicitly nullable.
    assert cols["error_message"].nullable is True
    assert cols["processing_ms"].nullable is True
    # Everything else must be NOT NULL.
    not_null_required = {
        "seq",
        "timestamp_utc",
        "actor",
        "action",
        "resource",
        "success",
        "args_digest",
        "result_digest",
        "prev_hash",
        "self_hash",
        "signature",
    }
    for name in not_null_required:
        assert cols[name].nullable is False, f"{name} should be NOT NULL"


def test_immutability_triggers_is_tuple_of_two():
    assert isinstance(IMMUTABILITY_TRIGGERS_SQL, tuple)
    assert len(IMMUTABILITY_TRIGGERS_SQL) == 2


def test_immutability_triggers_target_correct_table():
    for sql in IMMUTABILITY_TRIGGERS_SQL:
        # Each trigger must reference audit_records, RAISE(ABORT, ...),
        # and IF NOT EXISTS (so init_db can reapply on every boot).
        assert "audit_records" in sql.lower()
        assert "raise(abort" in sql.lower()
        assert "if not exists" in sql.lower()


def test_one_trigger_per_op():
    # Confirm coverage: exactly one UPDATE trigger and one DELETE trigger.
    has_update = any("update on audit_records" in s.lower() for s in IMMUTABILITY_TRIGGERS_SQL)
    has_delete = any("delete on audit_records" in s.lower() for s in IMMUTABILITY_TRIGGERS_SQL)
    assert has_update and has_delete


def test_triggers_parse_as_sqlite_sql():
    # Smoke check: each trigger compiles when we wrap it in a SQLite in-memory engine.
    # We create the table first (so the trigger has a target), then issue the
    # CREATE TRIGGER. If SQLite is unhappy, this raises.
    engine = sa.create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with engine.begin() as conn:
        for sql in IMMUTABILITY_TRIGGERS_SQL:
            conn.exec_driver_sql(sql)
    engine.dispose()
