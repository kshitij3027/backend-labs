"""Demonstrate Protocol Buffer schema evolution with forward/backward compatibility.

This module shows how protobuf's wire format enables seamless schema evolution:
  - Forward compatibility:  v1 data can be read by v2 code (new fields get defaults).
  - Backward compatibility: v2 data can be read by v1 code (unknown fields are preserved).
  - Round-trip preservation: unknown fields survive serialize -> deserialize cycles.
"""

from __future__ import annotations

from datetime import datetime, timezone

from google.protobuf.timestamp_pb2 import Timestamp

from src.generated.log_entry_pb2 import LogEntry
from src.generated.log_entry_v2_pb2 import LogEntryV2


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SEPARATOR = "=" * 70


def _make_timestamp(dt: datetime) -> Timestamp:
    """Create a protobuf Timestamp from a datetime."""
    ts = Timestamp()
    ts.FromDatetime(dt)
    return ts


def _print_header(part: int, title: str) -> None:
    print(f"\n{SEPARATOR}")
    print(f"  Part {part}: {title}")
    print(SEPARATOR)


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------


def run_schema_evolution_demo() -> None:
    """Run the full schema evolution demonstration."""

    print(SEPARATOR)
    print("  Protocol Buffer Schema Evolution Demo")
    print("  Demonstrating forward & backward compatibility")
    print(SEPARATOR)

    # Common timestamp for all examples
    now = datetime(2025, 6, 15, 10, 30, 0, tzinfo=timezone.utc)
    ts = _make_timestamp(now)

    # ------------------------------------------------------------------
    # Part 1: Forward Compatibility — v1 data read by v2 code
    # ------------------------------------------------------------------
    _print_header(1, "Forward Compatibility (v1 data -> v2 reader)")

    print("\nScenario: A producer is still running the OLD schema (v1),")
    print("but the consumer has been upgraded to the NEW schema (v2).\n")

    # Create and serialize a v1 message
    v1_entry = LogEntry()
    v1_entry.timestamp.CopyFrom(ts)
    v1_entry.service_name = "auth-service"
    v1_entry.level = 3  # ERROR
    v1_entry.message = "Login failed for user admin"
    v1_entry.metadata["request_id"] = "req-abc-123"
    v1_entry.metadata["ip"] = "192.168.1.50"

    v1_bytes = v1_entry.SerializeToString()
    print(f"  [v1] Serialized LogEntry  ->  {len(v1_bytes)} bytes")
    print(f"  [v1] service_name = {v1_entry.service_name!r}")
    print(f"  [v1] level        = {v1_entry.level} (ERROR)")
    print(f"  [v1] message      = {v1_entry.message!r}")
    print(f"  [v1] metadata     = {dict(v1_entry.metadata)}")

    # Deserialize those v1 bytes as a v2 message
    v2_from_v1 = LogEntryV2()
    v2_from_v1.ParseFromString(v1_bytes)

    print(f"\n  [v2] Deserialized as LogEntryV2:")
    print(f"  [v2] service_name  = {v2_from_v1.service_name!r}")
    print(f"  [v2] level         = {v2_from_v1.level} (ERROR)")
    print(f"  [v2] message       = {v2_from_v1.message!r}")
    print(f"  [v2] metadata      = {dict(v2_from_v1.metadata)}")
    print(f"  [v2] trace_id      = {v2_from_v1.trace_id!r}  (default — field not in v1)")
    print(f"  [v2] response_code = {v2_from_v1.response_code}  (default — field not in v1)")
    print(f"  [v2] duration_ms   = {v2_from_v1.duration_ms}  (default — field not in v1)")

    print("\n  RESULT: Forward compatibility works!")
    print("  Old (v1) data is safely readable by new (v2) code.")
    print("  New fields simply take their default values (empty string, 0, 0.0).")

    # ------------------------------------------------------------------
    # Part 2: Backward Compatibility — v2 data read by v1 code
    # ------------------------------------------------------------------
    _print_header(2, "Backward Compatibility (v2 data -> v1 reader)")

    print("\nScenario: A producer has been upgraded to the NEW schema (v2),")
    print("but the consumer is still running the OLD schema (v1).\n")

    # Create and serialize a v2 message with all fields populated
    v2_entry = LogEntryV2()
    v2_entry.timestamp.CopyFrom(ts)
    v2_entry.service_name = "payment-service"
    v2_entry.level = 1  # INFO
    v2_entry.message = "Payment processed successfully"
    v2_entry.metadata["order_id"] = "order-789"
    v2_entry.metadata["amount"] = "49.99"
    v2_entry.trace_id = "trace-xyz-456-abc"
    v2_entry.response_code = 200
    v2_entry.duration_ms = 23.45

    v2_bytes = v2_entry.SerializeToString()
    print(f"  [v2] Serialized LogEntryV2  ->  {len(v2_bytes)} bytes")
    print(f"  [v2] service_name  = {v2_entry.service_name!r}")
    print(f"  [v2] level         = {v2_entry.level} (INFO)")
    print(f"  [v2] message       = {v2_entry.message!r}")
    print(f"  [v2] metadata      = {dict(v2_entry.metadata)}")
    print(f"  [v2] trace_id      = {v2_entry.trace_id!r}")
    print(f"  [v2] response_code = {v2_entry.response_code}")
    print(f"  [v2] duration_ms   = {v2_entry.duration_ms}")

    # Deserialize those v2 bytes as a v1 message
    v1_from_v2 = LogEntry()
    v1_from_v2.ParseFromString(v2_bytes)

    print(f"\n  [v1] Deserialized as LogEntry:")
    print(f"  [v1] service_name = {v1_from_v2.service_name!r}")
    print(f"  [v1] level        = {v1_from_v2.level} (INFO)")
    print(f"  [v1] message      = {v1_from_v2.message!r}")
    print(f"  [v1] metadata     = {dict(v1_from_v2.metadata)}")

    # Unknown fields are preserved internally by protobuf even though
    # the Python API doesn't expose them directly in proto3 with the
    # upb/C runtime.  We verify their presence in Part 3 by doing
    # a round-trip and checking that the extra data survives.
    v1_reserialized_size = len(v1_from_v2.SerializeToString())
    print(f"  [v1] re-serialized size = {v1_reserialized_size} bytes "
          f"(v2 original was {len(v2_bytes)} bytes)")
    print(f"  [v1] Size preserved = {v1_reserialized_size == len(v2_bytes)} "
          "(unknown fields kept in wire format)")

    print("\n  RESULT: Backward compatibility works!")
    print("  New (v2) data is safely readable by old (v1) code.")
    print("  The v1 reader sees all the fields it knows about.")
    print("  Extra fields (trace_id, response_code, duration_ms) are")
    print("  silently preserved — we can verify this via re-serialization size")
    print("  and by round-tripping back to v2 (see Part 3).")

    # ------------------------------------------------------------------
    # Part 3: Round-trip Preservation
    # ------------------------------------------------------------------
    _print_header(3, "Round-trip Preservation")

    print("\nScenario: v2 data passes through a v1 intermediary and back to v2.")
    print("Do the extra fields survive the journey?\n")

    # Step 1: Start with v2 data
    print("  Step 1: Original v2 message")
    print(f"    trace_id      = {v2_entry.trace_id!r}")
    print(f"    response_code = {v2_entry.response_code}")
    print(f"    duration_ms   = {v2_entry.duration_ms}")
    original_v2_bytes = v2_entry.SerializeToString()
    print(f"    serialized    -> {len(original_v2_bytes)} bytes")

    # Step 2: Deserialize as v1 (intermediary)
    intermediary_v1 = LogEntry()
    intermediary_v1.ParseFromString(original_v2_bytes)
    print(f"\n  Step 2: Deserialized as v1 (intermediary)")
    print(f"    service_name = {intermediary_v1.service_name!r}")
    print(f"    message      = {intermediary_v1.message!r}")

    # Step 3: Re-serialize from v1
    round_trip_bytes = intermediary_v1.SerializeToString()
    print(f"\n  Step 3: Re-serialized from v1  ->  {len(round_trip_bytes)} bytes")

    # Step 4: Deserialize back as v2
    restored_v2 = LogEntryV2()
    restored_v2.ParseFromString(round_trip_bytes)
    print(f"\n  Step 4: Deserialized back as v2")
    print(f"    service_name  = {restored_v2.service_name!r}")
    print(f"    level         = {restored_v2.level} (INFO)")
    print(f"    message       = {restored_v2.message!r}")
    print(f"    metadata      = {dict(restored_v2.metadata)}")
    print(f"    trace_id      = {restored_v2.trace_id!r}")
    print(f"    response_code = {restored_v2.response_code}")
    print(f"    duration_ms   = {restored_v2.duration_ms}")

    # Verify round-trip fidelity
    trace_ok = restored_v2.trace_id == v2_entry.trace_id
    code_ok = restored_v2.response_code == v2_entry.response_code
    duration_ok = restored_v2.duration_ms == v2_entry.duration_ms
    all_ok = trace_ok and code_ok and duration_ok

    print(f"\n  Verification:")
    print(f"    trace_id preserved      = {trace_ok}")
    print(f"    response_code preserved = {code_ok}")
    print(f"    duration_ms preserved   = {duration_ok}")

    if all_ok:
        print("\n  RESULT: Round-trip preservation works!")
        print("  v2 data survived a full round-trip through a v1 intermediary.")
        print("  Protobuf preserves unknown fields during serialization,")
        print("  so no data is lost even when an intermediate service doesn't")
        print("  know about the newer fields.")
    else:
        print("\n  RESULT: Some fields were lost during the round-trip.")
        print("  Note: In proto3 with certain Python runtime versions,")
        print("  unknown field preservation may not be supported.")
        print("  This is a known limitation of the pure-Python runtime.")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print(f"\n{SEPARATOR}")
    print("  Summary: Why Schema Evolution Matters")
    print(SEPARATOR)
    print("""
  Protocol Buffers use field NUMBERS (not names) in the wire format.
  This means:

  1. FORWARD COMPATIBILITY — Old data, new code:
     You can add new fields to a .proto file, and old serialized data
     will still parse correctly. New fields get their default values.

  2. BACKWARD COMPATIBILITY — New data, old code:
     Old code can read data that contains new fields it doesn't know
     about. The unknown fields are preserved in memory (not discarded).

  3. ROUND-TRIP SAFETY:
     When a message passes through an intermediary that doesn't know
     about new fields, those fields are preserved and forwarded intact.

  Rules for safe schema evolution:
    - NEVER reuse a field number for a different field
    - NEVER change the type of an existing field
    - Use 'reserved' to retire old field numbers
    - New fields must use new, unused field numbers
    - Give new fields appropriate default values
""")


if __name__ == "__main__":
    run_schema_evolution_demo()
