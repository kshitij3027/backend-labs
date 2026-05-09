import json

from src.logging_setup import (
    TAG_ADMIT,
    TAG_CONFIG_UPDATE,
    TAG_DROP,
    TAG_PRESSURE,
    TAG_STATE,
    TAG_THROTTLE,
    configure_logging,
    get_logger,
)


def test_configure_logging_emits_json_to_stdout(capsys):
    configure_logging("INFO")
    logger = get_logger("test")
    logger.info(
        "hello",
        tag=TAG_STATE,
        from_state="normal",
        to_state="pressure",
    )

    captured = capsys.readouterr()
    lines = [line for line in captured.out.splitlines() if line.strip()]
    assert lines, "expected at least one log line on stdout"

    parsed = None
    for line in lines:
        try:
            candidate = json.loads(line)
        except json.JSONDecodeError:
            continue
        if candidate.get("event") == "hello":
            parsed = candidate
            break

    assert parsed is not None, f"no JSON log line with event=hello found in: {lines}"
    assert parsed["event"] == "hello"
    assert parsed["tag"] == "STATE"
    assert parsed["from_state"] == "normal"
    assert parsed["to_state"] == "pressure"
    assert parsed["level"] == "info"


def test_tag_constants_present():
    assert TAG_PRESSURE == "PRESSURE"
    assert TAG_THROTTLE == "THROTTLE"
    assert TAG_DROP == "DROP"
    assert TAG_ADMIT == "ADMIT"
    assert TAG_STATE == "STATE"
    assert TAG_CONFIG_UPDATE == "CONFIG_UPDATE"
