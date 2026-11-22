from datetime import datetime
from data_gen.producer import generate_event, load_config


def test_load_config_trims_values():
    config = load_config({
        "KAFKA_BOOTSTRAP_SERVERS": " localhost:9092 ",
        "KAFKA_TOPIC": " ev-sessions ",
        "RAW_OUTPUT_PATH": " data/events.jsonl ",
    })
    assert config == {
        "bootstrap_servers": "localhost:9092",
        "topic": "ev-sessions",
        "output_path": "data/events.jsonl",
    }


def test_load_config_reports_all_missing_values():
    try:
        load_config({})
    except ValueError as error:
        message = str(error)
    else:
        raise AssertionError("Expected missing configuration to fail")
    assert "KAFKA_BOOTSTRAP_SERVERS" in message
    assert "KAFKA_TOPIC" in message
    assert "RAW_OUTPUT_PATH" in message


def test_generated_event_has_unique_id_and_utc_timestamp():
    first = generate_event(1)
    second = generate_event(2)
    assert first["session_id"] != second["session_id"]
    assert datetime.fromisoformat(first["event_time"]).utcoffset().total_seconds() == 0
    assert first["city"] in {"Hamburg", "Berlin", "Munich", "Frankfurt"}


def test_synthetic_edge_cases_are_deterministic():
    missing_energy_event = generate_event(150)
    long_duration_event = generate_event(120)
    assert missing_energy_event["energy_kwh"] is None
    assert long_duration_event["duration_min"] == 999
