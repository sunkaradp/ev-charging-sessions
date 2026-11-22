"""Generate EV charging events and publish them reliably to Kafka."""
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path


CITIES = ["Hamburg", "Berlin", "Munich", "Frankfurt"]
VEHICLE_TYPES = ["sedan", "suv", "hatchback"]
CHARGER_TYPES = ["slow", "fast"]


def load_config(environ=None):
    """Read and validate required producer settings."""
    env = os.environ if environ is None else environ
    required = ("KAFKA_BOOTSTRAP_SERVERS", "KAFKA_TOPIC", "RAW_OUTPUT_PATH")
    missing = [key for key in required if not env.get(key, "").strip()]
    if missing:
        raise ValueError("Missing required environment variables: " + ", ".join(missing))
    return {
        "bootstrap_servers": env["KAFKA_BOOTSTRAP_SERVERS"].strip(),
        "topic": env["KAFKA_TOPIC"].strip(),
        "output_path": env["RAW_OUTPUT_PATH"].strip(),
    }


def generate_event(index):
    """Create one synthetic session event with a unique session identifier."""
    event = {
        "event_time": datetime.now(timezone.utc).isoformat(),
        "session_id": f"sess_{uuid.uuid4().hex}",
        "station_id": f"station_{random.randint(1, 20)}",
        "vehicle_type": random.choice(VEHICLE_TYPES),
        "city": random.choice(CITIES),
        "charger_type": random.choice(CHARGER_TYPES),
        "energy_kwh": round(random.uniform(5, 60), 2),
        "duration_min": random.randint(5, 90),
        "price_eur": round(random.uniform(3, 30), 2),
        "event_type": random.choice(["start", "update", "end"]),
    }
    if index and index % 50 == 0:
        event["energy_kwh"] = None
    if index and index % 120 == 0:
        event["duration_min"] = 999
    return event


def main():
    """Publish events and persist only messages acknowledged by Kafka."""
    from kafka import KafkaProducer

    config = load_config()
    producer = KafkaProducer(
        bootstrap_servers=config["bootstrap_servers"],
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
        enable_idempotence=True,
    )
    output_path = Path(config["output_path"])
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with output_path.open("a", encoding="utf-8") as output:
            index = 0
            while True:
                event = generate_event(index)
                # Wait for broker acknowledgement before recording the event locally.
                producer.send(config["topic"], event).get(timeout=30)
                output.write(json.dumps(event) + "\\n")
                output.flush()
                print("Sent:", event)
                index += 1
                time.sleep(3)
    except KeyboardInterrupt:
        print("Producer stopped.")
    finally:
        producer.flush(timeout=30)
        producer.close(timeout=30)


if __name__ == "__main__":
    main()
