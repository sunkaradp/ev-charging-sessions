import json
from datetime import datetime, timezone
from google.cloud import pubsub_v1

PROJECT_ID = "ev-charging-platform-487012"
TOPIC_ID = "ev-charging-sessions"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

event = {
    "event_time": datetime.now(timezone.utc).isoformat(),
    "session_id": "sess_test_001",
    "station_id": "station_1",
    "city": "Hamburg",
    "charger_type": "fast",
    "energy_kwh": 22.5,
    "duration_min": 35,
    "price_eur": 9.99,
    "event_type": "start"
}

data = json.dumps(event).encode("utf-8")
future = publisher.publish(topic_path, data=data)
print("Published message_id:", future.result())
print("Payload:", event)
