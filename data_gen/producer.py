import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv
import os

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
RAW_OUTPUT_PATH = os.getenv("RAW_OUTPUT_PATH")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

cities = ["Hamburg", "Berlin", "Munich", "Frankfurt"]
vehicle_types = ["sedan", "suv", "hatchback"]
charger_types = ["slow", "fast"]

def generate_event(i):
    event = {
        "event_time": datetime.utcnow().isoformat(),
        "session_id": f"sess_{random.randint(1000, 9999)}",
        "station_id": f"station_{random.randint(1, 20)}",
        "vehicle_type": random.choice(vehicle_types),
        "city": random.choice(cities),
        "charger_type": random.choice(charger_types),
        "energy_kwh": round(random.uniform(5, 60), 2),
        "duration_min": random.randint(5, 90),
        "price_eur": round(random.uniform(3, 30), 2),
        "event_type": random.choice(["start", "update", "end"])
    }

    if i % 50 == 0:
        event["energy_kwh"] = None

    if i % 120 == 0:
        event["duration_min"] = 999

    return event

def main():
    os.makedirs(os.path.dirname(RAW_OUTPUT_PATH), exist_ok=True)

    with open(RAW_OUTPUT_PATH, "a") as f:
        i = 0
        while True:
            event = generate_event(i)

            producer.send(KAFKA_TOPIC, event)

            f.write(json.dumps(event) + "\n")
            f.flush()

            print("Sent:", event)

            i += 1
            time.sleep(3)

if __name__ == "__main__":
    main()
