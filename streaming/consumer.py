import json
import time
from collections import Counter
import os
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

topic = os.getenv("KAFKA_TOPIC")
bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=bootstrap,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="ev-charging-consumer",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

BATCH_SECONDS = 60
DURATION_ALERT_THRESHOLD = 120
STATION_OVERLOAD_THRESHOLD = 30

def summarize(batch):
    count = len(batch)
    energies = [x.get("energy_kwh") for x in batch if isinstance(x.get("energy_kwh"), (int, float))]
    durations = [x.get("duration_min") for x in batch if isinstance(x.get("duration_min"), (int, float))]
    stations = [x.get("station_id") for x in batch if x.get("station_id")]

    avg_energy = round(sum(energies) / len(energies), 2) if energies else None
    avg_duration = round(sum(durations) / len(durations), 2) if durations else None
    top_stations = Counter(stations).most_common(3)

    alerts = []
    if avg_duration is not None and avg_duration > DURATION_ALERT_THRESHOLD:
        alerts.append(f"ALERT_DURATION avg_duration_min={avg_duration}")

    for station_id, c in top_stations:
        if c >= STATION_OVERLOAD_THRESHOLD:
            alerts.append(f"ALERT_STATION_LOAD station_id={station_id} count={c}")

    return {
        "window_seconds": BATCH_SECONDS,
        "events_count": count,
        "avg_energy_kwh": avg_energy,
        "avg_duration_min": avg_duration,
        "top_stations": top_stations,
        "alerts": alerts
    }

def main():
    batch = []
    window_start = time.time()

    while True:
        msg = consumer.poll(timeout_ms=1000, max_records=500)
        for _, records in msg.items():
            for record in records:
                batch.append(record.value)

        if time.time() - window_start >= BATCH_SECONDS:
            print(json.dumps(summarize(batch), indent=2))
            batch = []
            window_start = time.time()

if __name__ == "__main__":
    main()
