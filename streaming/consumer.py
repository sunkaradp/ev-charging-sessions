import json
import time
from collections import Counter, deque
from statistics import mean
from kafka import KafkaConsumer

TOPIC = "ev-charging-sessions"
BOOTSTRAP_SERVERS = "localhost:9092"
GROUP_ID = "ev-charging-consumer"
WINDOW_SECONDS = 60
PRINT_EVERY_SECONDS = 5

def safe_float(x):
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None

def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    window = deque()
    last_print = time.time()

    print("consumer started")

    for msg in consumer:
        event = msg.value
        now = time.time()

        window.append((now, event))

        cutoff = now - WINDOW_SECONDS
        while window and window[0][0] < cutoff:
            window.popleft()

        if now - last_print >= PRINT_EVERY_SECONDS:
            last_print = now

            events = [e for _, e in window]

            energy_vals = [safe_float(e.get("energy_kwh")) for e in events]
            energy_vals = [x for x in energy_vals if x is not None]

            duration_vals = [safe_float(e.get("duration_min")) for e in events]
            duration_vals = [x for x in duration_vals if x is not None]

            station_counts = Counter(
                [e.get("station_id") for e in events if e.get("station_id")]
            )

            alerts = []
            for e in events[-10:]:
                dur = safe_float(e.get("duration_min"))
                price = safe_float(e.get("price_eur"))
                if dur is not None and dur > 180:
                    alerts.append({"type": "long_session", "session_id": e.get("session_id")})
                if price is not None and price > 50:
                    alerts.append({"type": "high_price", "session_id": e.get("session_id")})

            output = {
                "window_seconds": WINDOW_SECONDS,
                "events_count": len(events),
                "avg_energy_kwh": round(mean(energy_vals), 2) if energy_vals else None,
                "avg_duration_min": round(mean(duration_vals), 2) if duration_vals else None,
                "top_stations": station_counts.most_common(5),
                "alerts": alerts,
            }

            print(json.dumps(output, indent=2))

if __name__ == "__main__":
    main()
