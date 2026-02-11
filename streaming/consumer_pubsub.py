from google.cloud import pubsub_v1
import json

PROJECT_ID = "ev-charging-platform-487012"
SUBSCRIPTION_ID = "ev-charging-sub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

def callback(message):
    data = json.loads(message.data.decode("utf-8"))
    print("\n✅ Received:", data)
    message.ack()

print("🚀 Listening on:", subscription_path)
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
