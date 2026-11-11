
import json
import os
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer

# Kafka configuration
KAFKA_SERVER = "localhost:9092"
INPUT_TOPIC = "stadtrad-availability"
ALERT_TOPIC = "bicycle-shortage-alerts"

# Persistent duplicate protection
CHECKPOINT_FILE = "processed_ids.txt"

if os.path.exists(CHECKPOINT_FILE):
    with open(CHECKPOINT_FILE, "r") as file:
        processed_ids = set(line.strip() for line in file if line.strip())
else:
    processed_ids = set()

consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    auto_offset_reset="latest",
    enable_auto_commit=False,
    group_id="bicycle-shortage-idempotency"
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

print("Bicycle shortage detector is running...")
print("Waiting for new StadtRAD observations...\n")

try:
    for message in consumer:
        event = message.value
        observation = event.get("observation", {})
        observation_id = str(observation.get("@iot.id"))

        # Skip observations already processed
        if observation_id in processed_ids:
            print(f"Duplicate skipped: {observation_id}")
            consumer.commit()
            continue

        bicycle_count = observation.get("result")

        if bicycle_count is None:
            print("Skipping observation: bicycle count is missing.")
            consumer.commit()
            continue

        processed_at = datetime.now(timezone.utc)

        received_at = datetime.fromisoformat(
            event["received_at"].replace("Z", "+00:00")
        )

        processing_latency = (
            processed_at - received_at
        ).total_seconds()

        if bicycle_count == 0:
            status = "EMPTY"
        elif 1 <= bicycle_count <= 5:
            status = "LOW"
        else:
            status = "NORMAL"

        print(
            f"Station: {event.get('station')} | "
            f"Bicycles: {bicycle_count} | Status: {status}"
        )

        print(f"Processing latency: {processing_latency:.3f} seconds")

        # Send shortage alert when needed
        if status in ["LOW", "EMPTY"]:
            alert = {
                "source": event.get("source"),
                "station": event.get("station"),
                "bicycle_count": bicycle_count,
                "status": status,
                "observation_time": observation.get("phenomenonTime"),
                "observation_id": observation_id
            }

            producer.send(ALERT_TOPIC, value=alert).get(timeout=10)
            print("SHORTAGE ALERT SENT TO KAFKA")

        # Save ID after successful processing
        with open(CHECKPOINT_FILE, "a") as file:
            file.write(observation_id + "\n")

        processed_ids.add(observation_id)

        # Commit Kafka offset after processing
        consumer.commit()
        print()

except KeyboardInterrupt:
    print("\nShortage detector stopped.")

finally:
    consumer.close()
    producer.flush()
    producer.close()
