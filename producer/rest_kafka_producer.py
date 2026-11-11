
import json
import os
import time
import requests
from datetime import datetime, timezone
from kafka import KafkaProducer

API_URL = "https://iot.hamburg.de/v1.0/Datastreams(25813)/Observations"
KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "stadtrad-availability"
POLL_INTERVAL_SECONDS = 5

BUFFER_FILE = "reliability_buffer.json"
STATE_FILE = "producer_state.json"

# Load saved state after a restart
if os.path.exists(STATE_FILE):
    with open(STATE_FILE, "r") as file:
        state = json.load(file)
else:
    state = {"last_time": "2026-11-10T17:47:01.786607Z"}

if os.path.exists(BUFFER_FILE):
    with open(BUFFER_FILE, "r") as file:
        buffer = json.load(file)
else:
    buffer = []

known_ids = {
    str(item["observation"]["@iot.id"]) for item in buffer
}

def save_json(filename, data):
    temporary_file = filename + ".tmp"
    with open(temporary_file, "w") as file:
        json.dump(data, file, indent=2)
    os.replace(temporary_file, filename)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

print("StadtRAD REST-to-Kafka producer started.")
print("Local reliability buffer is enabled.")

try:
    while True:
        # 1. Collect every observation after the saved timestamp
        try:
            url = API_URL
            params = {
                "$filter": f"phenomenonTime gt {state['last_time']}",
                "$orderby": "phenomenonTime asc",
                "$top": 100
            }

            while url:
                response = requests.get(
                    url,
                    params=params if url == API_URL else None,
                    timeout=15
                )
                response.raise_for_status()
                data = response.json()

                for observation in data.get("value", []):
                    observation_id = str(observation["@iot.id"])

                    if observation_id not in known_ids:
                        event = {
                            "source": "StadtRAD Hamburg",
                            "station": "S Reeperbahn / Königstraße",
                            "datastream_id": 25813,
                            "received_at": datetime.now(
                                timezone.utc
                            ).isoformat(),
                            "observation": observation
                        }

                        buffer.append(event)
                        known_ids.add(observation_id)

                        # Save observation before moving the timestamp
                        save_json(BUFFER_FILE, buffer)

                        state["last_time"] = observation["phenomenonTime"]
                        save_json(STATE_FILE, state)

                        print("Saved observation in local buffer:",
                              observation_id)

                url = data.get("@iot.nextLink")

        except Exception as error:
            print("Data collection error:", error)

        # 2. Send saved observations to Kafka
        while buffer:
            event = buffer[0]

            try:
                producer.send(
                    KAFKA_TOPIC,
                    value=event
                ).get(timeout=10)

                print("Sent to Kafka:",
                      event["observation"]["@iot.id"])

                buffer.pop(0)
                save_json(BUFFER_FILE, buffer)

            except Exception as error:
                print("Kafka unavailable. Keeping data in buffer:", error)
                break

        time.sleep(POLL_INTERVAL_SECONDS)

except KeyboardInterrupt:
    print("\nProducer stopped.")

finally:
    producer.flush()
    producer.close()
