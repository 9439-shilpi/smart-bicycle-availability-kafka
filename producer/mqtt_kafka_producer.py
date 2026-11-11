
import json
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from kafka import KafkaProducer

# StadtRAD Hamburg MQTT configuration
MQTT_BROKER = "iot.hamburg.de"
MQTT_PORT = 1883
MQTT_TOPIC = "v1.0/Datastreams(25813)/Observations"

# Kafka configuration
KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "stadtrad-availability"

# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print("Connected to StadtRAD MQTT broker")
        result, mid = client.subscribe(MQTT_TOPIC)

        if result == mqtt.MQTT_ERR_SUCCESS:
            print(f"Subscription requested: {MQTT_TOPIC}")
        else:
            print(f"Subscription failed: {result}")
    else:
        print(f"MQTT connection failed: {reason_code}")


def on_subscribe(client, userdata, mid, reason_code_list, properties):
    print(f"Subscription response: {reason_code_list}")


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
    print(f"MQTT disconnected. Reason: {reason_code}")


def on_message(client, userdata, message):
    try:
        observation = json.loads(message.payload.decode("utf-8"))

        event = {
            "source": "StadtRAD Hamburg",
            "station": "S Reeperbahn / Königstraße",
            "datastream_id": 25813,
            "received_at": datetime.now(timezone.utc).isoformat(),
            "observation": observation
        }

        producer.send(KAFKA_TOPIC, value=event).get(timeout=10)

        print("Observation sent to Kafka:")
        print(json.dumps(event, indent=2))

    except Exception as error:
        print(f"Error processing observation: {error}")


client = mqtt.Client(
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    client_id="stadtrad-kafka-producer"
)

client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_subscribe = on_subscribe
client.on_message = on_message

# Automatic reconnection delay
client.reconnect_delay_set(min_delay=2, max_delay=30)

try:
    print("Starting StadtRAD MQTT-to-Kafka producer...")
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_forever()

except KeyboardInterrupt:
    print("\nProducer stopped by user.")

finally:
    client.disconnect()
    producer.flush()
    producer.close()
