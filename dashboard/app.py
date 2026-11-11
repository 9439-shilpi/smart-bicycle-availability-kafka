
import json
import streamlit as st
from kafka import KafkaConsumer

st.set_page_config(
    page_title="StadtRAD Bicycle Monitor",
    page_icon="🚲",
    layout="wide"
)

st.title("🚲 Smart Bicycle Availability Monitoring")
st.subheader("StadtRAD Hamburg – Near-Real-Time Station Monitoring")

KAFKA_SERVER = "localhost:9092"
AVAILABILITY_TOPIC = "stadtrad-availability"
ALERT_TOPIC = "bicycle-shortage-alerts"


def read_messages(topic):
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=None
    )

    consumer.subscribe([topic])
    consumer.poll(timeout_ms=1000)
    consumer.seek_to_beginning()

    results = []

    try:
        while True:
            messages = consumer.poll(
                timeout_ms=1500,
                max_records=100
            )

            if not messages:
                break

            for records in messages.values():
                for message in records:
                    results.append(message.value)

    finally:
        consumer.close()

    return results


def get_observation_time(event):
    return event.get("observation", {}).get("phenomenonTime", "")


# Read Kafka messages
availability_messages = read_messages(AVAILABILITY_TOPIC)
alert_messages = read_messages(ALERT_TOPIC)


# Remove duplicate observations using observation ID
unique_observations = {}

for event in availability_messages:
    observation = event.get("observation", {})
    observation_id = observation.get("@iot.id")

    if observation_id is not None:
        unique_observations[str(observation_id)] = event
    else:
        key = (
            get_observation_time(event),
            observation.get("result")
        )
        unique_observations[str(key)] = event


# Sort observations by timestamp
sorted_observations = sorted(
    unique_observations.values(),
    key=get_observation_time
)

# Get the latest observation
availability = (
    sorted_observations[-1]
    if sorted_observations else None
)

# Get the latest recorded shortage alert
alert = (
    max(
        alert_messages,
        key=lambda item: item.get("observation_time", "")
    )
    if alert_messages else None
)


# Current Bicycle Availability
st.markdown("### Current Bicycle Availability")

if availability:
    observation = availability.get("observation", {})
    bicycle_count = observation.get("result")

    if bicycle_count == 0:
        status = "EMPTY"
    elif bicycle_count is not None and 1 <= bicycle_count <= 5:
        status = "LOW"
    elif bicycle_count is not None:
        status = "NORMAL"
    else:
        status = "UNKNOWN"

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Available Bicycles", bicycle_count)

    with col2:
        st.metric(
            "Station",
            availability.get("station", "Unknown")
        )

    with col3:
        st.metric("Availability Status", status)

    st.caption(
        f"Observation time: {observation.get('phenomenonTime', 'Unknown')}"
    )

else:
    st.info("Waiting for bicycle availability data from Kafka.")


# Recent Bicycle Observations
st.markdown("### Recent Bicycle Observations")

if sorted_observations:
    recent_observations = sorted_observations[-10:]

    history = []

    for event in recent_observations:
        observation = event.get("observation", {})

        history.append({
            "Station": event.get("station"),
            "Bicycles Available": observation.get("result"),
            "Observation Time": observation.get("phenomenonTime")
        })

    st.table(history)

    st.caption(
        f"Showing {len(recent_observations)} unique observations."
    )

else:
    st.info("No bicycle observations available yet.")


# Bicycle Shortage Alerts
st.divider()
st.markdown("### Bicycle Shortage Alerts")

if alert:
    alert_time = alert.get("observation_time", "")
    current_time = get_observation_time(availability) if availability else ""

    if current_time and alert_time < current_time:
        st.info(
            f"Historical shortage alert: {alert.get('status')} "
            f"availability at {alert.get('station')}: "
            f"{alert.get('bicycle_count')} bicycles available."
        )
        st.caption(f"Historical alert time: {alert_time}")

    else:
        st.warning(
            f"{alert.get('status')} availability at "
            f"{alert.get('station')}: "
            f"{alert.get('bicycle_count')} bicycles available."
        )
        st.caption(f"Alert time: {alert_time}")

else:
    st.success("No shortage alerts received yet.")


# Data source
st.caption(
    "Data source: Genuine StadtRAD Hamburg SensorThings REST API observations."
)
