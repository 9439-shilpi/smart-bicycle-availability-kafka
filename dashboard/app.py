
import json
import streamlit as st
from kafka import KafkaConsumer

# Page configuration
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


# Read all stored messages from a Kafka topic
def read_messages(topic):
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=None
    )

    consumer.subscribe([topic])

    # Wait for Kafka to assign the topic partition
    consumer.poll(timeout_ms=1000)

    # Start reading from the beginning
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


# Read availability and alert messages
availability_messages = read_messages(AVAILABILITY_TOPIC)
alert_messages = read_messages(ALERT_TOPIC)

# Get the latest availability and alert
availability = (
    availability_messages[-1]
    if availability_messages else None
)

alert = alert_messages[-1] if alert_messages else None


# Current Bicycle Availability
if availability:
    observation = availability.get("observation", {})
    bicycle_count = observation.get("result")

    st.markdown("### Current Bicycle Availability")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Available Bicycles", bicycle_count)

    with col2:
        st.metric(
            "Station",
            availability.get("station", "Unknown")
        )

    with col3:
        if bicycle_count == 0:
            status = "EMPTY"
        elif bicycle_count is not None and bicycle_count <= 5:
            status = "LOW"
        else:
            status = "NORMAL"

        st.metric("Availability Status", status)

    st.caption(
        f"Observation time: "
        f"{observation.get('phenomenonTime', 'Unknown')}"
    )

else:
    st.info("Waiting for bicycle availability data from Kafka.")


# Recent Bicycle Observations
st.markdown("### Recent Bicycle Observations")

if availability_messages:
    history = []

    for event in availability_messages:
        observation = event.get("observation", {})

        history.append({
            "Station": event.get("station"),
            "Bicycles Available": observation.get("result"),
            "Observation Time": observation.get("phenomenonTime")
        })

    st.table(history)

else:
    st.info("No bicycle observations available yet.")


# Bicycle Shortage Alerts
st.divider()

st.markdown("### Bicycle Shortage Alerts")

if alert:
    st.warning(
        f"{alert.get('status')} availability at "
        f"{alert.get('station')}: "
        f"{alert.get('bicycle_count')} bicycles available."
    )

    st.caption(
        f"Observation time: "
        f"{alert.get('observation_time', 'Unknown')}"
    )

else:
    st.success("No shortage alerts received yet.")


# Data source
st.caption(
    "Data source: Genuine StadtRAD Hamburg SensorThings REST API observations."
)
