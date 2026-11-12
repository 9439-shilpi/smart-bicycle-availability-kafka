# 🚲 Smart Bicycle Availability Monitoring Using Apache Kafka

## 1. Project Overview

This project monitors bicycle availability at StadtRAD Hamburg stations using Python and Apache Kafka. It collects real bicycle availability data, detects shortages, generates alerts, and displays results through an interactive Streamlit dashboard.

**Business objectives:**
- Monitor bicycle availability.
- Detect stations with low or zero bicycles.
- Generate shortage alerts.
- Evaluate streaming performance, reliability, and data processing.

## 2. Data Source

- **Provider:** StadtRAD Hamburg
- **Official dataset:** https://suche.transparenz.hamburg.de/dataset/stadtrad-stationen-hamburg39
- **SensorThings API:** https://iot.hamburg.de/v1.0
- **Collection method:** REST API
- **Station:** S Reeperbahn / Königstraße
- **Thing ID:** 4
- **Datastream ID:** 25813
- **Polling interval:** 5 seconds

The producer collects new observations when available. Since the source publishes observations at irregular intervals, the system provides near-real-time monitoring. MQTT was also tested, but REST API is used in the final implementation.

## 3. Technologies

Python 3, Apache Kafka 4.4.0 (KRaft), kafka-python, Requests, REST API, JSON, Streamlit, Git and GitHub.

## 4. System Architecture

**Data flow:**

StadtRAD Hamburg SensorThings REST API  
↓  
Python REST Producer  
↓  
Local Reliability Buffer  
↓  
Kafka Topic: `stadtrad-availability`  
↓  
Python Consumer  
↓  
Idempotency Check and Shortage Detection  
↓  
Kafka Topic: `bicycle-shortage-alerts`  
↓  
Streamlit Dashboard

## 5. Shortage Detection Rules

| Available Bicycles | Status |
|---|---|
| More than 5 | NORMAL |
| 1–5 | LOW |
| 0 | EMPTY |

These are project-defined thresholds.

## 6. Main Features

- **Data collection:** Collects genuine StadtRAD Hamburg observations through the REST API.
- **Kafka streaming:** Transfers observations using Kafka topics.
- **Shortage detection:** Classifies availability as NORMAL, LOW, or EMPTY.
- **Alert generation:** Publishes LOW and EMPTY alerts to a separate Kafka topic.
- **Idempotency:** Uses observation IDs and `processed_ids.txt` to skip previously processed messages.
- **Reliability buffering:** Stores pending observations in `reliability_buffer.json` when Kafka is unavailable.
- **Recovery:** Sends buffered observations to Kafka after the broker becomes available again.
- **Consumer checkpoint:** Records processed observation IDs to prevent repeated processing.
- **Dashboard:** Displays current availability, observation history, and shortage alerts.

### Reliability Test

Kafka was temporarily stopped during testing. The producer buffered observation ID `1537127106`. After Kafka restarted, the producer delivered the buffered observation, and the consumer processed and checkpointed it.

**Limitations:** The buffer is stored locally on the same machine. Kafka uses one broker with replication factor 1. Broker high availability, protection against local machine failure, and exactly-once alert delivery are not guaranteed.

## 7. Dashboard

The interactive Streamlit dashboard displays:

- Current available bicycle count.
- Station name and availability status.
- Latest observation timestamp.
- Recent unique observations.
- Shortage alerts, including historical alerts.

Run the dashboard using:

```bash
streamlit run dashboard/app.py
```

Open http://localhost:8501 in your browser.

## 8. Performance Evaluation

| Evaluation | Result |
|---|---|
| Processing latency | 0.006–0.019 seconds (recent samples) |
| Latest producer throughput | 8,620.69 messages/sec |
| Earlier producer benchmark | 4,464.29 messages/sec |
| Consumer throughput | 314.86 messages/sec |
| Scalability test | 10,000 messages across 3 partitions |
| Scalability producer throughput | 68,493.15 messages/sec |
| Scalability average latency | 2.70 ms |
| Reliability | Buffer recovery successfully tested |
| Idempotency | Duplicate observation successfully skipped |
| Classification validation | 5 out of 5 test cases passed |

**Evaluation notes:**
- Throughput benchmarks used separate Kafka test topics.
- The latest producer benchmark used 1,000 messages of 100 bytes each.
- Processing latency measures the time between producer API receipt (`received_at`) and consumer processing.
- Scalability was tested with three partitions on one broker, demonstrating partition-level distribution rather than multi-broker scalability.
- Classification validation checks predefined business rules, not machine-learning or statistical accuracy.

## 9. Project Structure

```text
smart-bicycle-streaming/
│
├── producer/
│   ├── rest_kafka_producer.py
│   └── mqtt_kafka_producer.py
│
├── consumer/
│   └── shortage_detector.py
│
├── dashboard/
│   └── app.py
│
├── Screenshots/
│   ├── 01_kafka_server.png
│   ├── 02_kafka_topics.png
│   ├── 03_live_producer.png
│   ├── 04_kafka_messages.png
│   ├── 05_shortage_consumer.png
│   ├── 06_alert_messages.png
│   ├── 07_dashboard.png
│   ├── 08_reliability_buffer.png
│   ├── 09_idempotency.png
│   ├── 10_idempotency_duplicate_skipped.png
│   ├── 11_performance_throughput.png
│   ├── 12_scalability_partitions.png
│   ├── 13_scalability_messages.png
│   ├── 14_accuracy_test.png
│   └── 15_latency_test.png
│
├── accuracy_test.py
├── processed_ids.txt
├── reliability_buffer.json
├── producer_state.json
└── README.md
```

## 10. How to Run / Reproduce the Project

Follow these steps to reproduce the project.

### Step 1: Prerequisites

Install Python 3, Apache Kafka 4.4.0, Git, and Java if required by your Kafka installation.

### Step 2: Clone the Repository

```bash
git clone https://github.com/9439-shilpi/smart-bicycle-availability-kafka.git
cd smart-bicycle-availability-kafka
```

### Step 3: Install Dependencies

```bash
python3 -m pip install kafka-python requests streamlit
```

### Step 4: Start Kafka

Start the Kafka broker in KRaft mode using the appropriate configuration file.

For the macOS Homebrew installation used during development:

```bash
kafka-server-start /opt/homebrew/etc/kafka/server.properties
```

Keep this terminal running. Kafka must be accessible at `localhost:9092`. The configuration path may differ on other systems.

### Step 5: Create Kafka Topics

Open another terminal and run:

```bash
kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic stadtrad-availability --partitions 1 --replication-factor 1
```

```bash
kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic bicycle-shortage-alerts --partitions 1 --replication-factor 1
```

Verify:

```bash
kafka-topics --bootstrap-server localhost:9092 --list
```

### Step 6: Start the Consumer

In a separate terminal, from the project directory:

```bash
python3 consumer/shortage_detector.py
```

The consumer waits for observations, detects shortages, and publishes alerts.

### Step 7: Start the Producer

In another terminal:

```bash
python3 producer/rest_kafka_producer.py
```

The producer collects genuine StadtRAD Hamburg observations and publishes them to Kafka.

### Step 8: Start the Dashboard

In another terminal:

```bash
streamlit run dashboard/app.py
```

Open http://localhost:8501 in your browser.

### Step 9: Verify the Results

- Check incoming observations in the `stadtrad-availability` topic.
- Verify shortage classification in the consumer output.
- Check generated alerts in `bicycle-shortage-alerts`.
- Verify current availability and observation history in the dashboard.

**Important:** Internet access is required for the StadtRAD API. New observations depend on the source publication schedule and are not guaranteed every five seconds. Preserve the local state and checkpoint files when reproducing the existing project state. For fresh tests, reset them only when appropriate.

To stop each running service, press `Ctrl + C` in its terminal.

## 11. Conclusion

The project demonstrates a working near-real-time bicycle availability monitoring prototype using genuine StadtRAD Hamburg data and Apache Kafka. It implements data streaming, shortage detection, alert generation, idempotency, reliability buffering, dashboard visualization, and performance evaluation.

## Author

**Sunandana Sahoo**

M.Sc. Applied Data Science & Artificial Intelligence

SRH University of Applied Sciences, Hamburg