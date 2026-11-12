# 🚲 Smart Bicycle Availability Monitoring Using Apache Kafka

## 1. Project Overview

This project monitors bicycle availability at StadtRAD Hamburg stations using Python and Apache Kafka.

It collects real bicycle availability data, detects shortages, generates alerts, and displays the results on a Streamlit dashboard.

## 2. Business Objective

- Monitor bicycle availability.
- Detect stations with low or zero bicycles.
- Generate shortage alerts.
- Display availability and alerts through a dashboard.
- Evaluate streaming performance, reliability, and data processing.

## 3. Data Source

- **Provider:** StadtRAD Hamburg
- **Official dataset:** https://suche.transparenz.hamburg.de/dataset/stadtrad-stationen-hamburg39
- **SensorThings API:** https://iot.hamburg.de/v1.0
- **Collection method:** REST API
- **Station:** S Reeperbahn / Königstraße
- **Thing ID:** 4
- **Datastream ID:** 25813

The Python producer checks the API every 5 seconds and collects new observations when available. Since the source publishes observations at irregular intervals, the system provides near-real-time monitoring.

MQTT was also tested as an alternative data collection method. REST API is used in the final implementation.

## 4. Technologies

- Python
- Apache Kafka
- REST API
- JSON
- kafka-python
- Requests
- Streamlit
- Git and GitHub

## 5. System Architecture

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

## 6. Shortage Detection Rules

| Available Bicycles | Status |
|---|---|
| More than 5 | NORMAL |
| 1–5 | LOW |
| 0 | EMPTY |

These are project-defined thresholds.

## 7. Main Features

- **Live data collection:** Collects genuine StadtRAD Hamburg observations.
- **Kafka streaming:** Transfers observations through Kafka topics.
- **Shortage detection:** Identifies low and empty bicycle availability.
- **Alert generation:** Publishes shortage alerts to a separate Kafka topic.
- **Idempotency:** Uses observation IDs to identify and skip duplicate messages.
- **Reliability buffering:** Saves pending observations in a local JSON buffer when Kafka is unavailable.
- **Recovery mechanism:** Sends buffered observations to Kafka after the broker becomes available again.
- **Consumer checkpoint:** Stores processed observation IDs in `processed_ids.txt` to prevent repeated processing.
- **Dashboard:** Displays current availability, observation history, and shortage alerts.

### Reliability Testing

The reliability mechanism was tested by temporarily stopping Kafka.

1. Kafka broker was stopped.
2. The producer saved an observation in `reliability_buffer.json`.
3. Kafka broker was restarted.
4. The producer successfully sent the buffered observation to Kafka.
5. The consumer processed the recovered observation and recorded its ID in `processed_ids.txt`.

This demonstrates local buffering and recovery after temporary Kafka downtime.

**Reliability limitations:** The buffer is stored locally on the same machine. The implementation does not provide protection against loss of the local machine or its files. Kafka was configured with a single broker and replication factor 1, so broker-level replication and high availability were not tested. Exactly-once alert delivery is not guaranteed.

## 8. Performance Evaluation

| Evaluation | Test Result |
|---|---|
| Processing latency | Recent samples: 0.006–0.019 seconds |
| Producer throughput | 8,620.69 messages/sec (latest test) |
| Earlier producer benchmark | 4,464.29 messages/sec |
| Consumer throughput | 314.86 messages/sec |
| Scalability test | 10,000 messages across 3 partitions |
| Scalability producer throughput | 68,493.15 messages/sec |
| Scalability average latency | 2.70 ms |
| Reliability | Buffer recovery successfully tested |
| Idempotency | Duplicate observation successfully skipped |
| Classification validation | 5 out of 5 test cases passed |

### Performance Test Notes

- Throughput tests used separate Kafka benchmark topics and test messages.
- The latest producer benchmark used 1,000 messages of 100 bytes each.
- The consumer latency measures the time between producer API receipt (`received_at`) and consumer processing.
- The scalability test used one Kafka broker with three partitions. It demonstrates partition-level distribution, not horizontal multi-broker scalability.
- Classification validation checks predefined business rules. It is not machine-learning accuracy or real-world statistical accuracy.

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
│   └── ...
│
├── accuracy_test.py
├── processed_ids.txt
├── reliability_buffer.json
├── producer_state.json
└── README.md
```

## 10. Project Status and Conclusion

**Status: Main implementation and core testing completed.**

The project demonstrates how Python and Apache Kafka can process genuine StadtRAD Hamburg bicycle availability observations, detect shortages, handle duplicate messages, recover from temporary Kafka downtime, and display results through a dashboard.

The system was evaluated for latency, throughput, reliability, idempotency, classification rules, and partition-level scalability.

The implementation provides a working near-real-time bicycle availability monitoring prototype using Apache Kafka.

## Dashboard

The project includes an interactive Streamlit dashboard to monitor live bicycle availability at the selected StadtRAD Hamburg station.

The dashboard displays:
- Current available bicycle count.
- Station name and availability status (NORMAL, LOW, or EMPTY).
- Latest observation timestamp.
- Recent unique bicycle availability observations.
- Bicycle shortage alerts, including historical alerts.

**Run the dashboard:**

```bash
streamlit run dashboard/app.py
```

Open `http://localhost:8501` in your browser.

### Author

**Sunandana Sahoo**

M.Sc. Applied Data Science & Artificial Intelligence

SRH University of Applied Sciences, Hamburg