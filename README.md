# 🚲 Smart Bicycle Availability Monitoring Using Apache Kafka

## 1. Project Overview

This project monitors bicycle availability at StadtRAD Hamburg stations using Python and Apache Kafka.

It collects real bicycle availability data, detects shortages, generates alerts, and displays the results on a Streamlit dashboard.

## 2. Business Objective

- Monitor bicycle availability.
- Detect stations with low or zero bicycles.
- Generate shortage alerts.
- Display availability and alerts through a dashboard.
- Process data reliably and efficiently.

## 3. Data Source

- **Provider:** StadtRAD Hamburg
- **Official dataset:** https://suche.transparenz.hamburg.de/dataset/stadtrad-stationen-hamburg39
- **SensorThings API:** https://iot.hamburg.de/v1.0
- **Collection method:** REST API
- **Station:** S Reeperbahn / Königstraße
- **Thing ID:** 4
- **Datastream ID:** 25813

The Python producer checks the API every 5 seconds and collects new observations when available. Since the source publishes data at irregular intervals, monitoring is near-real-time.

MQTT was also tested as an alternative.

## 4. Technologies

Python, Apache Kafka, REST API, JSON, kafka-python, Requests, Streamlit and GitHub.

## 5. System Architecture

**Data flow:**

StadtRAD REST API  
↓  
Python REST Producer  
↓  
Local Reliability Buffer  
↓  
Kafka: `stadtrad-availability`  
↓  
Python Consumer  
↓  
Idempotency Check and Shortage Detection  
↓  
Kafka: `bicycle-shortage-alerts`  
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

- **Data collection:** Collects genuine StadtRAD observations.
- **Kafka streaming:** Transfers observations through Kafka topics.
- **Shortage detection:** Identifies low and empty stations.
- **Idempotency:** Uses observation IDs to skip duplicates.
- **Reliability:** Stores pending observations in a local JSON buffer when Kafka is unavailable and sends them after recovery.
- **Dashboard:** Displays current availability, observation history and shortage alerts.

## 8. Performance Evaluation

| Evaluation | Test Result |
|---|---|
| Latency | Recent processing samples: 0.005–0.025 seconds |
| Producer throughput | 4,464.29 messages/sec |
| Consumer throughput | 314.86 messages/sec |
| Scalability | 10,000 messages across 3 partitions |
| Scalability throughput | 68,493.15 messages/sec |
| Scalability average latency | 2.70 ms |
| Reliability | Buffer recovery successfully tested |
| Idempotency | Duplicate observation successfully skipped |
| Classification validation | 5 out of 5 test cases passed |

*Throughput and scalability figures come from separate Kafka benchmarks using test messages. Classification validation is based on five predefined test cases, not real-world statistical accuracy. Scalability was tested with one Kafka broker.*

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
├── accuracy_test.py
├── processed_ids.txt
├── reliability_buffer.json
├── producer_state.json
└── README.md
```

## 10. Project Status and Conclusion

**Status: Main implementation and core testing completed.**

The project demonstrates how Python and Apache Kafka can process genuine bicycle availability observations, detect shortages, handle duplicate messages, recover from Kafka downtime, and display results through a dashboard.

Throughput, latency, reliability, idempotency, classification rules and partition-level scalability have been evaluated.