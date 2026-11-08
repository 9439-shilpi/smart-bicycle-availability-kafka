# Smart Bicycle Availability Monitoring and Shortage Detection Using Apache Kafka

## 1. Project Overview

This project develops a real-time bicycle availability monitoring and shortage detection system using Apache Kafka, MQTT and Python.

It uses actual bicycle availability observations from StadtRAD Hamburg to monitor bicycle rental stations and identify low or zero bicycle availability.

## 2. Business Objective

The main objective is to detect bicycle shortages at rental stations.

The system aims to:
- Monitor bicycle availability using live observations.
- Identify stations with low or zero bicycles.
- Generate shortage alerts.
- Display bicycle availability and alerts through a dashboard.

## 3. Real-Time Data Source

**Data provider:** StadtRAD Hamburg

**Official dataset:**  
https://suche.transparenz.hamburg.de/dataset/stadtrad-stationen-hamburg39

**SensorThings API:**  
https://iot.hamburg.de/v1.0

**MQTT broker:** `iot.hamburg.de`

**Confirmed station:** S Reeperbahn / Königstraße

**Datastream ID:** `25813`

**MQTT topic:**  
`v1.0/Datastreams(25813)/Observations`

Repeated live MQTT observations have been successfully received from this station.

## 4. Technologies

- Python
- Apache Kafka
- MQTT
- Paho MQTT
- kafka-python
- Streamlit

## 5. Proposed System Architecture

1. StadtRAD Hamburg provides live bicycle availability observations.
2. A Python MQTT subscriber receives the observations.
3. The Python producer publishes the data to Kafka topic `stadtrad-availability`.
4. A Kafka consumer processes incoming observations.
5. Shortage detection logic identifies low or zero bicycle availability.
6. Shortage alerts are published to `bicycle-shortage-alerts`.
7. A Streamlit dashboard displays availability and alerts.

## 6. Shortage Detection Rules

Initial project-defined thresholds:

- More than 5 bicycles: Normal availability.
- 1 to 5 bicycles: Low availability.
- 0 bicycles: Empty station.

These thresholds are defined for this project and are not official StadtRAD classifications.

## 7. Kafka Topics

- `stadtrad-availability`: Receives bicycle availability observations.
- `bicycle-shortage-alerts`: Stores generated shortage alerts.

## 8. Project Evaluation

The completed system will be evaluated using:

- Throughput
- Processing latency
- Scalability
- Reliability
- Shortage detection accuracy

## 9. Project Status

**Status:** In progress.

The official StadtRAD data source has been investigated, repeated live MQTT observations have been received from one station, and the local Kafka broker and required topics have been created.

The producer, consumer, shortage detection, dashboard and performance evaluation are yet to be implemented.

## 10. Future Work

- Implement the MQTT-to-Kafka producer.
- Implement the Kafka consumer.
- Develop shortage detection and alert generation.
- Build the Streamlit dashboard.
- Perform system testing and performance evaluation.
- Prepare the final project report.