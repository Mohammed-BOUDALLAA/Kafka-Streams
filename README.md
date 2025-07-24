# TP_KafkaStream

This directory contains Kafka Streams-based mini-projects demonstrating real-time stream processing using Apache Kafka, Spring Boot, and Docker.

## Contents

- [`click-counter-app`](./click-counter-app): Simulates user click events sent to Kafka, processes them using Kafka Streams, and exposes a REST API for real-time click counts.
- [`weatherStreamApp`](./weatherStreamApp): Processes real-time weather data streams from Kafka using Kafka Streams.
- [`docker-compose.yaml`](./docker-compose.yaml): Spins up the Kafka and Zookeeper environment for testing both apps.

## How to Use

1. **Start Kafka environment**:
   ```bash
   docker-compose up -d
   ```

2. **Navigate to the desired app folder** and follow its instructions to run the producer, stream processor, and consumer components.

---

> 📦 Make sure you have Java 17+, Maven, and Docker installed for best compatibility.
