# Kafka Streams Projects

Two Java 17 projects demonstrating event processing with Apache Kafka Streams.

## Projects

### Click Counter

A Spring Boot application that publishes click events and counts them per user.

- Input topic: `clicks`
- Output topic: `click-counts`
- Web interface: `http://localhost:9090`
- REST endpoints:
    - `GET /clicks/count/all`
    - `GET /clicks/count/{userId}`

Source code: [click-counter-app](./click-counter-app)

### Weather Stream Processing

Processes weather records in the format `station,temperature,humidity`.

- Keeps records with temperatures above 30 degrees Celsius.
- Converts temperatures to Fahrenheit.
- Calculates average temperature and humidity per station.
- Input topic: `weather-data`
- Output topic: `station-averages`

Source code: [weatherStreamApp](./weatherStreamApp)

## Requirements

- JDK 17
- Maven
- Docker with Docker Compose for the included Kafka environment

The root `docker-compose.yaml` defines Kafka and ZooKeeper.
Both Java applications connect to Kafka at `localhost:9092`.