### Logistics Data Processing with Kafka and MongoDB

This project integrates Kafka and MongoDB to process logistics data, using Avro for data serialization/deserialization. It includes a Kafka producer and consumer, along with an API for MongoDB interaction.

![alt text](/logistics_data_processing/diagram.png)
---

### Table of Contents

1. [Introduction](#introduction)
2. [Prerequisites](#prerequisites)
3. [Setup and Installation](#setup-and-installation)
4. [Usage](#usage)
5. [Schema Registry](#schema-registry)
6. [Kafka Producer](#kafka-producer)
7. [Kafka Consumer](#kafka-consumer)
8. [Data Validation](#data-validation)
9. [API Development](#api-development)


---

### Introduction

This project aims to demonstrate how to process logistics data using Kafka and MongoDB. It includes a Kafka producer that reads logistics data from a CSV file, serializes it into Avro format, and publishes it to a Kafka topic. A Kafka consumer subscribes to this topic, deserializes the Avro data, performs data validation, and ingests it into MongoDB. Additionally, an API is developed to interact with the MongoDB collection.

---

### Prerequisites

Before starting, ensure you have the following installed:

- Python (3.6+)
- Apache Kafka
- Confluent Kafka Python library (`confluent-kafka`)
- MongoDB
- Pandas
- Avro Python library (`avro-python3`)
- Flask (for API development)

---

### Setup and Installation

1. **Clone the repository:**

   ```bash
   git clone https://github.com/your/repository.git
   cd repository
   ```
2. **Install dependencies:**

   ```bash
     pip install -r requirements.txt
   ```   
3. **Set up Kafka and MongoDB:**

- Install and configure Kafka according to the official documentation.
- Set up MongoDB and create a database and collection for storing logistics data.

### Usage
1. **Start Kafka:**
- Start Zookeeper and Kafka server.

2**Run Kafka producer:**

```bash
    python kafka_producer.py
```
- This script reads data from data/logistics.csv, serializes it into Avro format, and publishes it to the Kafka topic logistics-topic.

3. **Run Kafka consumer:**
```bash
    python kafka_consumer.py
```
- The consumer script subscribes to logistics-topic, deserializes Avro data, validates it, and ingests it into MongoDB.
----------------------------------------

**Schema Registry**
- Ensure Schema Registry is set up and running. Avro schemas for serialization/deserialization are managed by Schema Registry to maintain compatibility between producer and consumer.

**Kafka Producer**
- The Kafka producer script (kafka_producer.py) reads logistics data from a CSV file using Pandas, serializes it into Avro format using an Avro schema fetched from Schema Registry, and publishes messages to Kafka.

**Kafka Consumer**
- The Kafka consumer script (kafka_consumer.py) subscribes to the Kafka topic, deserializes Avro data using the schema fetched from Schema Registry, validates the data (e.g., null value checks, data type validation), and inserts valid records into MongoDB.

**Data Validation**
- Data validation in the consumer script includes checks for null values, data type validation, and format checks. Assumptions made include assuming a specific structure and format of incoming logistics data.

**API Development**

*An API (app.py) is developed using Flask and MongoDB Atlas:*
- Endpoints are provided to interact with MongoDB collections.
- Example endpoints include filtering specific JSON documents and aggregating data based on certain criteria.

### API Development

An API (`app.py`) is developed using Flask and MongoDB Atlas:

- Endpoints are provided to interact with MongoDB collections.
- Example endpoints include filtering specific JSON documents and aggregating data based on certain criteria.

### Example Endpoints

For detailed examples and to explore API endpoints, refer to the [Postman Collection](/logistics_data_processing/delivery_trip_truck_data/logistics_api_collection.postman_collection.json) provided with this repository. The collection includes request examples for:

- Filtering specific JSON documents based on attributes.
- Aggregating data to retrieve summaries or statistics.

