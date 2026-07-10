# kafka-producer-consumer-mini-project
# Kafka Producer-Consumer Pipeline with MySQL and Avro

## Project Overview

This project demonstrates a real-time data pipeline using **MySQL**, **Apache Kafka**, **Confluent Schema Registry**, and **Python**. The producer continuously monitors a MySQL table for newly inserted or updated records, serializes them using **Avro**, and publishes them to a Kafka topic. Multiple Kafka consumers read the messages, process the data, and store the results in JSON files.

---

## Technologies Used

* Python 3.x
* MySQL
* Apache Kafka
* Confluent Cloud
* Confluent Schema Registry
* Apache Avro
* `confluent-kafka` Python library
* `mysql-connector-python`

---

## Project Architecture

```
                +------------------+
                |   MySQL Database |
                |    Product Table |
                +--------+---------+
                         |
     Fetch records where last_updated >
     last_read_timestamp
                         |
                         v
                +------------------+
                | Kafka Producer   |
                | Python + Avro    |
                +--------+---------+
                         |
                         |
                  Kafka Topic
                product_updates
                 (10 Partitions)
                         |
        ---------------------------------
        |        |        |        |
        v        v        v        v
   Consumer 1 Consumer 2 Consumer 3 ...
        |        |        |
        +--------+--------+
                 |
        Process & Save Data
                 |
                 v
            JSON Output Files
```

---

## Features

* Reads data incrementally from a MySQL database.
* Tracks the last processed timestamp using a configuration file.
* Serializes records into Avro format.
* Publishes messages to a Kafka topic.
* Uses Product ID as the Kafka message key to ensure records for the same product are routed to the same partition.
* Supports multiple Kafka consumers within the same consumer group.
* Writes consumed records into JSON files.
* Demonstrates Kafka partitioning and consumer group rebalancing.

---

## Database Schema

```sql
CREATE TABLE Product(
    id INT PRIMARY KEY,
    name VARCHAR(50),
    category VARCHAR(50),
    price FLOAT,
    last_updated TIMESTAMP
);
```

---

## Producer Workflow

1. Connect to MySQL.
2. Read the last processed timestamp from `config.json`.
3. Execute:

```sql
SELECT *
FROM Product
WHERE last_updated > last_read_timestamp;
```

4. Serialize each record using Avro.
5. Publish records to the Kafka topic `product_updates`.
6. Update `config.json` with the latest `last_updated` timestamp.

---

## Consumer Workflow

1. Subscribe to the `product_updates` Kafka topic.
2. Deserialize Avro messages.
3. Convert category names to lowercase.
4. Apply any required business logic.
5. Save processed records into a JSON file.
6. Continue polling for new messages.

---

## Project Structure

```
assignment_practice/
│
├── producer.py
├── consumer.py
├── schema.avsc
├── config.json
├── requirements.txt
├── README.md
└── output/
    ├── consumer1.json
    ├── consumer2.json
    └── ...
```

---

## Installation

Install the required packages:

```bash
pip install confluent-kafka
pip install mysql-connector-python
```

or

```bash
pip install -r requirements.txt
```

---

## Configuration

Before running the project, update the following values with your own Confluent Cloud and MySQL credentials:

* Kafka Bootstrap Server
* Kafka API Key
* Kafka API Secret
* Schema Registry URL
* Schema Registry API Key
* Schema Registry API Secret
* MySQL Host
* MySQL Username
* MySQL Password

**Do not commit real credentials to a public GitHub repository.**

---

## Running the Producer

```bash
python producer.py
```

---

## Running Consumers

Start one or more consumer instances:

```bash
python consumer.py
```

Multiple consumers using the same `group.id` will automatically share Kafka partitions.

---

## Sample Output

Producer

```
Record 101 successfully produced to product_updates
```

Consumer

```
Successfully consumed record with key 101
{
  "id": 101,
  "name": "Whiteboard",
  "category": "stationery",
  "price": 1599.0,
  "last_updated": "2026-07-10T17:40:00"
}
```

---

## Learning Outcomes

* Incremental data extraction from MySQL.
* Kafka Producer and Consumer implementation.
* Apache Avro serialization and deserialization.
* Confluent Schema Registry integration.
* Kafka message partitioning using keys.
* Kafka Consumer Groups and partition rebalancing.
* JSON data processing in Python.

---

## Author

**Debayudh Kundu**
