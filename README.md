
=======
# 🚀 Kafka with Spring Boot

A comprehensive **Beginner → Advanced** guide to learning **Apache Kafka with Spring Boot** through theory, hands-on coding, real-world examples, and interview-focused explanations.

---

## ✨ Features

* 📖 Step-by-step explanations
* 💻 Complete Spring Boot examples
* 🐳 Docker (KRaft Mode) setup
* 🏗️ Real-world project examples
* 🎯 Interview questions
* 📝 Best practices
* ⚠️ Common mistakes
* 📚 Beginner-friendly documentation

---

## 🛠️ Tech Stack

* Java 21
* Spring Boot 3
* Spring Kafka
* Apache Kafka (KRaft Mode)
* Docker
* Maven

---

## 📂 Project Structure

```text
kafka-springboot-tutorial
│
├── docker-compose.yml
├── README.md
├── pom.xml
├── src/
│
├── 01-Introduction
├── 02-Kafka-Setup
├── 03-Producer
├── 04-Consumer
├── 05-Producer-Consumer
├── 06-JSON-Messages
├── 07-Consumer-Groups
├── 08-Partitions
├── 09-Offsets
├── 10-Retry
├── 11-Dead-Letter-Topic
├── 12-Transactions
├── 13-Kafka-Streams
└── 14-Real-World-Projects
```

---

## 🚀 Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/SinghAnna/full-Kafka-with-Spring-boot.git
```

### 2. Navigate to the Project

```bash
cd kafka-springboot-tutorial
```

### 3. Start Kafka

```bash
docker compose up -d
```

### 4. Verify Running Containers

```bash
docker ps
```

### 5. Stop Kafka

```bash
docker compose down
```

---

## 📚 Learning Roadmap

* Introduction to Apache Kafka
* Kafka Installation & Setup
* Kafka Producer
* Kafka Consumer
* Producer & Consumer Communication
* JSON Serialization & Deserialization
* Consumer Groups
* Partitions
* Offsets
* Error Handling & Retry
* Dead Letter Topic (DLT)
* Transactions
* Kafka Streams
* Real-World Projects

---

# 📚 Suggested Course Roadmap

This roadmap is designed to take you from **Kafka fundamentals** to **advanced Kafka development using Spring Boot**. Each chapter builds on the concepts learned in the previous one, making it suitable for beginners as well as developers preparing for interviews.

---

# ✅ Chapter 01 — Introduction to Apache Kafka

## Theory

- What is Apache Kafka?
- Why Kafka was created
- Traditional Messaging vs Kafka
- Kafka Architecture
- Event Streaming
- Kafka Components
- Real-World Use Cases
- Advantages & Limitations

## Practical

- Install Docker
- Run Kafka using Docker
- Verify Kafka Installation

---

# ✅ Chapter 02 — Kafka Setup with Docker

## Theory

- Why Docker?
- Kafka KRaft Mode
- Docker Compose Overview
- Kafka Container Configuration

## Code

- Create `docker-compose.yml`
- Configure Kafka Broker
- Start Kafka
- Stop Kafka
- Verify Running Containers

---

# ✅ Chapter 03 — Kafka Producer

## Theory

- What is a Producer?
- Producer Workflow
- Producer Responsibilities
- Producer Lifecycle
- Real-World Example

## Code

- Spring Boot Kafka Setup
- Kafka Producer Service
- REST Controller
- Kafka Topic Configuration
- Send Messages using REST API

---

# ✅ Chapter 04 — Kafka Consumer

## Theory

- What is a Consumer?
- Consumer Workflow
- Consumer Groups (Introduction)
- Polling Mechanism
- Asynchronous Message Processing

## Code

- Configure Consumer
- Create Kafka Consumer
- Use `@KafkaListener`
- Receive Messages
- Test Producer → Consumer

---

# ✅ Chapter 05 — Producer & Consumer Communication Flow

## Theory

- Complete Message Journey
- Internal Kafka Workflow
- REST Client → Producer → Broker → Topic → Consumer
- Polling Mechanism
- Why Kafka is Asynchronous
- End-to-End Communication

## Code

- Reuse Existing Producer
- Reuse Existing Consumer
- Observe Complete Flow

---

# ✅ Chapter 06 — Kafka Consumer Groups

## Theory

- What is a Consumer Group?
- Why Consumer Groups are Needed
- Group ID
- Load Balancing
- Broadcast Processing
- Consumer Rebalancing
- Consumers vs Partitions
- Real-World Examples

## Code

- Multiple Consumers
- Same Consumer Group
- Different Consumer Groups
- Observe Message Distribution

---

# ✅ Chapter 07 — Kafka Partitions

## Theory

- What is a Partition?
- Why Partitions are Needed
- Topic vs Partition
- Message Ordering
- Parallel Processing
- Partition Assignment
- Leader & Follower Partitions
- Real-World Examples
- Best Practices

## Code

- Create Topic with Multiple Partitions
- Send Multiple Messages
- Observe Partition Distribution
- View Partitions using Kafka CLI

---

# ✅ Chapter 08 — Message Keys & Partitioning

## Theory

- What is a Message Key?
- Why Use Message Keys?
- Default Kafka Partitioner
- Hash-Based Partitioning
- Ordering Guarantee
- Same Key → Same Partition
- Null Key Behavior

## Code

- Send Messages with Keys
- Send Messages without Keys
- Observe Partition Assignment
- Verify Ordering

---

# ✅ Chapter 09 — Kafka Offsets

## Theory

- What is an Offset?
- Why Offsets Exist
- Offset Management
- Current Offset
- Committed Offset
- Auto Commit
- Manual Commit
- Replay Messages

## Code

- Configure Auto Commit
- Configure Manual Commit
- Read Offsets
- Replay Consumed Messages

---

# ✅ Chapter 10 — Consumer Rebalancing

## Theory

- What is Rebalancing?
- Why Rebalancing Happens
- Consumer Join
- Consumer Leave
- Consumer Crash
- Partition Reassignment
- Rebalance Listener

## Demo

- Start Multiple Consumers
- Stop a Consumer
- Add a New Consumer
- Observe Rebalancing

---

# ✅ Chapter 11 — Serialization & Deserialization

## Theory

- Serializer
- Deserializer
- String Serialization
- JSON Serialization
- Binary Serialization
- Avro (Introduction)

## Code

- Configure Serializers
- Configure Deserializers
- Send String Messages
- Send JSON Messages

---

# ✅ Chapter 12 — Sending Java Objects

## Theory

- Why Send Objects?
- POJO Serialization
- JSON Conversion
- Type Mapping

## Code

- Create DTO
- Configure JsonSerializer
- Configure JsonDeserializer
- Producer
- Consumer
- Send & Receive Java Objects

---

# ✅ Chapter 13 — Multiple Topics

## Theory

- Why Multiple Topics?
- Topic Design
- Naming Conventions
- Topic Organization

## Code

- Create Multiple Topics
- Multiple Producers
- Multiple Consumers
- Consume from Different Topics

---

# ✅ Chapter 14 — Kafka Acknowledgements (ACKS)

## Theory

- Producer Acknowledgements
- `acks=0`
- `acks=1`
- `acks=all`
- Reliability vs Performance
- Durability

## Code

- Configure Different ACK Modes
- Compare Behavior

---

# ✅ Chapter 15 — Retry Mechanism

## Theory

- Producer Retry
- Consumer Retry
- Retry Backoff
- Retry Policies
- Failure Handling

## Code

- Producer Retry Configuration
- Consumer Retry Configuration
- Backoff Strategy

---

# ✅ Chapter 16 — Dead Letter Topic (DLT)

## Theory

- What is DLT?
- Why DLT is Needed
- Error Handling
- Failed Messages
- Recovery Strategy

## Code

- Spring Kafka Retry
- Dead Letter Topic
- Dead Letter Publishing
- Error Handler Configuration

---

# ✅ Chapter 17 — Kafka Transactions

## Theory

- Kafka Transactions
- Exactly Once Semantics (EOS)
- Idempotent Producer
- Transaction Coordinator
- Atomic Writes

## Code

- Configure Transactions
- Transactional Producer
- Transactional Consumer

---

# ✅ Chapter 18 — Kafka Streams (Introduction)

## Theory

- What is Kafka Streams?
- Stream Processing
- Stateless Operations
- Stateful Operations
- KStream
- KTable
- Stream Topology

## Code

- Basic Kafka Streams Application
- Stream Transformation
- Aggregation Example

---

# ✅ Chapter 19 — Kafka with Spring Boot Microservices

## Theory

- Event-Driven Architecture
- Asynchronous Communication
- Service Decoupling
- Event Choreography

## Project

- Order Service
- Payment Service
- Inventory Service
- Shipping Service
- Notification Service
- Email Service

---

# ✅ Chapter 20 — Kafka Monitoring & Management

## Theory

- Kafka UI
- Consumer Lag
- Broker Monitoring
- Topic Monitoring
- Performance Metrics
- Logging

## Tools

- Kafka UI
- Docker Logs
- Kafka CLI
- Spring Boot Actuator

---

# ✅ Chapter 21 — Kafka Interview Questions

## Topics Covered

- Top 100 Kafka Interview Questions
- Frequently Asked Scenarios
- Producer Questions
- Consumer Questions
- Consumer Groups
- Partitions
- Offsets
- Rebalancing
- Transactions
- DLT
- Streams
- Spring Boot Integration
- Best Practices
- Common Mistakes
- Production Tips

---

# 🎯 Final Project

Build a complete **Event-Driven E-Commerce Microservices System** using **Spring Boot + Apache Kafka**.

### Services

- User Service
- Product Service
- Order Service
- Payment Service
- Inventory Service
- Shipping Service
- Notification Service
- Email Service

### Kafka Concepts Used

- Producers
- Consumers
- Topics
- Partitions
- Consumer Groups
- Offsets
- Keys
- JSON Serialization
- Retry
- Dead Letter Topics
- Transactions
- Kafka Streams

---

# 🚀 Outcome

After completing this roadmap, you will be able to:

- Build production-ready Kafka applications
- Integrate Kafka with Spring Boot
- Design scalable event-driven systems
- Understand advanced Kafka internals
- Implement fault-tolerant messaging
- Crack Kafka & Spring Boot interview questions confidently

## 🎯 Prerequisites

* Java Basics
* Spring Boot Basics
* REST APIs
* Maven
* Docker

---

## 🌟 Repository Goals

* Learn Kafka from scratch
* Build production-ready applications
* Prepare for Java/Spring Boot interviews
* Understand real-world Kafka architecture
* Practice with hands-on examples

---

## 🤝 Contributing

Contributions are welcome! Feel free to open an issue or submit a pull request to improve this repository.

---


