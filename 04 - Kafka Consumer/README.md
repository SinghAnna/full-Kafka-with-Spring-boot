# Chapter 04 - Kafka Consumer

## 📌 Overview

In the previous chapter, we learned how to publish messages to a Kafka Topic using a Kafka Producer.

In this chapter, we will learn how to consume those messages using a Kafka Consumer in Spring Boot.

A Kafka Consumer continuously listens to one or more Kafka Topics and processes incoming messages in real time.

By the end of this chapter, you will have a complete Producer → Topic → Consumer communication flow.

---

# 🎯 Learning Objectives

After completing this chapter, you will be able to:

- Understand what a Kafka Consumer is
- Understand the role of a Consumer
- Learn how Kafka Consumers work internally
- Configure a Kafka Consumer in Spring Boot
- Create a Consumer using `@KafkaListener`
- Understand Consumer Groups
- Receive messages from a Kafka Topic
- Test end-to-end Producer → Consumer communication

---

# 🏗 Architecture

```text
                REST Client
                     │
                     ▼
      Spring Boot REST Controller
                     │
                     ▼
            Kafka Producer
                     │
                     ▼
══════════════════════════════════════
            Apache Kafka Broker
══════════════════════════════════════
                     │
                     ▼
              first-topic
                     │
                     ▼
            Kafka Consumer
                     │
                     ▼
             Console Output
```

---

# 📂 Project Structure

```text
src
├── controller
│   └── KafkaProducerController.java
│
├── service
│   ├── KafkaProducerService.java
│   └── KafkaConsumerService.java
│
├── config
│   └── KafkaTopicConfig.java
│
└── resources
    └── application.properties
```

---

# 📄 Configuration Files

| File | Description |
|------|-------------|
| `application.properties` | Kafka Broker Configuration |
| `pom.xml` | Maven Dependencies |
| `docker-compose.yml` | Kafka Docker Container |

---

# 📦 Source Code Files

| Class | Responsibility |
|--------|----------------|
| `KafkaProducerController` | Exposes REST API to publish messages |
| `KafkaProducerService` | Publishes messages to Kafka Topic |
| `KafkaConsumerService` | Consumes messages using `@KafkaListener` |
| `KafkaTopicConfig` | Creates Kafka Topic automatically |

---

# ⚙️ How It Works

### Step 1

The client sends an HTTP request to the REST API.

```
POST /api/messages
```

↓

### Step 2

The Spring Boot Controller receives the request.

↓

### Step 3

The Controller calls the Producer Service.

↓

### Step 4

The Producer publishes the message to the Kafka Topic.

↓

### Step 5

Kafka Broker stores the message inside the Topic.

↓

### Step 6

The Kafka Consumer continuously listens to the Topic.

↓

### Step 7

As soon as a new message arrives, the Consumer receives it using `@KafkaListener`.

↓

### Step 8

The Consumer processes the message and prints it to the console.

---

# 🌐 API Endpoint

## Publish Message

| Method | Endpoint |
|---------|----------|
| POST | `/api/messages` |

### Request Body

```text
Hello Kafka
```

### Response

```text
Message Published Successfully
```

---

# 🚀 Running the Application

## 1. Start Kafka

```bash
docker compose up -d
```

---

## 2. Verify Kafka Container

```bash
docker ps
```

---

## 3. Run Spring Boot Application

```bash
mvn spring-boot:run
```

or run the project directly from IntelliJ IDEA.

---

## 4. Send a Request

```
POST http://localhost:8080/api/messages
```

Request Body

```text
Hello Kafka
```

---

# 📺 Expected Output

### Producer

```text
====================================
Message Sent : Hello Kafka
====================================
```

### Consumer

```text
====================================
Message Received : Hello Kafka
====================================
```

---

# 🔄 Complete Message Flow

```text
REST Client
      │
      ▼
Spring Boot Controller
      │
      ▼
Kafka Producer
      │
      ▼
Kafka Broker
      │
      ▼
first-topic
      │
      ▼
Kafka Consumer
      │
      ▼
Console Output
```

---

# 📚 Key Concepts Covered

- Kafka Consumer
- Consumer Workflow
- Kafka Listener
- Topic Subscription
- Kafka Broker
- Consumer Group
- Polling
- Message Consumption
- Event-Driven Architecture
- Producer → Consumer Communication

---

# 🛠 Prerequisites

Before starting this chapter, you should know:

- Java Basics
- Spring Boot Basics
- Kafka Basics
- Kafka Producer
- REST APIs
- Docker
- Maven

---

# 📖 Related Files

This chapter includes the following files:

- README.md
- Theory.md
- application-properties.md
- KafkaProducerController.java
- KafkaProducerService.java
- KafkaConsumerService.java
- KafkaTopicConfig.java

---

# 💡 What You'll Build

By the end of this chapter, you will have built:

- ✅ Kafka Topic
- ✅ Kafka Producer
- ✅ Kafka Consumer
- ✅ REST API
- ✅ End-to-End Producer → Consumer Communication

---

# 📖 Next Chapter

## Chapter 05 – Producer & Consumer End-to-End Communication

In the next chapter, we will explore the complete internal communication flow between Producer and Consumer.

Topics covered:

- Message Journey
- Internal Kafka Workflow
- Broker Communication
- Message Delivery
- Acknowledgement
- Event Streaming

---

# 🎯 Summary

In this chapter, we successfully created our first Kafka Consumer using Spring Boot.

We learned how a Consumer subscribes to a Kafka Topic, continuously listens for new messages, and processes them using the `@KafkaListener` annotation.

We also implemented an end-to-end communication flow where a REST API publishes messages through a Kafka Producer, Kafka stores those messages in a Topic, and the Consumer receives them in real time.

This chapter builds the foundation for advanced Kafka concepts such as Consumer Groups, Partitions, Offsets, Retry Mechanism, Dead Letter Topics (DLT), Transactions, and Kafka Streams.

---

⭐ **Congratulations!** You have successfully built your first Kafka Consumer with Spring Boot.