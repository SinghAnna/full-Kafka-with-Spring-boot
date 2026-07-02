# Chapter 06 - Kafka Consumer Groups

## 📌 Overview

In the previous chapter, we learned how a Kafka Producer sends messages to a Topic and how a Kafka Consumer receives them.

In this chapter, we will learn one of the most important Kafka concepts: **Consumer Groups**.

Consumer Groups enable Kafka to distribute messages across multiple consumers, allowing applications to scale horizontally, improve throughput, and provide fault tolerance.

This is one of the most frequently asked topics in Kafka interviews.

---

# 🎯 Learning Objectives

After completing this chapter, you will be able to:

- Understand what a Consumer Group is
- Understand why Consumer Groups are needed
- Learn how Kafka distributes messages
- Configure Consumer Groups in Spring Boot
- Understand Load Balancing
- Understand Broadcast Processing
- Learn Consumer Rebalancing
- Understand the relationship between Consumers and Partitions
- Build multiple Consumers in the same application

---

# ❓ Why Consumer Groups?

Imagine thousands of users are placing orders on an e-commerce website every second.

If only one Consumer processes all the messages,

- Processing becomes slow
- Consumer becomes overloaded
- Application cannot scale

Kafka solves this problem using **Consumer Groups**.

Instead of one Consumer,

Kafka allows multiple Consumers to work together.

Each Consumer processes a different subset of messages.

This increases:

- Performance
- Scalability
- Reliability

---

# 📚 What is a Consumer Group?

A Consumer Group is a collection of one or more Kafka Consumers working together to consume messages from the same Topic.

Each Consumer inside the group receives messages from different partitions.

Kafka automatically balances the workload among Consumers.

---

# 🏗 Architecture (Single Consumer)

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
```

---

# 🏗 Architecture (Same Consumer Group)

```text
                 Kafka Topic
                     │
      ┌──────────────┴──────────────┐
      │                             │
Consumer-1                    Consumer-2
(Group-1)                     (Group-1)

Kafka distributes messages
between both consumers.
```

---

# 🏗 Architecture (Different Consumer Groups)

```text
                 Kafka Topic
                     │
      ┌──────────────┴──────────────┐
      │                             │
Consumer-1                    Consumer-2
(Group-1)                     (Group-2)

Both consumers receive
every message independently.
```

---

# 🧠 How Consumer Groups Work

Step 1

Producer publishes messages.

↓

Step 2

Kafka stores the messages inside a Topic.

↓

Step 3

Consumers subscribe to the Topic.

↓

Step 4

Consumers having the same Group ID form one Consumer Group.

↓

Step 5

Kafka assigns partitions to Consumers.

↓

Step 6

Each Consumer reads only its assigned partitions.

---

# 📦 Same Group Example

Suppose we send six messages.

```
Message-1
Message-2
Message-3
Message-4
Message-5
Message-6
```

Consumer Configuration

```
Consumer-1

groupId = group-1
```

```
Consumer-2

groupId = group-1
```

Possible Output

```
Consumer-1

Message-1
Message-3
Message-5
```

```
Consumer-2

Message-2
Message-4
Message-6
```

Only one Consumer processes each message.

This is called **Load Balancing**.

---

# 📦 Different Group Example

Consumer Configuration

```
Consumer-1

groupId = group-1
```

```
Consumer-2

groupId = group-2
```

Output

```
Consumer-1

Message-1
Message-2
Message-3
Message-4
```

```
Consumer-2

Message-1
Message-2
Message-3
Message-4
```

Every Consumer Group receives all messages independently.

---

# ⚖ Same Group vs Different Group

| Same Group | Different Group |
|------------|-----------------|
| Messages are shared | Every group receives all messages |
| Used for Load Balancing | Used for Multiple Services |
| One Consumer processes one message | Every group processes every message |
| High Throughput | Independent Processing |

---

# 📂 Project Structure

```
src
├── controller
│      └── KafkaProducerController.java
│
├── service
│      ├── KafkaProducerService.java
│      ├── KafkaConsumerOne.java
│      └── KafkaConsumerTwo.java
│
├── config
│      └── KafkaTopicConfig.java
│
└── resources
       └── application.properties
```

---

# 📄 Configuration Files

| File | Purpose |
|------|---------|
| application.properties | Kafka Broker & Consumer Group Configuration |
| pom.xml | Kafka Dependencies |
| docker-compose.yml | Kafka Docker Configuration |

---

# 📦 Source Code Files

| Class | Purpose |
|--------|----------|
| KafkaProducerController | Publish messages |
| KafkaProducerService | Send messages |
| KafkaConsumerOne | First Consumer |
| KafkaConsumerTwo | Second Consumer |
| KafkaTopicConfig | Create Topic |

---

# ⚙ application.properties

```properties
spring.application.name=kafka-spring-boot

spring.kafka.bootstrap-servers=localhost:9093

spring.kafka.consumer.group-id=group-1

spring.kafka.consumer.auto-offset-reset=earliest

spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer

spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer
```

---

# 🔍 Properties Explained

| Property | Description |
|----------|-------------|
| bootstrap-servers | Kafka Broker Address |
| group-id | Consumer Group Name |
| auto-offset-reset | Reading Strategy |
| key-deserializer | Deserialize Message Key |
| value-deserializer | Deserialize Message Value |

---

# 🚀 Running the Application

## Step 1

Start Kafka

```bash
docker compose up -d
```

---

## Step 2

Run Spring Boot

```bash
mvn spring-boot:run
```

---

## Step 3

Publish Message

```
POST /api/messages
```

Request Body

```text
Hello Kafka
```

---

## Step 4

Observe Console

Producer

```
Message Sent : Hello Kafka
```

Consumer-1

```
Message Received : Hello Kafka
```

If another Consumer belongs to the same group, Kafka distributes future messages automatically.

---

# 🌍 Real-World Examples

## Food Delivery

```
Customer

↓

Kafka

↓

Restaurant Service

↓

Delivery Service

↓

Notification Service
```

Each service belongs to a different Consumer Group.

---

## Banking

```
Transaction

↓

Kafka

↓

SMS Service

↓

Email Service

↓

Fraud Detection

↓

Audit Service
```

Every service receives the same event.

---

## E-Commerce

```
Order Placed

↓

Kafka

↓

Inventory

↓

Payment

↓

Shipping

↓

Analytics
```

---

# 📚 Key Concepts Covered

- Consumer Group
- Group ID
- Load Balancing
- Broadcast Processing
- Kafka Listener
- Topic Subscription
- Parallel Processing
- Rebalancing
- Message Distribution

---

# 💡 Best Practices

- Always use meaningful Group IDs.
- Keep Consumers stateless whenever possible.
- Create enough partitions for parallel processing.
- Use different Consumer Groups for independent services.
- Monitor Consumer Lag in production.
- Avoid creating unnecessary Consumer Groups.

---

# 🧠 Interview Questions

1. What is a Consumer Group?
2. Why are Consumer Groups used?
3. What is the purpose of Group ID?
4. Can two Consumers belong to the same group?
5. Can one Consumer belong to multiple groups?
6. How does Kafka distribute messages?
7. What happens if a Consumer crashes?
8. What is Consumer Rebalancing?
9. What is Load Balancing?
10. What is Broadcast Processing?
11. What is Consumer Lag?
12. What happens if Consumers are more than Partitions?
13. What happens if Partitions are more than Consumers?
14. Can multiple groups consume the same Topic?
15. How does Kafka ensure fault tolerance?

---

# 🧠 Prerequisites

Before starting this chapter, you should know:

- Kafka Basics
- Kafka Producer
- Kafka Consumer
- Spring Boot
- Docker
- REST API

---

# 📖 Next Chapter

## ➜ Chapter 07 - Kafka Partitions

In the next chapter, we will learn:

- What is a Partition?
- Why Partitions are needed
- Partition Assignment
- Parallel Processing
- Message Ordering
- Partition Strategy
- Best Practices

---

# 🎯 Summary

In this chapter, we learned one of Kafka's most powerful features: **Consumer Groups**.

We understood how Kafka distributes messages among Consumers in the same group for load balancing, while allowing different Consumer Groups to receive the same messages independently.

Consumer Groups are the foundation of scalable, fault-tolerant, and high-performance Kafka applications. Understanding this concept is essential before moving on to Partitions, Offsets, and advanced Kafka architectures.