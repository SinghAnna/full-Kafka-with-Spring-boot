# Chapter 07 - Kafka Partitions

## 📌 Overview

In the previous chapter, we learned how Kafka Consumer Groups distribute messages among multiple consumers.

However, there is one important question:

> **How does Kafka decide which Consumer should receive which messages?**

The answer is **Partitions**.

Partitions are the core building block of Kafka. They enable Kafka to achieve:

- High Throughput
- Horizontal Scalability
- Parallel Processing
- Fault Tolerance
- Ordered Message Processing

Understanding Partitions is essential before learning Offsets, Rebalancing, Transactions, and Kafka Streams.

---

# 🎯 Learning Objectives

After completing this chapter, you will be able to:

- Understand what a Kafka Partition is
- Learn why Partitions are needed
- Differentiate between Topics and Partitions
- Understand Message Ordering
- Learn Parallel Processing
- Understand Partition Assignment
- Understand Leader and Follower Partitions
- Configure Topics with Multiple Partitions
- Observe Partition Distribution
- Use Kafka CLI to inspect Partitions
- Learn Best Practices
- Answer Kafka Partition Interview Questions

---

# ❓ Why Do We Need Partitions?

Imagine an e-commerce application receiving **one million orders every minute**.

If all messages were stored in a single file,

```
Order-1
Order-2
Order-3
Order-4
...
Order-1000000
```

only one consumer could process them sequentially.

Problems:

- Slow processing
- Low throughput
- No scalability
- Single bottleneck

Kafka solves this problem by dividing a Topic into multiple **Partitions**.

---

# 📚 What is a Partition?

A **Partition** is a smaller, ordered, append-only log inside a Kafka Topic.

Instead of storing all messages in one place,

Kafka stores them across multiple partitions.

Each partition maintains its own message order.

Example:

```
Topic

Orders
```

contains

```
Partition-0

Order-1
Order-4
Order-7
```

```
Partition-1

Order-2
Order-5
Order-8
```

```
Partition-2

Order-3
Order-6
Order-9
```

Together,

these partitions form a single Kafka Topic.

---

# 🏗 Topic vs Partition

## Topic

A Topic is a logical category where Producers publish messages.

Example:

```
orders
payments
notifications
users
```

---

## Partition

A Partition is the physical storage unit inside a Topic.

Each Topic can contain one or many Partitions.

Example

```
orders

├── Partition-0
├── Partition-1
└── Partition-2
```

---

# 🏗 Architecture

```
               Producer
                   │
                   ▼
          Kafka Broker
                   │
                   ▼
             Topic: Orders
                   │
   ┌───────────────┼───────────────┐
   ▼               ▼               ▼
Partition-0   Partition-1   Partition-2
```

---

# 📦 Message Storage

Suppose the Producer sends

```
Order-1
Order-2
Order-3
Order-4
Order-5
Order-6
```

Kafka may store them like this

```
Partition-0

Order-1
Order-4
```

```
Partition-1

Order-2
Order-5
```

```
Partition-2

Order-3
Order-6
```

---

# 🧠 How Partitions Work

Step 1

Producer sends a message.

↓

Step 2

Kafka decides which partition should store the message.

↓

Step 3

Message is appended to that partition.

↓

Step 4

Consumers read messages from partitions.

↓

Step 5

Consumer Groups divide partitions among themselves.

---

# 🚀 Parallel Processing

Without Partitions

```
Topic

↓

Consumer-1

↓

All Messages
```

Only one consumer processes all messages.

---

With Partitions

```
                Topic

        ┌───────┼────────┐
        ▼       ▼        ▼

   Partition0 Partition1 Partition2

        │       │        │

        ▼       ▼        ▼

 Consumer1 Consumer2 Consumer3
```

Now three consumers process messages simultaneously.

This significantly improves application performance.

---

# 📌 Message Ordering

Kafka guarantees message ordering **only within a Partition**.

Example

Partition-0

```
Order-1
Order-2
Order-3
Order-4
```

Kafka always delivers them in the same order.

However,

messages across different partitions are processed independently.

```
Partition-0

Order-1
Order-3
```

```
Partition-1

Order-2
Order-4
```

Global ordering is **not guaranteed**.

---

# 🎯 Partition Assignment

When a Producer sends a message,

Kafka decides the partition using one of these methods.

## Method 1

Round Robin

```
Message-1 → Partition-0

Message-2 → Partition-1

Message-3 → Partition-2

Message-4 → Partition-0
```

---

## Method 2

Message Key

```
Customer-101

↓

Hash(Customer-101)

↓

Partition-2
```

Every message having the same key goes to the same partition.

This guarantees ordering for that key.

---

# 👑 Leader and Follower Partitions

Kafka replicates partitions for fault tolerance.

Example

```
Broker-1

Partition-0 (Leader)
```

```
Broker-2

Partition-0 (Follower)
```

```
Broker-3

Partition-0 (Follower)
```

Producer and Consumers communicate only with the **Leader**.

Followers continuously replicate the Leader's data.

If the Leader crashes,

Kafka automatically promotes one Follower as the new Leader.

---

# 📂 Project Structure

```
src
├── controller
│      └── KafkaProducerController.java
│
├── service
│      ├── KafkaProducerService.java
│      ├── KafkaConsumerService.java
│      ├── KafkaConsumerTwo.java
│      └── KafkaConsumerThree.java
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
| application.properties | Kafka Configuration |
| pom.xml | Maven Dependencies |
| docker-compose.yml | Kafka Broker |
| KafkaTopicConfig.java | Create Topic with Partitions |

---

# ⚙ Topic Configuration

In this chapter we will create

```
Topic Name

first-topic
```

with

```
3 Partitions
```

using Spring Boot.

---

# 🧪 Practical Demonstration

We will:

- Create a Topic with **3 Partitions**
- Publish multiple messages
- Observe partition assignment
- Run multiple Consumers
- Verify parallel processing

---

# 💻 Kafka CLI Commands

## List Topics

```bash
kafka-topics.sh \
--bootstrap-server localhost:9092 \
--list
```

---

## Describe Topic

```bash
kafka-topics.sh \
--bootstrap-server localhost:9092 \
--describe \
--topic first-topic
```

Output

```
Topic: first-topic

PartitionCount: 3

ReplicationFactor: 1
```

---

## View Consumer Groups

```bash
kafka-consumer-groups.sh \
--bootstrap-server localhost:9092 \
--list
```

---

## Describe Consumer Group

```bash
kafka-consumer-groups.sh \
--bootstrap-server localhost:9092 \
--describe \
--group group-1
```

---

# 🌍 Real-World Examples

## E-Commerce

```
Orders Topic

↓

Partition-0

↓

Order Service
```

```
Partition-1

↓

Inventory Service
```

```
Partition-2

↓

Payment Service
```

---

## Banking

```
Transactions

↓

Partition-0

↓

Account Updates
```

```
Partition-1

↓

Fraud Detection
```

```
Partition-2

↓

Audit Service
```

---

## Food Delivery

```
Orders

↓

Partition-0

↓

Restaurant
```

```
Partition-1

↓

Delivery
```

```
Partition-2

↓

Notifications
```

---

# 💡 Best Practices

- Create enough partitions for future scalability.
- Keep partition count consistent in production.
- Use Message Keys when ordering matters.
- Avoid creating too many partitions unnecessarily.
- Monitor partition distribution regularly.
- Balance consumers with partitions.
- Plan partition count before production deployment.

---

# ⚠ Common Mistakes

- Assuming ordering across all partitions.
- Creating only one partition for high traffic.
- Creating hundreds of unnecessary partitions.
- Ignoring Message Keys.
- Increasing partitions without understanding ordering impact.

---

# 📚 Key Concepts Covered

- Topic
- Partition
- Message Ordering
- Parallel Processing
- Partition Assignment
- Leader
- Follower
- Replication
- Scalability
- Throughput

---

# 🧠 Interview Questions

1. What is a Kafka Partition?
2. Why are Partitions needed?
3. What is the difference between Topic and Partition?
4. How does Kafka assign partitions?
5. Does Kafka guarantee message ordering?
6. What is a Leader Partition?
7. What is a Follower Partition?
8. Can Consumers read from Followers?
9. How many Consumers can read one Partition?
10. What happens when the Leader crashes?
11. Why should we use Message Keys?
12. What happens if partitions are increased?
13. Can one Consumer read multiple partitions?
14. Can multiple Consumers read the same partition in one group?
15. How do Partitions improve scalability?

---

# 📖 Next Chapter

## ➜ Chapter 08 — Message Keys & Partitioning

In the next chapter, we will learn:

- What is a Message Key?
- Hashing
- Default Partitioner
- Same Key → Same Partition
- Ordering Guarantee
- Partition Selection Strategy

---

# 🎯 Summary

In this chapter, we explored Kafka Partitions, one of the most important concepts in Apache Kafka.

We learned how Kafka divides Topics into multiple Partitions to achieve scalability, parallel processing, and high throughput. We also understood how message ordering works, how Kafka assigns messages to partitions, and how Leader and Follower Partitions provide fault tolerance.

A solid understanding of Partitions is essential before moving on to Message Keys, Offsets, Rebalancing, and advanced Kafka architecture.