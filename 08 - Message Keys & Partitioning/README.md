# Chapter 08 - Message Keys & Partitioning

## 📌 Overview

In the previous chapter, we learned how Kafka Topics are divided into multiple Partitions to improve scalability and parallel processing.

In this chapter, we will explore **Message Keys**, one of the most important concepts in Kafka.

A Message Key determines **which partition a message is stored in**. It also guarantees **message ordering** for related events.

Understanding Message Keys is essential when building distributed systems such as banking, e-commerce, ride-sharing, logistics, and financial applications where event ordering matters.

---

# 🎯 Learning Objectives

After completing this chapter, you will be able to:

- Understand what a Message Key is
- Differentiate between Key and Value
- Understand why Message Keys are used
- Learn how Kafka selects partitions
- Understand the Default Kafka Partitioner
- Learn Hash-Based Partitioning
- Understand ordering guarantees
- Predict where messages will be stored
- Design partitioning strategies for real-world applications

---

# 📚 Prerequisites

Before starting this chapter, you should already know:

- Kafka Basics
- Kafka Topics
- Kafka Partitions
- Kafka Producer
- Kafka Consumer
- Spring Boot Basics

---

# ❓ What is a Message Key?

A **Message Key** is an optional identifier attached to every Kafka message.

Kafka uses the Key to decide **which partition should store the message**.

The message itself is stored as a **Key–Value pair**.

```
Key  -------> Partition Selection

Value ------> Actual Data
```

Example

```
Key

Customer-101
```

```
Value

Order Placed
```

Kafka hashes the key and automatically decides the destination partition.

---

# 📦 Structure of a Kafka Message

Every Kafka message consists of multiple parts.

```
+-----------------------------------------+
| Key        : Customer-101               |
| Value      : Order Created              |
| Timestamp  : 1751284216621              |
| Partition  : 2                          |
| Offset     : 35                         |
+-----------------------------------------+
```

Not every field is supplied by the Producer.

Kafka automatically generates the Partition, Offset, and Timestamp.

---

# 🔑 Key vs Value

| Key | Value |
|------|-------|
| Used for partition selection | Actual business data |
| Can be null | Usually contains useful information |
| Helps maintain ordering | Contains event payload |
| Used during hashing | Stored inside Kafka |

Example

```
Key

Order-1001
```

```
Value

Order Created Successfully
```

---

# ❓ Why Use Message Keys?

Without Keys, Kafka distributes messages across partitions automatically.

With Keys, Kafka ensures that related messages always go to the same partition.

Benefits include:

- Message Ordering
- Better Data Locality
- Predictable Partition Assignment
- Easier Consumer Processing
- Consistent Event Streams

---

# 🏦 Real-World Example

Imagine a banking application.

Transactions:

```
Deposit

Account-101
```

```
Withdraw

Account-101
```

```
Balance Check

Account-101
```

If these events are stored in different partitions,

```
Partition-0

Withdraw
```

```
Partition-2

Deposit
```

the consumer may process them out of order.

This can lead to incorrect account balances.

Using the same Message Key:

```
Key

Account-101
```

ensures all events are written to the same partition and processed in the correct sequence.

---

# 🛒 E-Commerce Example

Customer places multiple orders.

```
Key

Customer-55
```

Messages

```
Order Created

↓

Payment Success

↓

Inventory Updated

↓

Order Packed

↓

Order Delivered
```

Since every event uses the same key,

Kafka stores them in the same partition.

The consumer receives them in the exact order.

---

# 🚕 Ride-Sharing Example

```
Driver-11
```

Events

```
Ride Accepted

↓

Reached Pickup

↓

Ride Started

↓

Ride Completed
```

All events remain ordered because they share the same Message Key.

---

# 🏗 Architecture

```
REST Client
      │
      ▼
Spring Boot Controller
      │
      ▼
Kafka Producer
      │
      ▼
Message Key
      │
      ▼
Hash Function
      │
      ▼
Partition Selection
      │
      ▼
Kafka Topic
      │
      ▼
Kafka Consumer
```

---

# ⚙ Producer Workflow

Step 1

Client sends a request.

↓

Step 2

Spring Boot Controller receives the request.

↓

Step 3

Producer creates a Kafka message.

↓

Step 4

Producer optionally attaches a Message Key.

↓

Step 5

Kafka hashes the key.

↓

Step 6

Kafka selects a partition.

↓

Step 7

Message is stored in the selected partition.

↓

Step 8

Consumers read the message.

---

# 🧮 Default Kafka Partitioner

Kafka uses the following strategy:

### Case 1

If the Key is present

```
Partition = hash(Key) % Number_of_Partitions
```

Example

```
Key

Customer-101
```

```
hash(Customer-101)

↓

24537891

↓

24537891 % 3

↓

Partition-0
```

Every future message with the same key will also go to Partition-0.

---

### Case 2

If the Key is NULL

Kafka does not use hashing.

Instead, it distributes messages across partitions using its default partitioning strategy, which spreads records evenly for balanced throughput.

Example

```
Message-1

↓

Partition-0
```

```
Message-2

↓

Partition-1
```

```
Message-3

↓

Partition-2
```

---

# 🔢 Hash-Based Partitioning

Kafka internally computes a hash value from the Message Key.

```
Key

Order-1001

↓

Hash Function

↓

Random Integer

↓

Modulus (%)

↓

Partition Number
```

Formula

```
Partition = hash(Key) % Total Partitions
```

Example

```
Key

Order-1001

↓

Hash

↓

987654321

↓

987654321 % 3

↓

Partition-0
```

Whenever the same key appears,

```
Order-1001
```

Kafka repeats the same calculation and selects the same partition.

This guarantees consistent partition assignment and preserves ordering for that key.

---

# 🎯 Key Takeaways

- A Message Key is optional but extremely useful.
- Kafka uses the Key to determine the destination partition.
- The same Key always maps to the same partition (as long as the partition count remains unchanged).
- Ordering is guaranteed only within a single partition.
- Without a Key, Kafka distributes messages across partitions for load balancing.
- Choosing the right key is critical for scalable and reliable event-driven systems.

---

## 📖 Next Part

**Part 2** will cover:

- Sending messages **with a Message Key**
- Sending messages **without a Message Key**
- Producer implementation in Spring Boot
- REST APIs
- Console output
- Observing partition assignment
- Comparing keyed vs non-keyed messages