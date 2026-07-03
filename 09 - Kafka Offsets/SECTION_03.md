# Offset Reset Policies

A common question is:

> **What happens when a Consumer Group starts for the very first time and no committed Offset exists?**

Since Kafka cannot find any previously committed Offset for that Consumer Group, it must decide **where to begin reading**.

This behavior is controlled by the **Offset Reset Policy**.

Configuration:

```properties
spring.kafka.consumer.auto-offset-reset=earliest
```

Possible values are:

- earliest
- latest
- none

---

# 1️⃣ earliest

When the Offset Reset Policy is set to **earliest**, Kafka starts reading from the **beginning of each partition**.

Example

Partition-0

```
Offset 0 → Order-101

Offset 1 → Order-102

Offset 2 → Order-103

Offset 3 → Order-104
```

A new Consumer Group starts.

Kafka begins from

```
Offset 0
```

Result

```
Reads

Order-101

Order-102

Order-103

Order-104
```

### When to Use

- Analytics
- Reporting
- Data Warehousing
- ETL Pipelines
- Historical Data Processing

---

# 2️⃣ latest

When the policy is **latest**, Kafka ignores all existing messages.

It waits only for **new messages**.

Example

Current Topic

```
Offset 0

Offset 1

Offset 2

Offset 3
```

Consumer starts now.

Kafka ignores

```
0

1

2

3
```

Producer sends

```
Offset 4
```

Consumer starts reading from

```
Offset 4
```

### When to Use

- Live Notifications
- Real-Time Dashboards
- Chat Applications
- Monitoring Systems

---

# 3️⃣ none

This is the strictest policy.

If Kafka cannot find a committed Offset,

it throws an exception instead of automatically choosing a starting point.

Example

```
No Committed Offset

↓

Consumer Starts

↓

Kafka Throws Exception
```

This is useful when missing data is unacceptable.

---

# Comparison

| Policy | Reads Old Messages | Reads New Messages | Throws Exception |
|---------|-------------------|-------------------|------------------|
| earliest | ✅ Yes | ✅ Yes | ❌ No |
| latest | ❌ No | ✅ Yes | ❌ No |
| none | ❌ No | ❌ No | ✅ Yes |

---

# Message Replay

One of Kafka's biggest advantages is the ability to **replay messages**.

Replay means reading old messages again.

Example

```
Yesterday

↓

1000 Orders Processed
```

Later,

a bug is discovered.

Instead of asking users to place orders again,

Kafka simply replays the messages.

Consumer starts again from

```
Offset 0
```

All messages are processed again.

---

# Why Replay is Important

Replay is widely used for:

- Bug Fixes
- Analytics
- Data Migration
- Machine Learning
- Event Sourcing
- Disaster Recovery
- Cache Rebuilding

---

# Replay Workflow

```
Producer

↓

Kafka Topic

↓

Messages Stored

↓

Consumer Processes

↓

Bug Found

↓

Restart Consumer

↓

Replay From Offset 0

↓

Messages Processed Again
```

---

# Delivery Semantics

Another important concept related to Offsets is **Delivery Semantics**.

Delivery Semantics define **how many times a message can be processed**.

Kafka supports three delivery guarantees.

---

# 1️⃣ At Most Once

Meaning

```
Message

↓

Read

↓

Offset Committed

↓

Process
```

If the Consumer crashes after committing but before processing,

the message is permanently lost.

Example

```
Read Offset 100

↓

Commit Offset 100

↓

Crash
```

Message is never processed.

### Characteristics

- Fast
- Possible Data Loss
- No Duplicate Processing

---

# 2️⃣ At Least Once

Meaning

```
Read

↓

Process

↓

Commit Offset
```

If the Consumer crashes before committing,

Kafka reads the same message again.

Result

```
Duplicate Processing Possible
```

But

```
No Data Loss
```

This is the most commonly used approach in production.

---

# 3️⃣ Exactly Once

Exactly Once means

```
Every Message

↓

Processed Exactly One Time
```

No duplicates.

No data loss.

Kafka achieves this using

- Idempotent Producers
- Transactions
- Offset Coordination

Exactly Once is an advanced topic that will be covered in a later chapter.

---

# Delivery Semantics Comparison

| Feature | At Most Once | At Least Once | Exactly Once |
|-----------|-------------|--------------|--------------|
| Duplicate Processing | ❌ No | ✅ Possible | ❌ No |
| Data Loss | ✅ Possible | ❌ No | ❌ No |
| Complexity | Low | Medium | High |
| Production Usage | Rare | Very Common | Advanced Systems |

---

# Consumer Lag

Consumer Lag is one of the most important production monitoring metrics.

Consumer Lag means:

> **How far behind a Consumer is compared to the latest message in the partition.**

Formula

```
Lag = Latest Offset − Consumer Offset
```

Example

Latest Offset

```
250
```

Consumer Position

```
230
```

Consumer Lag

```
250 - 230 = 20
```

The Consumer still has **20 messages left to process**.

---

# Why Consumer Lag Happens

Common reasons include:

- Slow Consumer
- Heavy Database Operations
- Network Issues
- High Message Volume
- Consumer Crash
- Insufficient Partitions

---

# Monitoring Consumer Lag

Consumer Lag is one of the most monitored Kafka metrics because high lag indicates that consumers are unable to keep up with producers.

Large production systems continuously monitor Consumer Lag using tools such as:

- Kafka CLI
- Prometheus
- Grafana
- Confluent Control Center
- AKHQ

---

# Real-World Examples

## 🛒 E-Commerce

```
Customer Places Order

↓

Kafka Topic

↓

Inventory Service

↓

Payment Service

↓

Shipping Service
```

Each service maintains its own committed Offsets.

If Shipping crashes,

it resumes from its last committed Offset without affecting other services.

---

## 🏦 Banking

```
Transaction

↓

Kafka

↓

Fraud Detection

↓

SMS Service

↓

Email Service

↓

Audit Service
```

Every Consumer Group tracks its own Offset independently.

---

## 🍔 Food Delivery

```
Order Placed

↓

Kafka

↓

Restaurant

↓

Delivery Partner

↓

Notification Service
```

If Notification Service stops,

Restaurant and Delivery continue normally because each Consumer Group has independent Offsets.

---

# 💡 Key Takeaways

- Offset Reset Policy determines where a new Consumer Group starts reading.
- `earliest` reads all existing messages.
- `latest` reads only future messages.
- `none` throws an exception if no Offset exists.
- Kafka supports replaying old messages by resetting Offsets.
- Delivery Semantics define reliability and processing guarantees.
- Consumer Lag measures how far behind a Consumer is from the latest Offset.
- Monitoring Consumer Lag is essential for production Kafka applications.

---

## 📖 Next Section

In the final section, we will cover:

- Best Practices
- Common Mistakes
- Kafka CLI Commands
- Interview Questions
- Summary
- Next Chapter Preview