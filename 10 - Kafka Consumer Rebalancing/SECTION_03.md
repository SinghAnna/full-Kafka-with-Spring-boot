# Partition Reassignment

Partition Reassignment is the process of redistributing topic partitions among Consumers after a Rebalance.

Whenever Rebalancing occurs, Kafka calculates a new partition assignment and distributes the partitions fairly across all available Consumers.

Partition Reassignment happens automatically.

No manual intervention is required.

---

## Example

Initially only one Consumer is running.

```text
Consumer-1

Partition-0
Partition-1
Partition-2
Partition-3
```

Now Consumer-2 joins.

Kafka performs Rebalancing.

After Rebalancing,

```text
Consumer-1

Partition-0
Partition-1

Consumer-2

Partition-2
Partition-3
```

Kafka attempts to distribute the workload equally.

---

# Partition Assignment Strategies

Kafka provides multiple algorithms for assigning partitions to Consumers.

These are called **Partition Assignors**.

Spring Boot uses Kafka's assignors internally.

The commonly used assignment strategies are:

- Range Assignor
- Round Robin Assignor
- Sticky Assignor
- Cooperative Sticky Assignor

---

# 1️⃣ Range Assignor

Range Assignor is Kafka's traditional partition assignment strategy.

It assigns consecutive partitions to Consumers.

Example

Topic contains

```text
Partition-0
Partition-1
Partition-2
Partition-3
```

Consumers

```text
Consumer-1

Consumer-2
```

Assignment

```text
Consumer-1

Partition-0
Partition-1

Consumer-2

Partition-2
Partition-3
```

Advantages

- Simple
- Fast
- Default for many Kafka versions

Disadvantages

- Load may not always be perfectly balanced.

---

# 2️⃣ Round Robin Assignor

Round Robin distributes partitions one by one.

Example

Partitions

```text
P0
P1
P2
P3
P4
P5
```

Consumers

```text
C1

C2
```

Assignment

```text
Consumer-1

P0
P2
P4

Consumer-2

P1
P3
P5
```

Advantages

- Better load balancing
- Uniform partition distribution

Disadvantages

- May move many partitions during Rebalancing.

---

# 3️⃣ Sticky Assignor

Sticky Assignor tries to minimize partition movement.

Instead of assigning partitions from scratch,

Kafka keeps existing assignments whenever possible.

Example

Before Rebalance

```text
Consumer-1

P0
P1

Consumer-2

P2
P3
```

A new Consumer joins.

Instead of moving every partition,

Kafka moves only the minimum number required.

Example

```text
Consumer-1

P0

Consumer-2

P2
P3

Consumer-3

P1
```

Advantages

- Less partition movement
- Better performance
- Lower downtime

---

# 4️⃣ Cooperative Sticky Assignor

Cooperative Sticky Assignor is an improved version of Sticky Assignor.

Instead of revoking all partitions,

Kafka gradually transfers only the partitions that need reassignment.

Example

Traditional Rebalance

```text
Stop Everyone

↓

Remove All Partitions

↓

Assign Again

↓

Resume
```

Cooperative Rebalance

```text
Keep Working

↓

Move Required Partitions Only

↓

Continue Processing
```

Advantages

- Very small downtime
- Faster Rebalancing
- Better for production systems

---

# Assignment Strategy Comparison

| Strategy | Load Balance | Partition Movement | Performance |
|-----------|--------------|-------------------|-------------|
| Range | Good | Medium | Good |
| Round Robin | Excellent | High | Good |
| Sticky | Excellent | Low | Better |
| Cooperative Sticky | Excellent | Very Low | Best |

---

# Eager Rebalancing

Older Kafka versions use **Eager Rebalancing**.

Workflow

```text
Stop All Consumers

↓

Remove All Partitions

↓

Assign Again

↓

Resume Consumption
```

Disadvantages

- High downtime
- Message processing pauses
- More partition movement

---

# Cooperative Rebalancing

Modern Kafka prefers Cooperative Rebalancing.

Workflow

```text
Consumer Joins

↓

Move Required Partitions Only

↓

Other Consumers Continue Working

↓

Minimal Pause
```

Advantages

- Faster
- Less downtime
- Better throughput
- Production friendly

---

# Static Membership

Normally,

when a Consumer restarts,

Kafka considers it a completely new Consumer.

This triggers unnecessary Rebalancing.

Static Membership solves this problem.

Each Consumer gets a permanent identity.

Example

```properties
group.instance.id=consumer-1
```

Now,

if Consumer-1 restarts quickly,

Kafka recognizes it as the same Consumer.

Benefits

- Less Rebalancing
- Faster restart
- Stable Consumer Groups

---

# Rebalance Listener

Kafka provides a **ConsumerRebalanceListener** interface.

It allows developers to execute custom logic during Rebalancing.

Two important callback methods are available.

## onPartitionsRevoked()

Called before partitions are removed.

Typical use cases

- Commit offsets
- Save application state
- Close resources

---

## onPartitionsAssigned()

Called after new partitions are assigned.

Typical use cases

- Load state
- Initialize caches
- Resume processing

---

# Rebalance Listener Workflow

```text
Rebalance Starts

↓

onPartitionsRevoked()

↓

Kafka Assigns Partitions

↓

onPartitionsAssigned()

↓

Consumers Resume Processing
```

---

# Complete Consumer Rebalancing Lifecycle

```text
Consumer Starts

↓

Join Consumer Group

↓

Heartbeat Sent

↓

Consume Messages

↓

Consumer Leaves / Crashes

↓

Group Coordinator Detects Change

↓

Rebalance Triggered

↓

Partitions Revoked

↓

Partition Assignment

↓

Partitions Assigned

↓

Consumers Resume
```

---

# Real-World Example

## Food Delivery System

```text
Customers

↓

Kafka Topic

↓

Restaurant Service

↓

Delivery Service

↓

Notification Service
```

Suppose another Delivery Service instance starts.

Kafka automatically distributes partitions between both Delivery Service Consumers.

Result

- Faster order processing
- Better scalability
- No duplicate processing

---

## Banking System

```text
Transactions

↓

Kafka

↓

Fraud Detection

↓

Audit Service

↓

SMS Service

↓

Email Service
```

If one Fraud Detection Consumer crashes,

Kafka automatically transfers its partitions to another Consumer.

Processing continues without manual intervention.

---

# Best Practices

- Keep Consumers stateless whenever possible.
- Avoid long message processing.
- Increase partitions for better parallelism.
- Use Sticky or Cooperative Sticky Assignor.
- Configure appropriate heartbeat intervals.
- Tune session timeout carefully.
- Use Static Membership for stable applications.
- Monitor Consumer Lag.
- Handle Rebalance events using Rebalance Listeners.
- Minimize processing inside the Consumer thread.

---

# Common Mistakes

❌ Creating too many Consumers with very few partitions.

❌ Ignoring Heartbeat configuration.

❌ Very large processing time causing `max.poll.interval.ms` timeout.

❌ Using Eager Rebalancing unnecessarily.

❌ Not handling offsets before Rebalancing.

❌ Blocking Consumer threads for a long time.

---

# Interview Questions

1. What is Consumer Rebalancing?

2. Why does Kafka perform Rebalancing?

3. What triggers Rebalancing?

4. What is a Group Coordinator?

5. What happens when a Consumer crashes?

6. What is Session Timeout?

7. What are Heartbeats?

8. What is Max Poll Interval?

9. What is Partition Reassignment?

10. Explain Range Assignor.

11. Explain Round Robin Assignor.

12. Explain Sticky Assignor.

13. Explain Cooperative Sticky Assignor.

14. Difference between Eager and Cooperative Rebalancing?

15. What is Static Membership?

16. What is ConsumerRebalanceListener?

17. What methods are available in ConsumerRebalanceListener?

18. How can unnecessary Rebalancing be reduced?

19. Why is Cooperative Sticky Assignor preferred in production?

20. How does Kafka ensure fault tolerance during Consumer failures?

---

# 📚 Key Concepts Covered

- Consumer Rebalancing
- Consumer Join
- Consumer Leave
- Consumer Crash
- Group Coordinator
- Heartbeats
- Session Timeout
- Max Poll Interval
- Partition Reassignment
- Range Assignor
- Round Robin Assignor
- Sticky Assignor
- Cooperative Sticky Assignor
- Static Membership
- Rebalance Listener
- Eager Rebalancing
- Cooperative Rebalancing

---

# 📖 Next Chapter

## ➜ Chapter 11 - Serialization & Deserialization

In the next chapter, we will learn:

- What is Serialization?
- What is Deserialization?
- String Serializer
- JSON Serializer
- String Deserializer
- JSON Deserializer
- Java Object Serialization
- Sending POJOs using Spring Kafka
- Custom Object Mapping
- Best Practices

---

# 🎯 Summary

Consumer Rebalancing is one of Kafka's most important mechanisms for achieving scalability, fault tolerance, and high availability.

Whenever the Consumer Group changes, Kafka automatically redistributes partitions among Consumers using the Group Coordinator.

Modern Kafka minimizes downtime using Cooperative Sticky Assignor, Heartbeats, Session Timeout, Static Membership, and intelligent partition assignment strategies.

Understanding Consumer Rebalancing is essential before moving to Serialization, Transactions, Retry Mechanisms, and Production Kafka Architectures.