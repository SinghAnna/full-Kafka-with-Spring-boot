# Chapter 10 - Kafka Consumer Rebalancing

## 📌 Overview

In the previous chapter, we learned how Kafka tracks message consumption using **Offsets** and how Consumers commit those offsets either **automatically** or **manually**.

In this chapter, we will explore one of Kafka's most important concepts: **Consumer Rebalancing**.

Whenever a Consumer joins a Consumer Group, leaves the group, crashes unexpectedly, or when the number of partitions changes, Kafka automatically redistributes partitions among the available Consumers. This process is known as **Consumer Rebalancing**.

Consumer Rebalancing ensures that:

- Every partition is assigned to exactly one Consumer within a Consumer Group.
- Workload is evenly distributed among Consumers.
- Applications can scale horizontally.
- Consumer failures are handled automatically.
- Message processing continues with minimal interruption.

Understanding Consumer Rebalancing is essential for building highly available, fault-tolerant, and scalable Kafka applications.

---

# 🎯 Learning Objectives

After completing this chapter, you will be able to:

- Understand what Consumer Rebalancing is.
- Explain why Kafka performs Rebalancing.
- Understand the role of the Group Coordinator.
- Learn how Consumers join a Consumer Group.
- Learn how Consumers leave a Consumer Group.
- Understand Consumer Crash scenarios.
- Learn how Kafka reassigns partitions.
- Understand Heartbeats.
- Learn Session Timeout.
- Understand Max Poll Interval.
- Learn different Partition Assignment Strategies.
- Understand Rebalance Listeners.
- Compare Eager and Cooperative Rebalancing.
- Observe Rebalancing using Spring Boot.

---

# 🧠 Prerequisites

Before starting this chapter, you should already understand:

- Kafka Basics
- Kafka Topics
- Kafka Partitions
- Kafka Producers
- Kafka Consumers
- Consumer Groups
- Message Keys
- Kafka Offsets
- Spring Boot Basics
- Docker

---

# ❓ What is Consumer Rebalancing?

Consumer Rebalancing is the process by which Kafka automatically redistributes topic partitions among Consumers belonging to the same Consumer Group.

Whenever the Consumer Group changes, Kafka pauses message consumption, calculates a new partition assignment, assigns partitions to Consumers, and then resumes message consumption.

Rebalancing is completely automatic and managed by Kafka.

---

# 📖 Definition

> **Consumer Rebalancing** is the automatic redistribution of topic partitions among Consumers within the same Consumer Group whenever the membership of the Consumer Group or the partition count changes.

---

# 🤔 Why Consumer Rebalancing is Needed?

Imagine an application with only one Consumer.

```text
Producer
    │
    ▼
Kafka Topic
    │
    ▼
Consumer-1
```

Suppose the Topic has four partitions.

```text
Partition-0
Partition-1
Partition-2
Partition-3
```

Consumer-1 has to process all partitions.

Problems:

- Slow message processing
- High CPU utilization
- Poor scalability
- Single point of failure

Now another Consumer joins the same Consumer Group.

```text
Consumer-1

Consumer-2
```

Kafka automatically redistributes the partitions.

```text
Consumer-1
│
├── Partition-0
└── Partition-1

Consumer-2
│
├── Partition-2
└── Partition-3
```

Now both Consumers work simultaneously.

Benefits:

- Faster processing
- Better scalability
- Load balancing
- High availability
- Fault tolerance

---

# 🏗 Why Kafka Performs Rebalancing

Kafka performs Rebalancing whenever it detects a change in the Consumer Group.

Its goals are:

- Balance the workload
- Prevent duplicate processing
- Utilize all Consumers efficiently
- Recover from Consumer failures
- Support horizontal scaling

Without Rebalancing, some Consumers would remain idle while others would become overloaded.

---

# 🏛 Consumer Group Architecture

```text
                 Producer
                     │
                     ▼
               Kafka Broker
                     │
                     ▼
                first-topic
                     │
      ┌──────────────┼──────────────┐
      │              │              │
Partition-0     Partition-1    Partition-2
      │              │              │
      └──────────────┼──────────────┘
                     │
              Consumer Group
                     │
        ┌────────────┴────────────┐
        │                         │
   Consumer-1                Consumer-2
```

Kafka guarantees that:

- One partition belongs to only one Consumer inside a Consumer Group.
- A Consumer may own multiple partitions.
- Two Consumers in the same group never consume the same partition simultaneously.

---

# ⚙ Internal Working of Consumer Rebalancing

Kafka follows these steps during Rebalancing.

### Step 1

Consumers join a Consumer Group.

↓

### Step 2

Kafka elects a **Group Coordinator**.

↓

### Step 3

The Group Coordinator temporarily pauses all Consumers.

↓

### Step 4

Current partition ownership is revoked.

↓

### Step 5

Kafka calculates a new partition assignment.

↓

### Step 6

Partitions are assigned to Consumers.

↓

### Step 7

Consumers receive their assigned partitions.

↓

### Step 8

Consumers resume consuming messages.

---

# 🧩 What is a Group Coordinator?

Every Consumer Group has one Kafka Broker acting as its **Group Coordinator**.

The Group Coordinator manages the entire lifecycle of the Consumer Group.

Its responsibilities include:

- Managing Consumer membership
- Receiving Heartbeats
- Detecting Consumer failures
- Triggering Rebalancing
- Assigning partitions
- Tracking committed offsets

Without the Group Coordinator, Consumer Groups cannot function.

---

# 🏗 Group Coordinator Architecture

```text
              Kafka Cluster

        ┌───────────────┐
        │   Broker-1    │
        └───────────────┘

        ┌───────────────┐
        │   Broker-2    │
        └───────────────┘

        ┌───────────────┐
        │   Broker-3    │
        └───────────────┘
                 │
                 ▼
        Group Coordinator
                 │
        Consumer Group
                 │
      ┌──────────┴──────────┐
      │                     │
Consumer-1             Consumer-2
```

Each Consumer Group has only **one active Group Coordinator**.

Different Consumer Groups may have different Group Coordinators.

---

# 🚀 Consumer Join

Suppose only one Consumer is running.

```text
Consumer-1
```

It owns all partitions.

```text
Partition-0
Partition-1
Partition-2
Partition-3
```

Now Consumer-2 starts.

```text
Consumer-2
```

Kafka immediately detects that a new Consumer has joined the Consumer Group.

The Group Coordinator triggers Rebalancing.

After Rebalancing,

```text
Consumer-1
│
├── Partition-0
└── Partition-1

Consumer-2
│
├── Partition-2
└── Partition-3
```

The workload is now distributed evenly.

---

# 🔄 Consumer Join Workflow

```text
Consumer Starts

        │
        ▼

Joins Consumer Group

        │
        ▼

Group Coordinator Detects Join

        │
        ▼

Pause Existing Consumers

        │
        ▼

Recalculate Partition Assignment

        │
        ▼

Assign Partitions

        │
        ▼

Resume Consumption
```

---

# 🚪 Consumer Leave

Consumers may leave a Consumer Group gracefully.

Example:

```text
Consumer-1

Consumer-2
```

Suppose Consumer-2 shuts down normally.

Kafka immediately detects that Consumer-2 has left the Consumer Group.

The Group Coordinator starts Rebalancing.

The remaining Consumer receives all partitions.

```text
Consumer-1

├── Partition-0
├── Partition-1
├── Partition-2
└── Partition-3
```

Consumption continues automatically without any manual intervention.

---

# 🔄 Consumer Leave Workflow

```text
Consumer Shutdown

        │
        ▼

Leave Group Request

        │
        ▼

Coordinator Removes Consumer

        │
        ▼

Revoke Current Partitions

        │
        ▼

Assign Partitions to Remaining Consumers

        │
        ▼

Resume Consumption
```

---

# 📌 Key Points

- Consumer Rebalancing is fully automatic.
- Only Consumers belonging to the same Consumer Group participate in Rebalancing.
- Kafka temporarily pauses Consumers during Rebalancing.
- Every partition belongs to exactly one Consumer within the same Consumer Group.
- The Group Coordinator manages the complete Rebalancing process.
- Consumer Join and Consumer Leave are the two most common Rebalancing triggers.
- Rebalancing improves scalability, availability, and fault tolerance.

---

# 📖 Next Part

In **Part 2**, we will learn:

- Consumer Crash
- Heartbeats
- Session Timeout
- Max Poll Interval
- Partition Reassignment
- Rebalance Workflow
- Assignment Strategies (Range, Round Robin, Sticky, Cooperative Sticky)

These concepts explain **how Kafka detects Consumer failures and intelligently redistributes partitions across the Consumer Group.**