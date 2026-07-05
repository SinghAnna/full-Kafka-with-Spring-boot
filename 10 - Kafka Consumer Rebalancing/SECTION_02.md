# Consumer Crash

A Consumer does not always leave the Consumer Group gracefully.

Sometimes a Consumer may stop unexpectedly due to various reasons such as:

- Application Crash
- JVM Crash
- Server Failure
- Network Failure
- Power Failure
- Kubernetes Pod Restart
- Machine Shutdown

In these situations, Kafka does not immediately know that the Consumer has failed.

Instead, Kafka waits for a configurable amount of time before declaring the Consumer dead.

Once Kafka determines that the Consumer has crashed, it automatically starts the **Consumer Rebalancing** process.

---

## Consumer Crash Example

Initially two Consumers are consuming messages.

```text
                Consumer Group

        ┌─────────────────────────┐
        │                         │
 Consumer-1                 Consumer-2
      │                           │
Partition-0                Partition-1
Partition-2                Partition-3
```

Suppose Consumer-2 crashes unexpectedly.

```text
                Consumer Group

        ┌─────────────────────────┐
        │                         │
 Consumer-1                 ❌ Consumer-2
```

Kafka detects that Consumer-2 is no longer alive.

The Group Coordinator starts Rebalancing.

After Rebalancing,

```text
                Consumer Group

            Consumer-1

      Partition-0
      Partition-1
      Partition-2
      Partition-3
```

Consumer-1 now owns every partition.

No manual intervention is required.

---

# ❤️ Heartbeats

Kafka uses **Heartbeats** to determine whether a Consumer is still alive.

A Heartbeat is a small request periodically sent by every Consumer to the Group Coordinator.

As long as Heartbeats are received,

Kafka assumes the Consumer is healthy.

If Heartbeats stop arriving,

Kafka assumes the Consumer has crashed.

---

## Heartbeat Workflow

```text
Consumer

     │

Heartbeat

     │

Group Coordinator

     │

Heartbeat

     │

Consumer

     │

Heartbeat

     │

Group Coordinator
```

Heartbeats continue throughout the Consumer's lifetime.

---

## Why Heartbeats are Important

Without Heartbeats,

Kafka would never know whether a Consumer is still alive.

Heartbeats allow Kafka to:

- Detect crashed Consumers
- Remove inactive Consumers
- Trigger Rebalancing
- Maintain fault tolerance

---

# ⏳ Session Timeout

Heartbeats are controlled by the **Session Timeout**.

Kafka waits for Heartbeats until the Session Timeout expires.

If no Heartbeat is received before the timeout,

Kafka removes the Consumer from the Consumer Group.

---

## Example

```properties
session.timeout.ms=10000
```

This means

Kafka waits **10 seconds**.

If no Heartbeat arrives within 10 seconds,

Consumer is declared dead.

Rebalancing begins.

---

## Timeline

```text
Heartbeat Received

        │

        ▼

0 sec

        │

5 sec

        │

8 sec

        │

10 sec

        ▼

No Heartbeat

        ▼

Consumer Removed

        ▼

Rebalancing Starts
```

---

# ⏱ Heartbeat Interval

Heartbeats are not sent every second randomly.

Kafka sends them after a fixed interval.

Example

```properties
heartbeat.interval.ms=3000
```

Meaning

A Heartbeat is sent every **3 seconds**.

Example timeline

```text
0 sec

Heartbeat

↓

3 sec

Heartbeat

↓

6 sec

Heartbeat

↓

9 sec

Heartbeat
```

A common recommendation is:

```text
heartbeat.interval.ms

≈

session.timeout.ms / 3
```

This gives Kafka enough opportunities to detect Consumer failures quickly.

---

# ⌛ Max Poll Interval

Consumers continuously poll Kafka for new messages.

If a Consumer takes too long to process records and does not call `poll()` again,

Kafka assumes the Consumer is stuck.

Even though Heartbeats may still be sent,

Kafka removes the Consumer from the Consumer Group.

---

## Example

```properties
max.poll.interval.ms=300000
```

Meaning

Maximum processing time allowed:

**5 Minutes**

If processing exceeds five minutes,

Kafka starts Rebalancing.

---

## Example Scenario

Consumer receives

```text
5000 Messages
```

Processing starts.

Suppose processing takes

```text
8 Minutes
```

But

```properties
max.poll.interval.ms=300000
```

Kafka assumes the Consumer has become unresponsive.

Consumer is removed.

Partitions are reassigned.

---

# Heartbeat vs Session Timeout vs Max Poll Interval

| Property | Purpose |
|----------|---------|
| heartbeat.interval.ms | How often Heartbeats are sent |
| session.timeout.ms | Maximum time Kafka waits for Heartbeats |
| max.poll.interval.ms | Maximum processing time before next poll |

---

# 🔄 Complete Failure Detection Workflow

```text
Consumer Starts

        │

Sends Heartbeats

        │

Consumes Messages

        │

Calls poll()

        │

Everything Healthy

──────────────────────────────

Consumer Crashes

        │

Heartbeats Stop

        │

Session Timeout Expires

        │

Group Coordinator Detects Failure

        │

Rebalance Starts

        │

Partitions Reassigned

        │

Consumers Resume Processing
```

---

# 📌 Important Notes

- Consumer crashes automatically trigger Rebalancing.
- Heartbeats tell Kafka that a Consumer is alive.
- Session Timeout determines how long Kafka waits for Heartbeats.
- Max Poll Interval ensures Consumers are actively processing records.
- If Heartbeats stop, Kafka removes the Consumer.
- If `poll()` is not called within the configured interval, Kafka also removes the Consumer.
- These mechanisms help Kafka achieve high availability and fault tolerance.

---

# 📖 Next Part

In **Part 3**, we will learn:

- Partition Reassignment
- Assignment Strategies
    - Range Assignor
    - Round Robin Assignor
    - Sticky Assignor
    - Cooperative Sticky Assignor
- Rebalance Listener
- Static Membership
- Eager vs Cooperative Rebalancing
- Practical Rebalancing Examples
- Spring Boot Demonstration