# Chapter 08 - Part 2
# Practical Demos, Testing & Verification

---

# 🎯 Objective

In this practical section, we will verify everything learned in the theory.

By the end of this chapter, you will be able to observe:

- Message distribution without Keys
- Message distribution with Keys
- Hash-Based Partitioning
- Ordering Guarantee
- Partition Assignment
- Kafka CLI Commands
- Producer Logs
- Consumer Logs

---

# 📚 Prerequisites

Before starting:

✅ Docker Running

✅ Kafka Running

✅ Spring Boot Running

✅ Topic Created

```
first-topic
```

with

```
3 Partitions
```

---

# Demo 1
## Send Messages WITHOUT Keys

API

```
POST

/api/messages/without-key
```

Request Body

```
Hello-1
```

Repeat

```
Hello-2

Hello-3

Hello-4

Hello-5

Hello-6

Hello-7

Hello-8

Hello-9

Hello-10
```

---

Expected Producer Output

```
Message Sent Successfully

Partition : 0
```

```
Message Sent Successfully

Partition : 1
```

```
Message Sent Successfully

Partition : 2
```

```
Message Sent Successfully

Partition : 0
```

```
Message Sent Successfully

Partition : 1
```

```
Message Sent Successfully

Partition : 2
```

Kafka distributes messages across available partitions.

This improves throughput.

---

# What to Observe

Notice that

```
Partition Number
```

changes frequently.

This happens because

```
Key = NULL
```

Kafka chooses partitions automatically.

---

# Demo 2
## Send Messages WITH Same Key

API

```
POST

/api/messages/with-key?key=Customer-101
```

Messages

```
Order Created

Payment Success

Inventory Updated

Packed

Shipped

Delivered
```

---

Expected Output

```
Partition : 2
```

```
Partition : 2
```

```
Partition : 2
```

```
Partition : 2
```

```
Partition : 2
```

```
Partition : 2
```

Every message goes to exactly the same partition.

---

# Why?

Kafka calculates

```
hash(Customer-101)

↓

Some Integer

↓

Modulo 3

↓

Partition 2
```

Since the Key never changes,

the partition never changes.

---

# Demo 3
## Send Messages with Different Keys

Request

```
Customer-101
```

```
Customer-102
```

```
Customer-103
```

```
Customer-104
```

```
Customer-105
```

Possible Output

```
Customer-101

Partition-0
```

```
Customer-102

Partition-2
```

```
Customer-103

Partition-1
```

```
Customer-104

Partition-0
```

```
Customer-105

Partition-2
```

Different Keys

↓

Different Hash Values

↓

Different Partitions

---

# Demo 4
## Ordering Guarantee

Send

```
Key

Order-1001
```

Messages

```
Order Created

↓

Payment Done

↓

Inventory Reserved

↓

Packed

↓

Shipped

↓

Delivered
```

Console

```
Partition

1
```

Every event

↓

Same Partition

↓

Consumer reads

```
Order Created

↓

Payment Done

↓

Inventory Reserved

↓

Packed

↓

Shipped

↓

Delivered
```

Ordering is preserved.

---

# Demo 5
## Send 1000 Messages

Loop

```java
for (int i = 1; i <= 1000; i++) {

    kafkaTemplate.send(
            "first-topic",
            "Customer-" + i,
            "Message " + i
    );

}
```

Observe

```
Producer Logs
```

Different customers

↓

Different partitions

---

# Demo 6
## Same Customer Multiple Orders

```
Customer-101
```

Send

```
Order-1

Order-2

Order-3

Order-4

Order-5

Order-6
```

Expected

```
Partition 0
```

for every message.

---

# Demo 7
## Kafka Topic Details

Describe Topic

```bash
docker exec -it kafka kafka-topics \
--bootstrap-server localhost:9092 \
--describe \
--topic first-topic
```

Output

```
Topic

first-topic
```

```
Partitions

3
```

```
Replication

1
```

---

# Demo 8
## Consume Messages

```bash
docker exec -it kafka kafka-console-consumer \
--bootstrap-server localhost:9092 \
--topic first-topic \
--from-beginning
```

Output

```
Hello Kafka

Order Created

Payment Success
```

---

# Demo 9
## Print Message Keys

```bash
docker exec -it kafka kafka-console-consumer \
--bootstrap-server localhost:9092 \
--topic first-topic \
--from-beginning \
--property print.key=true
```

Output

```
Customer-101

Order Created
```

```
Customer-101

Payment Success
```

```
Customer-102

Order Created
```

Now you can verify

Keys

and

Messages

together.

---

# Demo 10
## Observe Producer Logs

Example

```
=====================================

Topic      : first-topic

Partition  : 1

Offset      : 32

Timestamp   : 1751284216621

Key         : Customer-101

Message     : Payment Success

=====================================
```

Observe

✔ Topic

✔ Partition

✔ Offset

✔ Timestamp

✔ Key

✔ Message

---

# Screenshot Checklist

Capture these screenshots for GitHub.

## Screenshot 1

Topic Creation

---

## Screenshot 2

Producer Console

---

## Screenshot 3

Consumer Console

---

## Screenshot 4

Partition Numbers

---

## Screenshot 5

Kafka CLI

---

## Screenshot 6

print.key=true Output

---

## Screenshot 7

Postman Requests

---

# GitHub Folder Structure

```
Chapter-08-Message-Keys

│

├── README.md

├── screenshots

│      ├── producer.png

│      ├── consumer.png

│      ├── partitions.png

│      ├── kafka-cli.png

│      └── postman.png

│

└── source-code
```

---

# Common Mistakes

❌ Expecting ordering across different partitions

❌ Using random keys for ordered events

❌ Forgetting that NULL keys are distributed automatically

❌ Changing the partition count after data has been produced (can change key-to-partition mapping)

❌ Assuming different keys always map to different partitions (hash collisions are possible)

---

# Interview Questions

1. What is a Message Key?

2. Why are Message Keys important?

3. How does Kafka select a partition?

4. What happens if Key is NULL?

5. What is Hash-Based Partitioning?

6. Can two Keys go to the same partition?

7. Why does the same Key always reach the same partition?

8. Does Kafka guarantee ordering?

9. Is ordering guaranteed across partitions?

10. Which is better?

- With Keys
- Without Keys

Explain with examples.

---

# Summary

Congratulations!

You now understand one of Kafka's most important concepts.

You learned:

✅ Message Keys

✅ Key vs Value

✅ Hash-Based Partitioning

✅ Same Key → Same Partition

✅ Ordering Guarantee

✅ Producer Logs

✅ Kafka CLI

✅ Practical Testing

These concepts form the foundation for the next chapter:

➡ **Chapter 09 – Kafka Offsets**, where you'll learn how Kafka tracks every consumed message and how consumers resume processing reliably.