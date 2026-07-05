# Consumer Position & Offset Commit

After understanding what an Offset is, the next question is:

> **How does Kafka know where a Consumer should continue reading after a restart?**

The answer is **Offset Commit**.

Kafka stores the reading progress of every Consumer Group so that Consumers can resume processing without losing or duplicating messages.

---

# 🧠 Consumer Position

A Consumer continuously reads messages from a Kafka Topic.

As it reads messages, it moves forward one Offset at a time.

Example

```
Partition-0

Offset 0 → Order Created

Offset 1 → Payment Success

Offset 2 → Inventory Updated

Offset 3 → Packed

Offset 4 → Delivered
```

Suppose the Consumer has already processed:

```
Offset 0

↓

Offset 1

↓

Offset 2
```

Its current reading position becomes:

```
Next Offset = 3
```

This position is called the **Consumer Position**.

---

# 📍 What is Consumer Position?

Consumer Position represents the **next Offset that the Consumer will read**.

For example,

```
Offsets

0
1
2
3
4
5
```

Consumer already processed

```
0

1

2
```

Current Position

```
3
```

Kafka always knows:

> "The next message should be read from Offset 3."

---

# 📌 Current Offset

The **Current Offset** is simply the Offset that the Consumer is currently processing.

Example

```
Partition-0

Offset 0

Offset 1

Offset 2  ← Current Offset

Offset 3

Offset 4
```

After processing Offset 2,

the Consumer moves to Offset 3.

---

# ✅ Committed Offset

Processing a message does **not** automatically mean Kafka remembers it forever.

Kafka only remembers the progress when the Consumer **commits** the Offset.

Example

```
Processed

Offset 0

↓

Offset 1

↓

Offset 2
```

Consumer commits

```
Committed Offset = 2
```

Now Kafka permanently remembers:

```
This Consumer Group has processed everything up to Offset 2.
```

If the Consumer crashes,

Kafka resumes from

```
Offset 3
```

---

# 🎯 Why Commit Offsets?

Imagine this scenario.

Consumer processes

```
Offset 0

↓

Offset 1

↓

Offset 2
```

Suddenly,

```
Application Crash
```

If Offset 2 was committed,

Kafka resumes from

```
Offset 3
```

No duplicate processing.

---

If Offset 2 was **not committed**,

Kafka may restart from

```
Offset 2
```

which means

```
Offset 2
```

gets processed again.

This behavior provides reliability.

---

# 🔄 Offset Commit Workflow

```
Producer

      │

      ▼

Kafka Topic

      │

      ▼

Consumer Reads Message

      │

      ▼

Business Logic Executes

      │

      ▼

Offset Commit

      │

      ▼

Kafka Stores Progress

      │

      ▼

Consumer Reads Next Offset
```

Notice that

Kafka stores the Offset **after** processing.

---

# ⚙ Auto Offset Commit

Kafka can automatically commit Offsets at regular intervals.

Configuration

```properties
spring.kafka.consumer.enable-auto-commit=true
```

Commit Interval

```properties
spring.kafka.consumer.auto-commit-interval=5000
```

Meaning

```
Every 5 seconds

↓

Kafka commits latest processed Offset.
```

---

## Auto Commit Workflow

```
Consumer Reads

↓

Process Message

↓

Process Next Message

↓

Process Next Message

↓

Every 5 Seconds

↓

Kafka Commits Offset
```

---

### Advantages

- Very easy to configure
- Less code
- Suitable for beginners
- Good for simple applications

---

### Disadvantages

Imagine

```
Process Offset 100
```

Auto commit hasn't happened yet.

Application crashes.

Kafka restarts from

```
Offset 100
```

Message gets processed twice.

Duplicate processing is possible.

---

# ✍ Manual Offset Commit

Instead of Kafka deciding when to commit,

the application decides.

Workflow

```
Read Message

↓

Process Successfully

↓

Save Database

↓

Call Commit

↓

Kafka Stores Offset
```

Only after successful processing,

Offset is committed.

---

### Advantages

- Complete control
- Better reliability
- Prevents unnecessary commits
- Used in production systems

---

### Disadvantages

- More code
- Slightly more complex
- Requires proper error handling

---

# ⚖ Auto Commit vs Manual Commit

| Auto Commit | Manual Commit |
|-------------|---------------|
| Kafka commits automatically | Application commits |
| Less code | More code |
| Easy configuration | More control |
| Suitable for beginners | Suitable for production |
| Can cause duplicate processing | Better reliability |

---

# 🗂 Where are Offsets Stored?

Many beginners think Kafka stores Offsets inside the Topic.

This is **incorrect**.

Kafka stores committed Offsets in a special internal Topic called

```
__consumer_offsets
```

It is automatically created by Kafka.

You never create it manually.

---

# 🏗 Internal Architecture

```
                Kafka Broker

      ┌───────────────────────────┐
      │                           │
      │      first-topic          │
      │                           │
      └───────────────────────────┘

                  │

                  ▼

          Consumer Group

                  │

                  ▼

      __consumer_offsets

Stores

Group ID

Partition

Committed Offset
```

---

# 📝 What Does Kafka Store?

Kafka stores information similar to:

| Consumer Group | Topic | Partition | Committed Offset |
|----------------|-------|-----------|------------------|
| group-1 | first-topic | 0 | 120 |
| group-1 | first-topic | 1 | 118 |
| group-2 | first-topic | 0 | 96 |

Each Consumer Group maintains its own independent progress.

---

# 🔄 Consumer Restart Flow

Suppose

Consumer has processed

```
Offset 50
```

Kafka stores

```
Committed Offset = 50
```

Application crashes.

↓

Application starts again.

↓

Kafka checks

```
__consumer_offsets
```

↓

Finds

```
Committed Offset = 50
```

↓

Consumer resumes from

```
Offset 51
```

No need to start from the beginning.

---

# 👥 Multiple Consumer Groups

Suppose we have two Consumer Groups.

```
Group-1

Current Offset = 150
```

```
Group-2

Current Offset = 40
```

Both are reading the same Topic.

Kafka maintains separate committed Offsets.

```
Topic

↓

Group-1 → Offset 150

↓

Group-2 → Offset 40
```

This allows multiple applications to process the same data independently.

---

# 💡 Key Takeaways

- Consumer Position indicates the next Offset to read.
- Current Offset is the message currently being processed.
- Committed Offset is the last Offset safely stored by Kafka.
- Kafka stores committed Offsets inside the internal `__consumer_offsets` topic.
- Auto Commit is simple but less reliable.
- Manual Commit provides better control and is commonly used in production.
- Each Consumer Group maintains its own Offset independently.

---

## 📖 Next Section

In the next section, we will learn:

- Offset Reset Policies (`earliest`, `latest`, `none`)
- Replaying Messages
- Delivery Semantics
- At Most Once
- At Least Once
- Exactly Once (Introduction)
- Consumer Lag
- Real-World Examples