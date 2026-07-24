# Kafka Message Format

Every message stored in Kafka is called a **Record**.

A Kafka Record contains much more than just the message.

Internally, every record consists of multiple fields.

```
+------------------------------------------------------+
|                     Kafka Record                     |
+------------------------------------------------------+
| Key | Value | Topic | Partition | Offset | Timestamp |
+------------------------------------------------------+
| Headers | Checksum | Metadata                     |
+------------------------------------------------------+
```

Each field has its own purpose.

---

# Kafka Record Components

A Kafka Record consists of:

- Topic
- Partition
- Offset
- Key
- Value
- Timestamp
- Headers

Example

```
Topic       : order-topic

Partition   : 2

Offset      : 145

Key         : Order-101

Value       : {"id":101,"amount":5000}

Timestamp   : 1719820100101

Headers     : source=payment-service
```

---

# Topic

A Topic is a logical category where Kafka stores messages.

Example

```
order-topic

payment-topic

employee-topic

notification-topic
```

Think of a Topic as a folder.

```
Topics

├── employee-topic

├── payment-topic

├── order-topic

└── notification-topic
```

---

# Partition

Every Topic is divided into one or more Partitions.

```
employee-topic

├── Partition-0

├── Partition-1

├── Partition-2

└── Partition-3
```

A message is always stored inside exactly one partition.

---

# Offset

Every message inside a partition gets a unique Offset.

```
Partition-0

Offset 0

Offset 1

Offset 2

Offset 3

Offset 4

Offset 5
```

Offset identifies the exact position of a message.

Offsets are unique only within a partition.

---

# Key

The Key decides which partition stores the message.

Example

```
Key

↓

Hash Function

↓

Partition
```

Example

```
Key = Employee-101

↓

hash(Employee-101)

↓

Partition-2
```

If the same key is used again,

Kafka always chooses the same partition.

---

# Value

The Value is the actual business data.

Example

```json
{
   "id":101,
   "name":"Anant",
   "salary":85000
}
```

Usually, the Value is serialized into JSON.

```
Employee Object

↓

JsonSerializer

↓

JSON

↓

Bytes

↓

Kafka
```

---

# Timestamp

Kafka automatically stores the creation time of every message.

Example

```
Timestamp

↓

1719820100101
```

It is useful for:

- Auditing
- Monitoring
- Event Ordering
- Analytics

---

# Headers

Headers store additional metadata.

Example

```
source = payment-service

version = v1

content-type = application/json
```

Headers do not affect the message body.

They provide extra information.

---

# ProducerRecord

The Producer sends a message using a **ProducerRecord**.

```
Producer

↓

ProducerRecord

↓

Kafka Broker
```

Example

```java
ProducerRecord<String, Employee> record =
        new ProducerRecord<>(
                "employee-topic",
                "emp-101",
                employee
        );
```

ProducerRecord contains:

- Topic
- Key
- Value
- Timestamp
- Headers

---

# ConsumerRecord

The Consumer receives a **ConsumerRecord**.

```
Kafka Broker

↓

ConsumerRecord

↓

Consumer
```

Example

```java
@KafkaListener(topics = "employee-topic")
public void consume(
        ConsumerRecord<String, Employee> record
) {

}
```

A ConsumerRecord contains:

- Topic
- Partition
- Offset
- Timestamp
- Headers
- Key
- Value

---

# Complete Producer Workflow

```
Java Object

        │

        ▼

JsonSerializer

        │

        ▼

Byte[]

        │

        ▼

ProducerRecord

        │

        ▼

Kafka Producer

        │

        ▼

Kafka Broker

        │

        ▼

Topic

        │

        ▼

Partition
```

---

# Complete Consumer Workflow

```
Partition

        │

        ▼

Byte[]

        │

        ▼

ConsumerRecord

        │

        ▼

JsonDeserializer

        │

        ▼

Employee Object

        │

        ▼

Business Logic
```

---

# End-to-End Message Lifecycle

```
Employee Object

↓

JsonSerializer

↓

Byte[]

↓

ProducerRecord

↓

Kafka Producer

↓

Kafka Broker

↓

Topic

↓

Partition

↓

ConsumerRecord

↓

Byte[]

↓

JsonDeserializer

↓

Employee Object

↓

Consumer
```

---

# Important Notes

✅ Producer sends a ProducerRecord.

✅ Kafka stores Bytes only.

✅ Consumer receives a ConsumerRecord.

✅ Offset identifies the message position.

✅ Partition stores the message.

✅ Key decides the partition.

✅ Value contains the actual business data.

---

# Next Section

➡️ **03-Kafka-Serializers.md**

In the next section, we'll cover:

- StringSerializer
- StringDeserializer
- JsonSerializer
- JsonDeserializer
- ByteArraySerializer
- ByteArrayDeserializer
- Which serializer should be used in production?