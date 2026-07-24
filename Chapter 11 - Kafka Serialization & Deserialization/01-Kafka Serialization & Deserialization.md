# Chapter 11 - Kafka Serialization & Deserialization

Serialization and Deserialization are among the most important concepts in Apache Kafka.

Kafka does **not understand Java Objects**.

It only stores and transfers **Bytes**.

Before sending data to Kafka, the Producer converts Java Objects into Bytes (**Serialization**).

When the Consumer receives those Bytes, it converts them back into Java Objects (**Deserialization**).

Without Serialization and Deserialization, Kafka cannot transmit Java Objects between applications.

---

# Learning Objectives

After completing this chapter, you will be able to:

- Understand Serialization
- Understand Deserialization
- Why Kafka stores Bytes
- Producer Serialization Workflow
- Consumer Deserialization Workflow
- StringSerializer
- StringDeserializer
- JsonSerializer
- JsonDeserializer
- ByteArraySerializer
- ByteArrayDeserializer
- Spring Boot Configuration
- Send Java Objects
- Receive Java Objects
- Trusted Packages
- Type Headers
- Common Serialization Errors
- Production Best Practices

---

# What is Serialization?

Serialization is the process of converting a Java Object into a sequence of Bytes so that it can be transferred over the network or stored inside Kafka.

```
Java Object

        │

        ▼

Serializer

        │

        ▼

Byte[]

        │

        ▼

Kafka Broker
```

Without Serialization, Kafka cannot send Java Objects.

---

# What is Deserialization?

Deserialization is the reverse process.

It converts the received Byte Array back into the original Java Object.

```
Kafka Broker

        │

        ▼

Byte[]

        │

        ▼

Deserializer

        │

        ▼

Java Object
```

Without Deserialization, the Consumer would receive only binary data.

---

# Why is Serialization Needed?

Suppose we have an Employee object.

```java
Employee employee = new Employee(
        101,
        "Anant",
        "Software Engineer",
        85000
);
```

Can Kafka send this object directly?

**No.**

Kafka stores only Bytes.

It does not know:

- Employee
- Order
- Product
- Payment
- Customer

Kafka understands only:

```
Byte[]
```

Therefore every Java Object must first be converted into Bytes.

---

# Real World Analogy

Imagine sending a gift through a courier.

```
Gift

↓

Packing

↓

Courier

↓

Delivery

↓

Unpacking
```

Kafka works in exactly the same way.

```
Java Object

↓

Serialization

↓

Kafka Broker

↓

Deserialization

↓

Java Object
```

The courier never understands the gift.

Kafka never understands Java Objects.

It only transfers Bytes.

---

# Why Kafka Uses Bytes?

Bytes are:

- Language Independent
- Fast
- Compact
- Easy to Store
- Easy to Transfer
- Platform Independent

Because of this,

A Java Producer can send data.

A Python Consumer can read it.

A Go Consumer can read it.

A NodeJS Consumer can read it.

As long as they use the same serialization format.

---

# Serialization Workflow

```
Employee Object

        │

        ▼

JsonSerializer

        │

        ▼

Byte[]

        │

        ▼

Producer

        │

        ▼

Kafka Broker
```

---

# Deserialization Workflow

```
Kafka Broker

        │

        ▼

Byte[]

        │

        ▼

JsonDeserializer

        │

        ▼

Employee Object

        │

        ▼

Consumer
```

---

# Complete End-to-End Flow

```
Producer

↓

Java Object

↓

Serializer

↓

Byte[]

↓

Kafka Topic

↓

Byte[]

↓

Deserializer

↓

Java Object

↓

Consumer
```

---

# Key Takeaways

✅ Kafka never stores Java Objects.

✅ Kafka stores only Bytes.

✅ Producer performs Serialization.

✅ Consumer performs Deserialization.

✅ Every Kafka message is ultimately stored as a Byte Array.

---

# Next Section

➡️ **02-Kafka-Message-Format.md**

- Kafka Message Structure
- Producer Record
- Consumer Record
- Key
- Value
- Headers
- Timestamp
- Offset
- Partition
- Record Metadata