# Kafka Serializers & Deserializers

Kafka itself does not know how to convert Java Objects into Bytes.

This responsibility is handled by **Serializer** and **Deserializer** classes.

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

Every Kafka Producer requires a **Serializer**.

Every Kafka Consumer requires a **Deserializer**.

---

# What is a Serializer?

A Serializer converts a Java Object into a Byte Array.

```
Java Object

↓

Serializer

↓

Byte[]
```

Producer uses a Serializer before sending the message.

Without a Serializer,

Kafka cannot send data.

---

# What is a Deserializer?

A Deserializer converts a Byte Array back into the original Java Object.

```
Byte[]

↓

Deserializer

↓

Java Object
```

Consumer uses a Deserializer after receiving the message.

Without a Deserializer,

the Consumer receives only binary data.

---

# Producer Side

```
Employee Object

↓

JsonSerializer

↓

Byte[]

↓

Kafka Producer

↓

Kafka Topic
```

---

# Consumer Side

```
Kafka Topic

↓

Byte[]

↓

JsonDeserializer

↓

Employee Object
```

---

# Common Kafka Serializers

Kafka provides several built-in serializers.

| Serializer | Purpose |
|------------|----------|
| StringSerializer | Converts String → Bytes |
| IntegerSerializer | Converts Integer → Bytes |
| LongSerializer | Converts Long → Bytes |
| DoubleSerializer | Converts Double → Bytes |
| ByteArraySerializer | Sends Raw Bytes |
| UUIDSerializer | Converts UUID → Bytes |
| JsonSerializer (Spring Kafka) | Converts Java Objects → JSON Bytes |

---

# Common Kafka Deserializers

| Deserializer | Purpose |
|--------------|----------|
| StringDeserializer | Bytes → String |
| IntegerDeserializer | Bytes → Integer |
| LongDeserializer | Bytes → Long |
| DoubleDeserializer | Bytes → Double |
| ByteArrayDeserializer | Bytes → Byte[] |
| JsonDeserializer (Spring Kafka) | JSON Bytes → Java Object |

---

# StringSerializer

Used when the message is a simple String.

Example

```java
KafkaTemplate<String, String>
```

Producer Configuration

```properties
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer

spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer
```

Workflow

```
"Hello Kafka"

↓

StringSerializer

↓

Byte[]

↓

Kafka
```

---

# StringDeserializer

Converts Bytes back into a String.

Consumer Configuration

```properties
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer

spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer
```

Workflow

```
Byte[]

↓

StringDeserializer

↓

"Hello Kafka"
```

---

# JsonSerializer

Used when sending Java Objects.

Example

```java
Employee employee =
        new Employee(
                101,
                "Anant",
                "Software Engineer",
                85000
        );
```

Producer

```
Employee Object

↓

JsonSerializer

↓

JSON

↓

Byte[]

↓

Kafka
```

Generated JSON

```json
{
  "id": 101,
  "name": "Anant",
  "designation": "Software Engineer",
  "salary": 85000
}
```

---

# JsonDeserializer

Converts JSON Bytes back into a Java Object.

Workflow

```
Byte[]

↓

JSON

↓

JsonDeserializer

↓

Employee Object
```

Consumer automatically receives

```java
Employee employee
```

instead of raw bytes.

---

# ByteArraySerializer

Sometimes applications need to send raw binary data.

Examples

- Images
- PDFs
- Audio Files
- Video Files
- Machine Learning Models
- Encrypted Data

Workflow

```
Image

↓

Byte[]

↓

ByteArraySerializer

↓

Kafka
```

---

# ByteArrayDeserializer

Used when the Consumer expects raw binary data.

```
Kafka

↓

Byte[]

↓

ByteArrayDeserializer

↓

Image
```

---

# Which Serializer Should You Use?

| Data Type | Recommended Serializer |
|-----------|------------------------|
| String | StringSerializer |
| Integer | IntegerSerializer |
| Long | LongSerializer |
| Double | DoubleSerializer |
| JSON Object | JsonSerializer |
| Binary Data | ByteArraySerializer |

---

# Spring Boot Example

Producer

```java
KafkaTemplate<String, Employee>
```

Consumer

```java
@KafkaListener(topics = "employee-topic")
public void consume(Employee employee){

}
```

Spring automatically performs

```
Employee

↓

JsonSerializer

↓

Kafka

↓

JsonDeserializer

↓

Employee
```

---

# Serialization Flow

```
Employee Object

↓

JsonSerializer

↓

JSON

↓

Byte[]

↓

Kafka Broker
```

---

# Deserialization Flow

```
Kafka Broker

↓

Byte[]

↓

JSON

↓

JsonDeserializer

↓

Employee Object
```

---

# Common Mistakes

❌ Using `StringSerializer` to send Java Objects.

❌ Using `StringDeserializer` to read JSON Objects.

❌ Forgetting to configure `JsonDeserializer`.

❌ Missing `trusted.packages`.

❌ Sending incompatible object types.

❌ Producer and Consumer using different serialization formats.

---

# Production Recommendations

✅ Use `StringSerializer` for plain text messages.

✅ Use `JsonSerializer` for Java Objects.

✅ Use `ByteArraySerializer` for binary data.

✅ Producer and Consumer must use compatible serializers.

✅ Always validate serialization before deploying.

---

# Summary

| Component | Responsibility |
|-----------|----------------|
| Serializer | Java Object → Bytes |
| Deserializer | Bytes → Java Object |
| StringSerializer | String → Bytes |
| JsonSerializer | Java Object → JSON Bytes |
| ByteArraySerializer | Binary Data → Bytes |
| StringDeserializer | Bytes → String |
| JsonDeserializer | JSON Bytes → Java Object |
| ByteArrayDeserializer | Bytes → Binary Data |

---

# Next Section

➡️ **04-JsonSerializer-and-JsonDeserializer.md**

We'll learn:

- JsonSerializer in Spring Boot
- JsonDeserializer in Spring Boot
- Jackson ObjectMapper
- Trusted Packages
- Type Headers
- Producer Configuration
- Consumer Configuration
- Complete Spring Boot JSON Example