# JsonSerializer & JsonDeserializer

In modern Spring Boot applications, messages are usually exchanged as **JSON** instead of plain Strings.

For example, instead of sending:

```text
Employee Joined
```

we send

```json
{
  "id":101,
  "name":"Anant",
  "department":"Engineering",
  "salary":85000
}
```

Spring Kafka automatically converts Java Objects into JSON using **JsonSerializer** and converts JSON back into Java Objects using **JsonDeserializer**.

---

# Why JSON?

JSON is the most widely used message format because it is:

- Human Readable
- Lightweight
- Language Independent
- Easy to Debug
- Easy to Extend
- Supported by almost every programming language

Because of this,

A Java Producer can send JSON.

A Python Consumer can read it.

A NodeJS Consumer can read it.

A Go Consumer can read it.

A .NET Consumer can read it.

---

# Traditional String Message

Producer

```java
kafkaTemplate.send(
        "employee-topic",
        "Hello Kafka"
);
```

Consumer receives

```
Hello Kafka
```

Only plain text can be transferred.

---

# JSON Message

Producer

```java
Employee employee =
        new Employee(
                101,
                "Anant",
                "Developer",
                85000
        );

kafkaTemplate.send(
        "employee-topic",
        employee
);
```

Internally

```
Employee Object

        │

        ▼

JsonSerializer

        │

        ▼

JSON

        │

        ▼

Byte[]

        │

        ▼

Kafka Broker
```

---

# JSON Stored Inside Kafka

Kafka never stores Java Objects.

Internally it stores JSON as Bytes.

Example

```json
{
    "id":101,
    "name":"Anant",
    "designation":"Developer",
    "salary":85000
}
```

Internally

```
JSON

↓

Byte[]

↓

Kafka Topic
```

---

# JsonSerializer Workflow

```
Java Object

        │

        ▼

Jackson ObjectMapper

        │

        ▼

JSON

        │

        ▼

UTF-8 Bytes

        │

        ▼

Kafka Broker
```

---

# JsonDeserializer Workflow

```
Kafka Broker

        │

        ▼

UTF-8 Bytes

        │

        ▼

JSON

        │

        ▼

Jackson ObjectMapper

        │

        ▼

Java Object
```

---

# Jackson ObjectMapper

Spring Kafka internally uses the **Jackson** library.

```
Employee

↓

ObjectMapper

↓

JSON
```

Reverse Process

```
JSON

↓

ObjectMapper

↓

Employee
```

You usually don't need to call ObjectMapper manually.

Spring Boot does it automatically.

---

# Producer Configuration

Spring Boot Producer

```properties
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer

spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer
```

Workflow

```
Employee

↓

JsonSerializer

↓

JSON

↓

Kafka
```

---

# Consumer Configuration

```properties
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer

spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer
```

---

# Trusted Packages

JsonDeserializer blocks unknown classes for security.

Therefore,

Spring requires trusted packages.

Example

```properties
spring.kafka.consumer.properties.spring.json.trusted.packages=edu.anant.dto
```

Trust all packages

```properties
spring.kafka.consumer.properties.spring.json.trusted.packages=*
```

Recommended only for learning.

Production applications should trust only specific packages.

---

# Type Headers

Spring Kafka automatically sends type information.

Example

```
__TypeId__

↓

edu.anant.dto.Employee
```

Consumer uses this header to determine which Java class should be created.

---

# Disable Type Headers

Sometimes multiple applications use different package names.

Producer

```properties
spring.kafka.producer.properties.spring.json.add.type.headers=false
```

Consumer

```properties
spring.kafka.consumer.properties.spring.json.use.type.headers=false
```

Then configure

```properties
spring.kafka.consumer.properties.spring.json.value.default.type=edu.anant.dto.Employee
```

---

# Complete Serialization Process

```
Employee Object

↓

JsonSerializer

↓

Jackson

↓

JSON

↓

UTF-8 Bytes

↓

Kafka Producer

↓

Kafka Topic
```

---

# Complete Deserialization Process

```
Kafka Topic

↓

UTF-8 Bytes

↓

JSON

↓

Jackson

↓

JsonDeserializer

↓

Employee Object

↓

Consumer
```

---

# Advantages of JSON

✅ Human Readable

✅ Easy Debugging

✅ Cross Platform

✅ Easy Integration

✅ Language Independent

✅ REST Compatible

---

# Limitations of JSON

❌ Larger than Avro

❌ Larger than Protobuf

❌ Slower Serialization

❌ More Network Bandwidth

❌ More Storage

---

# When Should You Use JSON?

Use JSON when:

- Spring Boot Microservices
- REST APIs
- Event Driven Architecture
- Employee Events
- Order Events
- Payment Events
- Notification Events
- Inventory Events

---

# Common Mistakes

❌ Missing JsonSerializer

❌ Missing JsonDeserializer

❌ Forgetting trusted.packages

❌ Producer sends String while Consumer expects Employee

❌ Consumer expects Order while Producer sends Employee

❌ Different DTO structure on Producer and Consumer

---

# Production Best Practices

✅ Use DTOs instead of Entities.

✅ Version your JSON schema.

✅ Trust only required packages.

✅ Keep JSON objects small.

✅ Validate incoming messages.

✅ Log serialization failures.

✅ Prefer immutable DTOs (Records/Lombok).

---

# Summary

| Component | Responsibility |
|-----------|----------------|
| JsonSerializer | Java Object → JSON |
| JsonDeserializer | JSON → Java Object |
| Jackson | Object ↔ JSON Conversion |
| Trusted Packages | Security |
| Type Headers | Class Information |
| UTF-8 | Byte Encoding |
| Kafka | Stores Only Bytes |

---

# Next Section

➡️ **05-Serialization-Errors-and-Best-Practices.md**

In the next section, we'll cover:

- SerializationException
- DeserializationException
- ClassNotFoundException
- InvalidDefinitionException
- Trusted Package Errors
- Version Compatibility
- Debugging Serialization Problems
- Production Best Practices