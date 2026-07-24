# File 8 — application.properties

```properties
#################################################
# Application
#################################################

spring.application.name=kafka-spring-boot

#################################################
# Kafka Bootstrap Server
#################################################

spring.kafka.bootstrap-servers=localhost:9093

#################################################
# Producer Configuration
#################################################

spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer

spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer

#################################################
# Consumer Configuration
#################################################

spring.kafka.consumer.group-id=employee-group

spring.kafka.consumer.auto-offset-reset=earliest

spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer

spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer

#################################################
# JsonDeserializer Configuration
#################################################

spring.kafka.consumer.properties.spring.json.trusted.packages=edu.anant.dto

spring.kafka.consumer.properties.spring.json.value.default.type=edu.anant.dto.Employee

spring.kafka.consumer.properties.spring.json.use.type.headers=false
```

---

# How Spring Boot Uses These Properties

```
application.properties

        │

        ▼

Kafka Auto Configuration

        │

        ▼

Producer

        │

        ▼

Consumer

        │

        ▼

Kafka Broker
```

---

# Application Name

```properties
spring.application.name=kafka-spring-boot
```

Only the Spring Boot application name.

Useful for

- Logging
- Monitoring
- Spring Actuator

---

# Bootstrap Server

```properties
spring.kafka.bootstrap-servers=localhost:9093
```

This tells both Producer and Consumer

```
Kafka Broker

↓

localhost:9093
```

Without this property

```
Producer ❌

Consumer ❌
```

cannot connect.

---

# Producer Key Serializer

```properties
spring.kafka.producer.key-serializer=
org.apache.kafka.common.serialization.StringSerializer
```

Converts

```
String Key

↓

Bytes
```

Example

```
emp-101

↓

Byte[]
```

---

# Producer Value Serializer

```properties
spring.kafka.producer.value-serializer=
org.springframework.kafka.support.serializer.JsonSerializer
```

Converts

```
Employee Object

↓

JSON

↓

UTF-8 Bytes
```

Example

```java
Employee
```

↓

```json
{
  "id":101,
  "firstName":"Anant"
}
```

↓

Bytes

↓

Kafka

---

# Consumer Group

```properties
spring.kafka.consumer.group-id=employee-group
```

All Consumers with this group id belong to

```
employee-group
```

Kafka distributes messages among them.

---

# Auto Offset Reset

```properties
spring.kafka.consumer.auto-offset-reset=earliest
```

If no committed offset exists

Consumer starts from

```
Beginning

↓

Offset 0
```

If

```properties
latest
```

Consumer reads only new messages.

---

# Key Deserializer

```properties
spring.kafka.consumer.key-deserializer=
org.apache.kafka.common.serialization.StringDeserializer
```

Converts

```
Byte[]

↓

String
```

---

# Value Deserializer

```properties
spring.kafka.consumer.value-deserializer=
org.springframework.kafka.support.serializer.JsonDeserializer
```

Converts

```
Byte[]

↓

JSON

↓

Employee Object
```

---

# Trusted Packages

```properties
spring.kafka.consumer.properties.spring.json.trusted.packages=
edu.anant.dto
```

Allows JsonDeserializer to create classes only from

```
edu.anant.dto
```

Good Practice

```
edu.anant.dto
```

Bad Practice

```
*
```

Never use `*` in production.

---

# Default Value Type

```properties
spring.kafka.consumer.properties.spring.json.value.default.type=
edu.anant.dto.Employee
```

Whenever JSON arrives

Spring creates

```
Employee
```

automatically.

No ObjectMapper needed.

---

# Type Headers

```properties
spring.kafka.consumer.properties.spring.json.use.type.headers=false
```

Ignore Kafka Type Headers.

Always deserialize into

```
Employee
```

instead.

Useful when

- Java Producer
- Python Consumer

or

- Spring Boot Producer
- Node.js Consumer

---

# Complete Serialization Flow

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

UTF-8 Bytes

        │

        ▼

Kafka Broker

        │

        ▼

JsonDeserializer

        │

        ▼

Employee Object
```

---

# Spring Boot Startup Flow

```
application.properties

        │

        ▼

Kafka Auto Configuration

        │

        ▼

ProducerFactory

        │

        ▼

KafkaTemplate

        │

        ▼

ConsumerFactory

        │

        ▼

KafkaListenerContainer

        │

        ▼

@KafkaListener
```

---

# Best Practices

✅ Keep broker address in `application.properties`.

✅ Use `JsonSerializer` for object publishing.

✅ Use `JsonDeserializer` for object consumption.

✅ Trust only required packages.

✅ Use `earliest` during development.

✅ Use `latest` in most production applications.

---

# Summary

| Property | Purpose |
|----------|----------|
| `spring.kafka.bootstrap-servers` | Kafka Broker Address |
| `producer.key-serializer` | String → Bytes |
| `producer.value-serializer` | Employee → JSON |
| `consumer.group-id` | Consumer Group |
| `consumer.auto-offset-reset` | Starting Offset |
| `consumer.key-deserializer` | Bytes → String |
| `consumer.value-deserializer` | JSON → Employee |
| `trusted.packages` | Security |
| `value.default.type` | Default DTO |
| `use.type.headers` | Ignore Kafka Type Headers |

---

# Project Structure

```
src
│
├── config
│      ├── KafkaJsonTopicConfig.java
│      ├── KafkaJsonProducerConfig.java
│      └── KafkaJsonConsumerConfig.java
│
├── controller
│      └── KafkaJsonController.java
│
├── dto
│      └── Employee.java
│
├── service
│      ├── KafkaJsonProducerService.java
│      └── KafkaJsonConsumerService.java
│
└── resources
       └── application.properties
```

---

# ✅ Chapter 11 Implementation Completed

You now have a complete JSON Kafka application with:

- ✅ Employee DTO
- ✅ Topic Configuration
- ✅ Producer Configuration
- ✅ Consumer Configuration
- ✅ Producer Service
- ✅ Consumer Service
- ✅ REST Controller
- ✅ `application.properties`

---

# Next Chapter

➡️ **Chapter 12 — Kafka Consumer Groups (Deep Dive)**

We'll cover:

- Consumer Groups
- Group Coordinator
- Partition Assignment Strategies
- Range Assignor
- Round Robin Assignor
- Sticky Assignor
- Cooperative Sticky Assignor
- Load Balancing
- Scaling Consumers
- Production Demo with Multiple Consumers
```