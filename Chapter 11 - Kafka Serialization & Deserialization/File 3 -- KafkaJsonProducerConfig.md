# File 3 — KafkaJsonProducerConfig.java

```java
package edu.anant.config;

import edu.anant.dto.Employee;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaJsonProducerConfig {

    private final KafkaProperties kafkaProperties;

    public KafkaJsonProducerConfig(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Bean
    public ProducerFactory<String, Employee> producerFactory() {

        Map<String, Object> props = new HashMap<>();

        props.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaProperties.getBootstrapServers().get(0)
        );

        props.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class
        );

        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                JsonSerializer.class
        );

        return new DefaultKafkaProducerFactory<>(props);

    }

    @Bean
    public KafkaTemplate<String, Employee> kafkaTemplate() {

        return new KafkaTemplate<>(producerFactory());

    }

}
```

---

# How this Configuration Works

```
Spring Boot

        │

        ▼

ProducerFactory

        │

        ▼

KafkaTemplate

        │

        ▼

JsonSerializer

        │

        ▼

Kafka Broker
```

---

# @Configuration

```java
@Configuration
```

Marks this class as a Spring Configuration class.

Spring loads it during application startup.

---

# KafkaProperties

```java
private final KafkaProperties kafkaProperties;
```

Instead of hardcoding

```java
localhost:9093
```

Spring reads

```properties
spring.kafka.bootstrap-servers
```

from

```
application.properties
```

---

# ProducerFactory

```java
@Bean
public ProducerFactory<String, Employee>
```

ProducerFactory creates Kafka Producers.

```
ProducerFactory

↓

Kafka Producer

↓

Kafka Broker
```

Spring creates Producer instances from this factory.

---

# Producer Properties

A Map stores all Producer configurations.

```java
Map<String,Object> props =
        new HashMap<>();
```

---

# Bootstrap Server

```java
props.put(

ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,

kafkaProperties.getBootstrapServers().get(0)

);
```

Equivalent

```properties
spring.kafka.bootstrap-servers=localhost:9093
```

This tells the Producer where Kafka is running.

---

# Key Serializer

```java
props.put(

ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,

StringSerializer.class

);
```

Keys are Strings.

Example

```
emp-101

↓

StringSerializer

↓

Byte[]
```

---

# Value Serializer

```java
props.put(

ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,

JsonSerializer.class

);
```

This is the most important line.

Spring automatically converts

```
Employee
```

↓

into

```
JSON
```

↓

then

```
UTF-8 Bytes
```

↓

Kafka

No manual ObjectMapper code is required.

---

# DefaultKafkaProducerFactory

```java
return new DefaultKafkaProducerFactory<>(props);
```

Creates Kafka Producer objects using the above configuration.

```
Properties

↓

ProducerFactory

↓

KafkaProducer
```

---

# KafkaTemplate

```java
@Bean
public KafkaTemplate<String, Employee>
```

KafkaTemplate is the main class used to send messages.

Example

```java
kafkaTemplate.send(
        "employee-topic",
        employee
);
```

Internally

```
Employee

↓

KafkaTemplate

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

# Complete Producer Workflow

```
REST API

↓

Controller

↓

Producer Service

↓

KafkaTemplate

↓

ProducerFactory

↓

JsonSerializer

↓

JSON

↓

UTF-8 Bytes

↓

Kafka Producer

↓

Kafka Broker

↓

employee-topic
```

---

# What Does JsonSerializer Actually Do?

Suppose we send

```java
Employee employee = Employee.builder()
        .id(101)
        .firstName("Anant")
        .lastName("Singh")
        .department("Engineering")
        .designation("Software Engineer")
        .salary(85000.0)
        .build();
```

Internally Spring converts it into

```json
{
  "id":101,
  "firstName":"Anant",
  "lastName":"Singh",
  "department":"Engineering",
  "designation":"Software Engineer",
  "salary":85000.0
}
```

Then

```
JSON

↓

UTF-8 Encoding

↓

Byte[]

↓

Kafka
```

Kafka never receives the Java object.

It receives only bytes.

---

# Why KafkaTemplate<String, Employee>?

The generic types mean:

```java
<String, Employee>
```

| Type | Meaning |
|------|----------|
| String | Message Key |
| Employee | Message Value |

So we can send

```java
kafkaTemplate.send(
        "employee-topic",
        "emp-101",
        employee
);
```

---

# Best Practices

✅ Use `KafkaProperties` instead of hardcoded broker addresses.

✅ Use `JsonSerializer` for Java objects.

✅ Keep Producer configuration separate from business logic.

✅ Use `KafkaTemplate<String, Employee>` for type safety.

✅ Let Spring handle JSON conversion automatically.

---

# Summary

| Component | Responsibility |
|-----------|----------------|
| ProducerFactory | Creates Kafka Producers |
| KafkaTemplate | Sends Messages |
| StringSerializer | Serializes Message Keys |
| JsonSerializer | Serializes Employee Objects |
| KafkaProperties | Reads Kafka Configuration |
| DefaultKafkaProducerFactory | Builds Producer Instances |

---

# Project Structure

```
src
└── main
    └── java
        └── edu
            └── anant
                ├── config
                │      ├── KafkaJsonTopicConfig.java
                │      └── KafkaJsonProducerConfig.java
                │
                └── dto
                       └── Employee.java
```

---

# Next File

➡️ **KafkaJsonConsumerConfig.java**

In the next file, we'll configure:

- `ConsumerFactory`
- `JsonDeserializer`
- `ConcurrentKafkaListenerContainerFactory`
- `trusted.packages`
- `default.value.type`
- Complete JSON deserialization pipeline.