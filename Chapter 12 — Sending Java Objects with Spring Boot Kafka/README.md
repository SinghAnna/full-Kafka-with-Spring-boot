# Chapter 12 — Sending Java Objects with Spring Boot Kafka

> In this chapter, we will send and receive Java objects such as `Employee` through Apache Kafka using Spring Boot, `JsonSerializer`, and `JsonDeserializer`.

---

## Learning Objectives

After completing this chapter, you will understand:

- Why Kafka messages need serialization
- What POJO and DTO mean
- How Java objects become JSON and bytes
- How `JsonSerializer` works
- How `JsonDeserializer` works
- How type mapping works
- How to configure Kafka Producer and Consumer
- How to send messages with and without keys
- How to test object messaging using Postman
- Important production practices for JSON-based Kafka messaging

---

# 1. Why Send Java Objects?

Until now, Kafka messages may have been simple strings:

```java
"Hello Kafka"
```

But real-world applications work with structured data such as:

- Employee
- Order
- Product
- Payment
- Customer
- Notification

For example, an Order Service may create an order:

```java
Order order = new Order(
        101,
        "Laptop",
        85000
);
```

Instead of sending a comma-separated string:

```text
101,Laptop,85000
```

we can send structured JSON:

```json
{
  "id": 101,
  "product": "Laptop",
  "price": 85000
}
```

This is easier to read, debug, and consume in Java, Node.js, Python, Go, or .NET applications.

---

# 2. Kafka Stores Bytes, Not Java Objects

Kafka cannot directly store an `Employee` or `Order` Java object.

Kafka stores data as bytes:

```text
Java Object
     ↓
Serialization
     ↓
JSON / Bytes
     ↓
Kafka Topic
     ↓
Bytes / JSON
     ↓
Deserialization
     ↓
Java Object
```

---

# 3. What Is a POJO?

**POJO** means **Plain Old Java Object**.

Example:

```java
public class Employee {

    private Long id;
    private String name;
    private String email;

}
```

A POJO commonly contains:

- Fields
- Constructors
- Getters
- Setters
- Business-related data

In Kafka applications, such objects are commonly called **DTOs**.

> DTO means **Data Transfer Object**. It carries data between services or application layers.

---

# 4. Serialization

Serialization converts a Java object into a format Kafka can store.

```text
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

Example Java object:

```java
Employee employee = new Employee(
        101L,
        "Anant",
        "anant@gmail.com",
        "Engineering",
        85000.0
);
```

Equivalent JSON:

```json
{
  "id": 101,
  "name": "Anant",
  "email": "anant@gmail.com",
  "department": "Engineering",
  "salary": 85000.0
}
```

---

# 5. Deserialization

Deserialization is the reverse process.

```text
Kafka Bytes
     ↓
JsonDeserializer
     ↓
JSON
     ↓
Employee Object
```

The consumer receives bytes from Kafka and converts them back into an `Employee` object.

---

# 6. Complete Producer-Consumer Flow

```text
                    PRODUCER

Employee Object
       ↓
JsonSerializer
       ↓
JSON
       ↓
Bytes
       ↓
Kafka Topic
       ↓
Bytes
       ↓
JsonDeserializer
       ↓
Employee Object

                    CONSUMER
```

---

# 7. Type Mapping

When a consumer receives JSON, it must know which Java class to create.

Example JSON:

```json
{
  "id": 101,
  "name": "Anant"
}
```

The consumer needs to know:

```text
Incoming JSON
     ↓
Create Employee.class object
```

There are two common approaches.

## Method 1: Default Type

```properties
spring.kafka.consumer.properties.spring.json.value.default.type=edu.anant.dto.Employee
```

This explicitly tells Spring Kafka to deserialize incoming JSON as `Employee`.

## Method 2: Type Headers

Spring Kafka can store type information in Kafka headers:

```text
__TypeId__ = edu.anant.dto.Employee
```

The consumer reads the header and decides which Java class should be created.

In this chapter, we use the **default type approach** and disable type headers.

---

# 8. Project Structure

```text
src
└── main
    ├── java
    │   └── edu.anant
    │       ├── config
    │       │   ├── KafkaObjectProducerConfig.java
    │       │   ├── KafkaObjectConsumerConfig.java
    │       │   └── KafkaObjectTopicConfig.java
    │       │
    │       ├── controller
    │       │   └── KafkaObjectController.java
    │       │
    │       ├── dto
    │       │   └── Employee.java
    │       │
    │       └── service
    │           ├── KafkaObjectProducerService.java
    │           └── KafkaObjectConsumerService.java
    │
    └── resources
        └── application.properties
```

---

# 9. Employee DTO

**File:** `Employee.java`

```java
package edu.anant.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Employee {

    private Long id;

    private String name;

    private String email;

    private String department;

    private Double salary;

}
```

## Why `@NoArgsConstructor` Is Important

Jackson commonly creates an object like this during deserialization:

```java
Employee employee = new Employee();
```

Then it populates the fields from JSON.

Therefore, a no-args constructor is generally important for JSON deserialization.

---

# 10. Kafka Topic Configuration

**File:** `KafkaObjectTopicConfig.java`

```java
package edu.anant.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaObjectTopicConfig {

    @Bean
    public NewTopic employeeTopic() {

        return TopicBuilder
                .name("employee-object-topic")
                .partitions(3)
                .replicas(1)
                .build();
    }

}
```

## Topic Architecture

```text
employee-object-topic

Partition 0
Partition 1
Partition 2
```

Kafka distributes messages across partitions.

When a key is provided:

```text
Employee Key
     ↓
Hash Calculation
     ↓
Partition Selection
```

Messages with the same key generally go to the same partition, preserving order for that key.

---

# 11. Producer Configuration

**File:** `KafkaObjectProducerConfig.java`

```java
package edu.anant.config;

import edu.anant.dto.Employee;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaObjectProducerConfig {

    private final KafkaProperties kafkaProperties;

    public KafkaObjectProducerConfig(
            KafkaProperties kafkaProperties
    ) {
        this.kafkaProperties = kafkaProperties;
    }

    @Bean
    public DefaultKafkaProducerFactory<String, Employee>
    producerFactory() {

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

## Why `KafkaTemplate<String, Employee>`?

```java
KafkaTemplate<String, Employee>
```

It means:

| Part | Type |
|---|---|
| Kafka key | `String` |
| Kafka value | `Employee` |

Example:

```text
Key   : emp-101
Value : Employee Object
```

---

# 12. JsonSerializer

`JsonSerializer` converts Java objects into JSON bytes.

```text
Employee
    ↓
JsonSerializer
    ↓
JSON
    ↓
Bytes
```

Example:

```java
Employee employee = Employee.builder()
        .id(101L)
        .name("Anant")
        .email("anant@gmail.com")
        .department("Engineering")
        .salary(85000.0)
        .build();
```

Serialized JSON:

```json
{
  "id": 101,
  "name": "Anant",
  "email": "anant@gmail.com",
  "department": "Engineering",
  "salary": 85000.0
}
```

---

# 13. Producer Service

**File:** `KafkaObjectProducerService.java`

```java
package edu.anant.service;

import edu.anant.dto.Employee;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaObjectProducerService {

    private static final String TOPIC =
            "employee-object-topic";

    private final KafkaTemplate<String, Employee> kafkaTemplate;

    public void send(Employee employee) {

        kafkaTemplate
                .send(TOPIC, employee)
                .whenComplete(this::handleResult);
    }

    public void send(
            String key,
            Employee employee
    ) {

        kafkaTemplate
                .send(TOPIC, key, employee)
                .whenComplete(this::handleResult);
    }

    private void handleResult(
            SendResult<String, Employee> result,
            Throwable exception
    ) {

        if (exception != null) {
            log.error(
                    "Kafka message failed: {}",
                    exception.getMessage()
            );
            return;
        }

        log.info("========================================");
        log.info("EMPLOYEE SENT SUCCESSFULLY");
        log.info(
                "Topic     : {}",
                result.getRecordMetadata().topic()
        );
        log.info(
                "Partition : {}",
                result.getRecordMetadata().partition()
        );
        log.info(
                "Offset    : {}",
                result.getRecordMetadata().offset()
        );
        log.info(
                "Timestamp : {}",
                result.getRecordMetadata().timestamp()
        );
        log.info(
                "Key       : {}",
                result.getProducerRecord().key()
        );
        log.info(
                "Employee  : {}",
                result.getProducerRecord().value()
        );
        log.info("========================================");
    }

}
```

---

# 14. Send Message Without Key

```java
kafkaTemplate.send(
        TOPIC,
        employee
);
```

Flow:

```text
Employee
     ↓
Kafka
     ↓
Kafka selects a partition
```

Without a key, Kafka decides the partition based on its partitioning strategy.

---

# 15. Send Message With Key

```java
kafkaTemplate.send(
        TOPIC,
        "emp-101",
        employee
);
```

Flow:

```text
emp-101
     ↓
Hash
     ↓
Partition X

Employee
     ↓
Same Partition X
```

Use a key when ordering matters for the same entity.

Example keys:

```text
employee-101
order-5001
customer-200
```

If all events for `employee-101` use the same key, Kafka sends them to the same partition, preserving their order within that partition.

---

# 16. Consumer Configuration

**File:** `KafkaObjectConsumerConfig.java`

```java
package edu.anant.config;

import edu.anant.dto.Employee;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaObjectConsumerConfig {

    private final KafkaProperties kafkaProperties;

    public KafkaObjectConsumerConfig(
            KafkaProperties kafkaProperties
    ) {
        this.kafkaProperties = kafkaProperties;
    }

    @Bean
    public DefaultKafkaConsumerFactory<String, Employee>
    employeeConsumerFactory() {

        Map<String, Object> props = new HashMap<>();

        props.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaProperties.getBootstrapServers().get(0)
        );

        props.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "employee-object-group"
        );

        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );

        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                JsonDeserializer.class
        );

        props.put(
                JsonDeserializer.TRUSTED_PACKAGES,
                "edu.anant.dto"
        );

        props.put(
                JsonDeserializer.VALUE_DEFAULT_TYPE,
                Employee.class.getName()
        );

        props.put(
                JsonDeserializer.USE_TYPE_INFO_HEADERS,
                false
        );

        props.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
        );

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<
            String,
            Employee
            > employeeKafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<
                String,
                Employee
                > factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(employeeConsumerFactory());

        return factory;
    }

}
```

---

# 17. Consumer Flow

```text
Kafka
  ↓
Bytes
  ↓
JsonDeserializer
  ↓
JSON
  ↓
Employee Object
  ↓
@KafkaListener
```

---

# 18. Trusted Packages

```java
props.put(
        JsonDeserializer.TRUSTED_PACKAGES,
        "edu.anant.dto"
);
```

This tells Spring Kafka which packages are trusted for deserialization.

Use a specific package in production:

```text
edu.anant.dto
```

Avoid this in production:

```text
*
```

Allowing every package may create security risks if untrusted type information is deserialized.

---

# 19. Default Type

```java
props.put(
        JsonDeserializer.VALUE_DEFAULT_TYPE,
        Employee.class.getName()
);
```

This tells Spring:

```text
Incoming JSON
     ↓
Employee.class
```

It is useful when the consumer is explicitly designed to receive only one DTO type.

---

# 20. Disable Type Headers

```java
props.put(
        JsonDeserializer.USE_TYPE_INFO_HEADERS,
        false
);
```

This means:

```text
Ignore Kafka type headers
        ↓
Always use Employee.class
```

This approach gives the consumer explicit control over the expected object type.

---

# 21. Consumer Service

**File:** `KafkaObjectConsumerService.java`

```java
package edu.anant.service;

import edu.anant.dto.Employee;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class KafkaObjectConsumerService {

    @KafkaListener(
            topics = "employee-object-topic",
            groupId = "employee-object-group",
            containerFactory = "employeeKafkaListenerContainerFactory"
    )
    public void consume(
            ConsumerRecord<String, Employee> record
    ) {

        Employee employee = record.value();

        log.info("========================================");
        log.info("EMPLOYEE RECEIVED");

        log.info("Topic      : {}", record.topic());
        log.info("Partition  : {}", record.partition());
        log.info("Offset     : {}", record.offset());
        log.info("Timestamp  : {}", record.timestamp());
        log.info("Key        : {}", record.key());

        log.info("Employee ID : {}", employee.getId());
        log.info("Name        : {}", employee.getName());
        log.info("Email       : {}", employee.getEmail());
        log.info("Department  : {}", employee.getDepartment());
        log.info("Salary      : {}", employee.getSalary());

        log.info("========================================");
    }

}
```

## Important Point

Use:

```java
ConsumerRecord<String, Employee>
```

Not:

```java
ConsumerRecord<String, String>
```

Because after deserialization, the consumer receives an actual `Employee` object.

---

# 22. REST Controller

**File:** `KafkaObjectController.java`

```java
package edu.anant.controller;

import edu.anant.dto.Employee;
import edu.anant.service.KafkaObjectProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/employees")
@RequiredArgsConstructor
public class KafkaObjectController {

    private final KafkaObjectProducerService producerService;

    @PostMapping
    public String sendEmployee(
            @RequestBody Employee employee
    ) {

        producerService.send(employee);

        return "Employee sent successfully";
    }

    @PostMapping("/{key}")
    public String sendEmployeeWithKey(
            @PathVariable String key,
            @RequestBody Employee employee
    ) {

        producerService.send(key, employee);

        return "Employee sent with key: " + key;
    }

    @PostMapping("/bulk/{count}")
    public String sendBulk(
            @PathVariable int count
    ) {

        for (int i = 1; i <= count; i++) {

            Employee employee =
                    Employee.builder()
                            .id((long) i)
                            .name("Employee-" + i)
                            .email(
                                    "employee"
                                            + i
                                            + "@gmail.com"
                            )
                            .department("Engineering")
                            .salary(
                                    50000.0 + (i * 1000)
                            )
                            .build();

            producerService.send(employee);
        }

        return count + " employees sent successfully";
    }

}
```

---

# 23. Application Configuration

**File:** `application.properties`

```properties
spring.application.name=kafka-spring-boot

# Kafka Broker
spring.kafka.bootstrap-servers=localhost:9093

# Producer
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer

# Consumer
spring.kafka.consumer.group-id=employee-object-group
spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer

# JSON Deserializer
spring.kafka.consumer.properties.spring.json.trusted.packages=edu.anant.dto
spring.kafka.consumer.properties.spring.json.value.default.type=edu.anant.dto.Employee
spring.kafka.consumer.properties.spring.json.use.type.headers=false
```

> If your Kafka Docker container exposes a different port, update `spring.kafka.bootstrap-servers` accordingly.

---

# 24. Testing with Postman

## Send Without Key

**Request**

```http
POST http://localhost:8080/api/employees
```

**Body**

```json
{
  "id": 101,
  "name": "Anant",
  "email": "anant@gmail.com",
  "department": "Engineering",
  "salary": 85000
}
```

## Expected Producer Log

```text
EMPLOYEE SENT SUCCESSFULLY

Topic     : employee-object-topic
Partition : 1
Offset    : 0
Key       : null

Employee  : Employee(
    id=101,
    name=Anant,
    email=anant@gmail.com,
    department=Engineering,
    salary=85000.0
)
```

## Expected Consumer Log

```text
EMPLOYEE RECEIVED

Topic      : employee-object-topic
Partition  : 1
Offset     : 0
Key        : null

Employee ID : 101
Name        : Anant
Email       : anant@gmail.com
Department  : Engineering
Salary      : 85000.0
```

---

# 25. Send With Key

**Request**

```http
POST http://localhost:8080/api/employees/emp-101
```

**Body**

```json
{
  "id": 101,
  "name": "Anant",
  "email": "anant@gmail.com",
  "department": "Engineering",
  "salary": 85000
}
```

Kafka receives:

```text
Key   : emp-101
Value : Employee Object
```

Use the same key for all events related to the same employee when event ordering matters.

---

# 26. Bulk Testing

**Request**

```http
POST http://localhost:8080/api/employees/bulk/10
```

This sends:

```text
Employee-1
Employee-2
Employee-3
Employee-4
Employee-5
Employee-6
Employee-7
Employee-8
Employee-9
Employee-10
```

---

# 27. Complete Architecture

```text
                         REST API
                            │
                            ▼
                  KafkaObjectController
                            │
                            ▼
             KafkaObjectProducerService
                            │
                            ▼
              KafkaTemplate<String, Employee>
                            │
                            ▼
                     JsonSerializer
                            │
                            ▼
                      JSON / Bytes
                            │
                            ▼
                       Kafka Broker
                            │
                            ▼
                employee-object-topic
                            │
                            ▼
                    Kafka Consumer
                            │
                            ▼
                    JsonDeserializer
                            │
                            ▼
                    Employee Object
                            │
                            ▼
             KafkaObjectConsumerService
```

---

# 28. String vs Object Messaging

| Feature | String Messaging | Object Messaging |
|---|---|---|
| Kafka template | `KafkaTemplate<String, String>` | `KafkaTemplate<String, Employee>` |
| Value format | Plain string | POJO / DTO |
| Serializer | `StringSerializer` | `JsonSerializer` |
| Deserializer | `StringDeserializer` | `JsonDeserializer` |
| Type safety | Lower | Higher |
| Complex data | Difficult to manage | Easy to manage |
| Readability | Medium | High |
| Microservice integration | Limited | Better |

Example string message:

```java
"Hello Kafka"
```

Example object message:

```java
Employee employee = new Employee();
```

---

# 29. Why JSON?

JSON is popular because it is:

- Human-readable
- Language-independent
- Easy to debug
- Supported by Java
- Supported by JavaScript and Node.js
- Supported by Python
- Supported by Go
- Supported by .NET

Example:

```json
{
  "id": 101,
  "name": "Anant"
}
```

---

# 30. Production Considerations

JSON is convenient, but it may not be the best choice for very large or highly schema-sensitive systems.

Common serialization formats include:

- JSON
- Avro
- Protobuf
- JSON Schema

A common production-level architecture looks like this:

```text
Producer
   ↓
Avro / Protobuf
   ↓
Schema Registry
   ↓
Kafka
   ↓
Consumer
```

For learning and many internal services, JSON is an excellent starting point.

---

# 31. Common Errors

## Error: No Type Information

**Problem**

The consumer does not know which Java object to create.

**Solution**

```properties
spring.kafka.consumer.properties.spring.json.value.default.type=edu.anant.dto.Employee
```

---

## Error: Trusted Package Error

**Problem**

```text
The class is not in the trusted packages
```

**Solution**

```properties
spring.kafka.consumer.properties.spring.json.trusted.packages=edu.anant.dto
```

---

## Error: JSON and DTO Mismatch

If Kafka sends:

```json
{
  "id": "ABC"
}
```

but the DTO expects:

```java
private Long id;
```

deserialization can fail.

Ensure JSON field types match the Java DTO field types.

---

## Error: Consumer Reads Old Messages

If you use:

```properties
spring.kafka.consumer.auto-offset-reset=earliest
```

a new consumer group can read all old messages from the beginning.

For latest messages only, use:

```properties
spring.kafka.consumer.auto-offset-reset=latest
```

---

# 32. DTO Evolution

Suppose version 1 of `Employee` is:

```java
public class Employee {

    private Long id;
    private String name;

}
```

Later, version 2 adds a new field:

```java
private String department;
```

Older Kafka messages may not contain `department`.

Therefore, keep DTO changes backward-compatible whenever possible.

Good practices:

- Add optional fields instead of removing existing ones
- Avoid renaming fields without a migration plan
- Avoid changing field data types
- Use schema-based formats such as Avro or Protobuf in larger systems

---

# 33. Best Practices

## Producer

```java
KafkaTemplate<String, Employee>
```

Use typed Kafka templates instead of raw types.

## Consumer

```java
ConsumerRecord<String, Employee>
```

Use typed consumer records for better type safety.

## Serializer

```java
JsonSerializer
```

Use it when producing Java objects as JSON.

## Deserializer

```java
JsonDeserializer
```

Use it when consuming JSON as Java objects.

## Trusted Packages

Prefer:

```text
edu.anant.dto
```

Avoid:

```text
*
```

in production.

## Kafka Keys

Use keys when ordering matters for a particular entity.

Example:

```text
employee-101
order-5001
customer-200
```

## DTO Stability

Keep Kafka DTOs stable and backward-compatible because old messages can remain in Kafka topics.

---

# 34. Interview Questions

## Q1. Can Kafka directly store Java objects?

No. Kafka stores byte data. Java objects must be serialized before being sent.

## Q2. What is serialization?

```text
Java Object → Bytes
```

## Q3. What is deserialization?

```text
Bytes → Java Object
```

## Q4. What does `JsonSerializer` do?

```text
Java Object → JSON → Bytes
```

## Q5. What does `JsonDeserializer` do?

```text
Bytes → JSON → Java Object
```

## Q6. Why is a no-args constructor commonly needed?

Jackson may use it to create the DTO object before setting values from JSON.

## Q7. What are trusted packages?

They restrict which Java packages `JsonDeserializer` is allowed to deserialize.

## Q8. What is type mapping?

Type mapping tells the consumer which Java class should be created from incoming JSON.

## Q9. What is `VALUE_DEFAULT_TYPE`?

It defines the default Java class used for deserialization.

```properties
spring.json.value.default.type=edu.anant.dto.Employee
```

## Q10. Why use `ConsumerRecord<String, Employee>`?

It provides both the deserialized message and Kafka metadata:

- Key
- Value
- Topic
- Partition
- Offset
- Timestamp
- Headers

---

# 35. Golden Rule

```text
PRODUCER

Java Object
     ↓
Serializer
     ↓
Bytes
     ↓
Kafka


CONSUMER

Kafka
     ↓
Bytes
     ↓
Deserializer
     ↓
Java Object
```

---

# 36. Chapter Checklist

- [x] Why send Java objects through Kafka?
- [x] POJO and DTO
- [x] Serialization
- [x] Deserialization
- [x] JSON conversion
- [x] Type mapping
- [x] Employee DTO
- [x] Kafka topic creation
- [x] Producer configuration
- [x] `JsonSerializer`
- [x] Producer service
- [x] Consumer configuration
- [x] `JsonDeserializer`
- [x] Consumer service
- [x] REST controller
- [x] `application.properties`
- [x] Send without key
- [x] Send with key
- [x] Bulk sending
- [x] Postman testing
- [x] Common errors
- [x] DTO evolution
- [x] Production best practices
- [x] Interview questions

---

# Next Chapter

## Chapter 13 — Kafka Error Handling

Topics:

- Producer errors
- Consumer errors
- Serialization errors
- Deserialization errors
- Listener exceptions
- `ErrorHandler`
- `DefaultErrorHandler`
- Retry
- `BackOff`
- `FixedBackOff`
- Dead Letter Topic (DLT)
- `@RetryableTopic`
- Recovery strategies
- Production error handling