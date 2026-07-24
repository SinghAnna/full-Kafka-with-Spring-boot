# File 6 — KafkaJsonConsumerService.java

```java
package edu.anant.service;

import edu.anant.dto.Employee;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class KafkaJsonConsumerService {

    @KafkaListener(
            topics = "employee-topic",
            groupId = "employee-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(
            ConsumerRecord<String, Employee> record
    ) {

        Employee employee = record.value();

        log.info("========================================");
        log.info("EMPLOYEE RECEIVED SUCCESSFULLY");
        log.info("Topic      : {}", record.topic());
        log.info("Partition  : {}", record.partition());
        log.info("Offset     : {}", record.offset());
        log.info("Timestamp  : {}", record.timestamp());
        log.info("Key        : {}", record.key());
        log.info("Employee Id        : {}", employee.getId());
        log.info("First Name        : {}", employee.getFirstName());
        log.info("Last Name         : {}", employee.getLastName());
        log.info("Email             : {}", employee.getEmail());
        log.info("Department        : {}", employee.getDepartment());
        log.info("Designation       : {}", employee.getDesignation());
        log.info("Salary            : {}", employee.getSalary());
        log.info("========================================");

    }

}
```

---

# How This Consumer Works

```
Kafka Topic

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

consume()
```

---

# @KafkaListener

```java
@KafkaListener(
        topics = "employee-topic",
        groupId = "employee-group",
        containerFactory = "kafkaListenerContainerFactory"
)
```

Spring automatically starts a Kafka Consumer.

No need to write

```java
consumer.poll()
```

Spring continuously polls Kafka in the background.

---

# Topics

```java
topics = "employee-topic"
```

The Consumer listens only to this topic.

```
employee-topic

        │

        ▼

KafkaJsonConsumerService
```

---

# Group ID

```java
groupId = "employee-group"
```

All consumers with the same Group ID share the messages.

Example

```
employee-group

      │

 ┌────┴────┐

Consumer1  Consumer2
```

---

# Container Factory

```java
containerFactory =
"kafkaListenerContainerFactory"
```

Spring uses the configuration created in

```
KafkaJsonConsumerConfig
```

which contains

- JsonDeserializer
- Trusted Packages
- ConsumerFactory
- Bootstrap Server

---

# ConsumerRecord<String, Employee>

```java
ConsumerRecord<String, Employee>
```

The Generic Types mean

```
<String, Employee>
```

| Generic | Meaning |
|----------|----------|
| String | Message Key |
| Employee | Message Value |

---

# Get Employee

```java
Employee employee = record.value();
```

Spring has already converted

```
Byte[]

↓

JSON

↓

Employee
```

No ObjectMapper required.

No manual parsing.

---

# Record Metadata

```java
record.topic()
```

Returns

```
employee-topic
```

---

```java
record.partition()
```

Example

```
Partition : 2
```

---

```java
record.offset()
```

Example

```
Offset : 45
```

Each message has a unique offset inside its partition.

---

```java
record.timestamp()
```

Returns the timestamp when Kafka stored the message.

---

```java
record.key()
```

Returns

```
emp-101
```

or

```
null
```

---

# Employee Fields

Now we can directly access every property.

```java
employee.getId()

employee.getFirstName()

employee.getLastName()

employee.getEmail()

employee.getDepartment()

employee.getDesignation()

employee.getSalary()
```

This proves that JsonDeserializer successfully converted the JSON into an Employee object.

---

# Internal Flow

```
Producer

        │

        ▼

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

Kafka Topic

        │

        ▼

JsonDeserializer

        │

        ▼

Employee Object

        │

        ▼

@KafkaListener

        │

        ▼

Business Logic
```

---

# Example Console Output

```
========================================
EMPLOYEE RECEIVED SUCCESSFULLY
Topic      : employee-topic
Partition  : 1
Offset     : 12
Timestamp  : 1753357000000
Key        : emp-101

Employee Id        : 101
First Name         : Anant
Last Name          : Singh
Email              : anant@gmail.com
Department         : Engineering
Designation        : Software Engineer
Salary             : 85000.0
========================================
```

---

# Why Use ConsumerRecord?

Instead of

```java
public void consume(Employee employee)
```

using

```java
ConsumerRecord<String, Employee>
```

provides additional metadata.

You get access to:

- Topic
- Partition
- Offset
- Timestamp
- Headers
- Key
- Value

This is extremely useful for debugging and production monitoring.

---

# Best Practices

✅ Use `ConsumerRecord` when you need Kafka metadata.

✅ Let Spring deserialize JSON automatically.

✅ Keep business logic separate from deserialization.

✅ Log Topic, Partition and Offset during development.

✅ Avoid manual `ObjectMapper` usage when using `JsonDeserializer`.

---

# Summary

| Component | Responsibility |
|-----------|----------------|
| `@KafkaListener` | Starts Kafka Consumer |
| `ConsumerRecord` | Contains Metadata + Employee |
| `JsonDeserializer` | JSON → Employee |
| `record.value()` | Returns Employee Object |
| `record.key()` | Returns Message Key |
| `record.partition()` | Returns Partition |
| `record.offset()` | Returns Offset |
| `record.timestamp()` | Returns Kafka Timestamp |

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
├── dto
│      └── Employee.java
│
└── service
       ├── KafkaJsonProducerService.java
       └── KafkaJsonConsumerService.java
```

---

# Next File

➡️ **KafkaJsonController.java**

We'll create REST APIs to:

- Send a single Employee
- Send an Employee with a message key
- Send multiple Employees (Bulk)
- Test everything from Postman while observing Producer and Consumer logs.