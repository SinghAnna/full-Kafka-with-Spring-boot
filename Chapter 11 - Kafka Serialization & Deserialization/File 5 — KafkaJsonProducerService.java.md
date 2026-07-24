# File 5 — KafkaJsonProducerService.java

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
public class KafkaJsonProducerService {

    private final KafkaTemplate<String, Employee> kafkaTemplate;

    private static final String TOPIC = "employee-topic";

    /**
     * Send Employee without Key
     */
    public void send(Employee employee) {

        kafkaTemplate.send(TOPIC, employee)
                .whenComplete(this::printResult);

    }

    /**
     * Send Employee with Key
     */
    public void send(String key, Employee employee) {

        kafkaTemplate.send(TOPIC, key, employee)
                .whenComplete(this::printResult);

    }

    private void printResult(
            SendResult<String, Employee> result,
            Throwable ex
    ) {

        if (ex != null) {

            log.error("Message Sending Failed : {}", ex.getMessage());

            return;

        }

        log.info("========================================");
        log.info("EMPLOYEE SENT SUCCESSFULLY");
        log.info("Topic      : {}", result.getRecordMetadata().topic());
        log.info("Partition  : {}", result.getRecordMetadata().partition());
        log.info("Offset     : {}", result.getRecordMetadata().offset());
        log.info("Timestamp  : {}", result.getRecordMetadata().timestamp());
        log.info("Key        : {}", result.getProducerRecord().key());
        log.info("Employee   : {}", result.getProducerRecord().value());
        log.info("========================================");

    }

}
```

---

# How This Service Works

```
Controller

      │

      ▼

Producer Service

      │

      ▼

KafkaTemplate

      │

      ▼

JsonSerializer

      │

      ▼

Kafka Topic
```

---

# KafkaTemplate<String, Employee>

```java
private final KafkaTemplate<String, Employee> kafkaTemplate;
```

Generic types:

```
<String, Employee>
```

| Generic | Meaning |
|----------|----------|
| String | Message Key |
| Employee | Message Value |

---

# Topic Name

```java
private static final String TOPIC = "employee-topic";
```

Instead of writing

```java
"employee-topic"
```

multiple times, we store it once as a constant.

---

# Send Without Key

```java
public void send(Employee employee)
```

Internally

```java
kafkaTemplate.send(
        TOPIC,
        employee
);
```

Flow

```
Employee

↓

JsonSerializer

↓

JSON

↓

Bytes

↓

Kafka
```

Kafka automatically chooses the partition.

---

# Send With Key

```java
public void send(
        String key,
        Employee employee
)
```

Example

```java
send(
    "emp-101",
    employee
);
```

Flow

```
Key

↓

Hash

↓

Partition

↓

Employee
```

The same key always goes to the same partition.

---

# Async Sending

```java
.whenComplete(this::printResult);
```

Kafka sends messages asynchronously.

```
Application

↓

Send()

↓

Kafka

↓

Success / Failure

↓

whenComplete()
```

The application thread doesn't wait for Kafka.

---

# SendResult

```java
SendResult<String, Employee>
```

Contains:

- Topic
- Partition
- Offset
- Timestamp
- ProducerRecord

---

# Record Metadata

```java
result.getRecordMetadata()
```

Provides metadata after Kafka stores the message.

---

# Topic

```java
result.getRecordMetadata().topic()
```

Example

```
employee-topic
```

---

# Partition

```java
result.getRecordMetadata().partition()
```

Example

```
Partition : 2
```

Useful for understanding partitioning.

---

# Offset

```java
result.getRecordMetadata().offset()
```

Example

```
Offset : 15
```

Every message has a unique offset inside its partition.

---

# Timestamp

```java
result.getRecordMetadata().timestamp()
```

Kafka stores the message creation time.

---

# Key

```java
result.getProducerRecord().key()
```

Example

```
emp-101
```

or

```
null
```

---

# Employee Object

```java
result.getProducerRecord().value()
```

Logs the complete Employee object.

Example

```
Employee(
 id=101,
 firstName=Anant,
 lastName=Singh,
 email=anant@gmail.com,
 department=Engineering,
 designation=Software Engineer,
 salary=85000.0
)
```

---

# Success Flow

```
Employee Object

        │

        ▼

KafkaTemplate

        │

        ▼

JsonSerializer

        │

        ▼

Kafka Broker

        │

        ▼

Topic

        │

        ▼

whenComplete()

        │

        ▼

Print Metadata
```

---

# Example Console Output

```
========================================
EMPLOYEE SENT SUCCESSFULLY
Topic      : employee-topic
Partition  : 1
Offset     : 25
Timestamp  : 1753354500000
Key        : emp-101
Employee   : Employee(id=101, firstName=Anant, lastName=Singh, email=anant@gmail.com, department=Engineering, designation=Software Engineer, salary=85000.0)
========================================
```

---

# Best Practices

✅ Keep topic names as constants.

✅ Use asynchronous sending.

✅ Log Topic, Partition and Offset for debugging.

✅ Use keys when ordering is important.

✅ Handle failures in `whenComplete()`.

---

# Summary

| Method | Purpose |
|----------|----------|
| send(Employee) | Send without key |
| send(String, Employee) | Send with key |
| KafkaTemplate | Sends message |
| JsonSerializer | Employee → JSON |
| whenComplete() | Callback after send |
| SendResult | Metadata after sending |

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
       └── KafkaJsonProducerService.java
```

---

# Next File

➡️ **KafkaJsonConsumerService.java**

In the next file, we'll create the consumer that receives JSON from Kafka, automatically converts it back into an `Employee` object using `JsonDeserializer`, and logs all metadata including Topic, Partition, Offset, Key, Timestamp, and the deserialized `Employee`.