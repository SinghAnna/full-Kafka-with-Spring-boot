# Chapter — Kafka Transactions and Exactly-Once Semantics

> In this chapter, we will learn how Kafka transactions work, the difference between exactly-once, at-least-once, and at-most-once semantics, and how to implement transactional producers and consumers in Spring Boot applications.

---

## Learning Objectives

After completing this chapter, you will understand:

- What are Kafka transactions?
- Exactly-once vs at-least-once vs at-most-once semantics
- Transactional producers
- Transactional consumers
- Read committed vs read uncommitted isolation
- Transaction configuration
- Transactional patterns (write-write, read-process-write)
- Common pitfalls and solutions
- Production best practices
- Monitoring and debugging transactions

---

# 1. What are Kafka Transactions?

Kafka transactions enable **atomic writes** across multiple topics and partitions.

Just like database transactions ensure atomicity for database operations, Kafka transactions ensure atomicity for message production.

```text
Without Transactions:

Producer ──► Topic A (Message 1) ✓
         ──► Topic B (Message 2) ✗ (fails)

Result: Message 1 sent, Message 2 lost (inconsistent state)


With Transactions:

Producer ──► Begin Transaction
         ──► Topic A (Message 1) (pending)
         ──► Topic B (Message 2) (pending)
         ──► Commit Transaction

Result: Both messages sent atomically OR both aborted
```

---

## Transaction Guarantees

| Guarantee | Description |
|---|---|
| **Atomicity** | All messages in transaction succeed or all fail |
| **Consistency** | System remains in valid state |
| **Isolation** | Uncommitted messages not visible to consumers |
| **Durability** | Committed messages persist even after failures |

---

# 2. Delivery Semantics

## At-Most-Once

Message is sent once, but may be lost.

```text
Producer ──► Send Message
         ──► (No retry on failure)

Result: Message may be lost, but never duplicated
```

**Use Cases:**

- Metrics collection
- Log aggregation
- Non-critical monitoring data

**Configuration:**

```properties
spring.kafka.producer.acks=0
spring.kafka.producer.retries=0
```

---

## At-Least-Once

Message is guaranteed to be delivered, but may be duplicated.

```text
Producer ──► Send Message
         ──► Ack received? No
         ──► Retry
         ──► Ack received? Yes

Result: Message delivered, but may be duplicated
```

**Use Cases:**

- Most common pattern
- Order events
- User activity tracking
- General microservices communication

**Configuration:**

```properties
spring.kafka.producer.acks=1
spring.kafka.producer.retries=3
spring.kafka.producer.properties.enable.idempotence=true
```

---

## Exactly-Once

Message is delivered exactly once, no loss, no duplicates.

```text
Producer ──► Begin Transaction
         ──► Send Message 1 (pending)
         ──► Send Message 2 (pending)
         ──► Commit Transaction

Result: Messages delivered exactly once
```

**Use Cases:**

- Financial transactions
- Payment processing
- Critical business operations
- Data pipelines requiring consistency

**Configuration:**

```properties
spring.kafka.producer.acks=all
spring.kafka.producer.retries=2147483647
spring.kafka.producer.properties.enable.idempotence=true
spring.kafka.producer.transaction-id-prefix=my-tx-
```

---

## Comparison Table

| Semantics | Message Loss | Duplicates | Performance | Complexity |
|---|---|---|---|---|
| **At-Most-Once** | Possible | No | Highest | Lowest |
| **At-Least-Once** | No | Possible | High | Medium |
| **Exactly-Once** | No | No | Lower | Highest |

---

## Visual Comparison

```text
At-Most-Once:
Send ──► Fire and Forget
       ──► May lose messages


At-Least-Once:
Send ──► Retry on Failure
       ──► May duplicate messages


Exactly-Once:
Begin Tx ──► Send Messages ──► Commit
         ──► Abort on Failure
         ──► No loss, no duplicates
```

---

# 3. Transactional Producers

## How Transactions Work

```text
Step 1: Initialize Transaction
Producer ──► Kafka Cluster
         ──► Get Transactional ID
         ──► Begin Transaction


Step 2: Send Messages
Producer ──► Topic A (Message 1) [pending]
         ──► Topic B (Message 2) [pending]
         ──► Topic C (Message 3) [pending]


Step 3: Commit or Abort
Success: Commit ──► All messages visible
Failure: Abort ──► All messages discarded
```

---

## Transactional Producer Configuration

**File:** `TransactionalProducerConfig.java`

```java
package edu.anant.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class TransactionalProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public DefaultKafkaProducerFactory<String, String> 
    transactionalProducerFactory() {

        Map<String, Object> props = new HashMap<>();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Transactional Configuration
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-transactional-id-1");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        // Timeout Configuration
        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60000); // 60 seconds
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> transactionalKafkaTemplate() {

        KafkaTemplate<String, String> template = 
            new KafkaTemplate<>(transactionalProducerFactory());

        template.setTransactionIdPrefix("tx-");

        return template;
    }

}
```

---

## Transactional Producer Service

**File:** `TransactionalProducerService.java`

```java
package edu.anant.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransactionalProducerService {

    private static final String TOPIC_A = "topic-a";
    private static final String TOPIC_B = "topic-b";

    private final KafkaTemplate<String, String> kafkaTemplate;

    // Transactional send to multiple topics
    @Transactional
    public void sendToMultipleTopics(String key, String messageA, String messageB) {

        log.info("========================================");
        log.info("BEGIN TRANSACTION");
        log.info("Sending to Topic A: {}", messageA);
        log.info("Sending to Topic B: {}", messageB);

        try {

            // Send to Topic A
            kafkaTemplate.send(TOPIC_A, key, messageA)
                .whenComplete(this::handleResultA);

            // Send to Topic B
            kafkaTemplate.send(TOPIC_B, key, messageB)
                .whenComplete(this::handleResultB);

            log.info("Transaction committed successfully");
            log.info("========================================");

        } catch (Exception e) {

            log.error("========================================");
            log.error("TRANSACTION FAILED");
            log.error("Error: {}", e.getMessage());
            log.error("Transaction will be rolled back");
            log.error("========================================");

            throw e; // Spring will rollback
        }
    }

    private void handleResultA(SendResult<String, String> result, Throwable exception) {

        if (exception != null) {
            log.error("Failed to send to Topic A: {}", exception.getMessage());
            return;
        }

        log.info("Message sent to Topic A - Offset: {}", 
            result.getRecordMetadata().offset());
    }

    private void handleResultB(SendResult<String, String> result, Throwable exception) {

        if (exception != null) {
            log.error("Failed to send to Topic B: {}", exception.getMessage());
            return;
        }

        log.info("Message sent to Topic B - Offset: {}", 
            result.getRecordMetadata().offset());
    }

}
```

---

## Manual Transaction Control

**File:** `ManualTransactionalService.java`

```java
package edu.anant.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class ManualTransactionalService {

    private static final String TOPIC_A = "topic-a";
    private static final String TOPIC_B = "topic-b";

    private final KafkaTemplate<String, String> kafkaTemplate;

    // Manual transaction control
    public void sendWithManualTransaction(String key, 
                                          String messageA, 
                                          String messageB) {

        log.info("========================================");
        log.info("MANUAL TRANSACTION START");

        try {

            // Begin transaction
            kafkaTemplate.executeInTransaction(operations -> {

                log.info("Transaction started");

                // Send to Topic A
                log.info("Sending to Topic A: {}", messageA);
                SendResult<String, String> resultA = 
                    operations.send(TOPIC_A, key, messageA).get();

                log.info("Message A sent - Offset: {}", 
                    resultA.getRecordMetadata().offset());

                // Send to Topic B
                log.info("Sending to Topic B: {}", messageB);
                SendResult<String, String> resultB = 
                    operations.send(TOPIC_B, key, messageB).get();

                log.info("Message B sent - Offset: {}", 
                    resultB.getRecordMetadata().offset());

                // Simulate business logic
                processBusinessLogic();

                log.info("Business logic completed");

                return null; // Transaction will commit

            });

            log.info("MANUAL TRANSACTION COMMITTED SUCCESSFULLY");
            log.info("========================================");

        } catch (Exception e) {

            log.error("========================================");
            log.error("MANUAL TRANSACTION FAILED");
            log.error("Error: {}", e.getMessage());
            log.error("Transaction aborted");
            log.error("========================================");

            // Handle failure
            handleTransactionFailure(e);
        }
    }

    private void processBusinessLogic() {

        // Simulate business logic
        log.info("Executing business logic...");

        // This could throw an exception
        // If it does, transaction will be aborted
    }

    private void handleTransactionFailure(Exception e) {

        // Save to database for retry
        // Send alert
        // Log for manual review
    }

}
```

---

# 4. Transactional Consumers

## Read Committed vs Read Uncommitted

### Read Uncommitted (Default)

Consumer sees all messages, including uncommitted ones.

```properties
spring.kafka.consumer.isolation-level=read_uncommitted
```

**Behavior:**

- Sees both committed and uncommitted messages
- May see messages that will be aborted
- Faster but less consistent

---

### Read Committed

Consumer only sees committed messages.

```properties
spring.kafka.consumer.isolation-level=read_committed
```

**Behavior:**

- Only sees committed messages
- Waits for transactions to complete
- Slower but consistent

---

## Transactional Consumer Configuration

**File:** `TransactionalConsumerConfig.java`

```java
package edu.anant.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class TransactionalConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public DefaultKafkaConsumerFactory<String, String> 
    transactionalConsumerFactory() {

        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "transactional-consumer-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Read committed isolation
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> 
    transactionalKafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(transactionalConsumerFactory());
        factory.setConcurrency(3);

        return factory;
    }

}
```

---

## Transactional Consumer Service

**File:** `TransactionalConsumerService.java`

```java
package edu.anant.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class TransactionalConsumerService {

    @KafkaListener(
        topics = "topic-a",
        groupId = "transactional-consumer-group",
        containerFactory = "transactionalKafkaListenerContainerFactory"
    )
    public void consumeTopicA(ConsumerRecord<String, String> record) {

        log.info("========================================");
        log.info("TRANSACTIONAL MESSAGE RECEIVED (Topic A)");
        log.info("Topic      : {}", record.topic());
        log.info("Partition  : {}", record.partition());
        log.info("Offset     : {}", record.offset());
        log.info("Key        : {}", record.key());
        log.info("Value      : {}", record.value());
        log.info("Is Committed: Yes (read_committed)");
        log.info("========================================");

        // Process message
        processMessage(record.key(), record.value());
    }

    @KafkaListener(
        topics = "topic-b",
        groupId = "transactional-consumer-group",
        containerFactory = "transactionalKafkaListenerContainerFactory"
    )
    public void consumeTopicB(ConsumerRecord<String, String> record) {

        log.info("========================================");
        log.info("TRANSACTIONAL MESSAGE RECEIVED (Topic B)");
        log.info("Topic      : {}", record.topic());
        log.info("Partition  : {}", record.partition());
        log.info("Offset     : {}", record.offset());
        log.info("Key        : {}", record.key());
        log.info("Value      : {}", record.value());
        log.info("Is Committed: Yes (read_committed)");
        log.info("========================================");

        // Process message
        processMessage(record.key(), record.value());
    }

    private void processMessage(String key, String value) {

        log.info("Processing transactional message: {} - {}", key, value);

        // Business logic
    }

}
```

---

# 5. Read-Process-Write Pattern

One of the most common transactional patterns.

## Architecture

```text
        CONSUMER              PROCESSOR              PRODUCER
           │                      │                      │
           │  Read Message        │                      │
           │◄─────────────────────│                      │
           │                      │                      │
           │  Process Message     │                      │
           │─────────────────────►│                      │
           │                      │                      │
           │                      │  Send Result         │
           │                      │─────────────────────►│
           │                      │                      │
           │  Commit Offset       │                      │
           │─────────────────────►│                      │
           │                      │                      │
```

---

## Read-Process-Write Service

**File:** `ReadProcessWriteService.java`

```java
package edu.anant.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class ReadProcessWriteService {

    private static final String INPUT_TOPIC = "input-topic";
    private static final String OUTPUT_TOPIC = "output-topic";

    private final KafkaTemplate<String, String> kafkaTemplate;

    @KafkaListener(
        topics = INPUT_TOPIC,
        groupId = "read-process-write-group",
        containerFactory = "transactionalKafkaListenerContainerFactory"
    )
    @Transactional
    public void consumeAndProduce(ConsumerRecord<String, String> record) {

        log.info("========================================");
        log.info("READ-PROCESS-WRITE START");
        log.info("Input Topic   : {}", record.topic());
        log.info("Input Key     : {}", record.key());
        log.info("Input Value   : {}", record.value());

        try {

            // Process the message
            String processedValue = processMessage(record.value());

            log.info("Processed Value: {}", processedValue);

            // Send to output topic (same transaction)
            kafkaTemplate.send(OUTPUT_TOPIC, record.key(), processedValue)
                .whenComplete((result, exception) -> {

                    if (exception != null) {
                        log.error("Failed to send to output topic: {}", 
                            exception.getMessage());
                        return;
                    }

                    log.info("========================================");
                    log.info("READ-PROCESS-WRITE SUCCESS");
                    log.info("Output Topic  : {}", result.getRecordMetadata().topic());
                    log.info("Output Offset : {}", result.getRecordMetadata().offset());
                    log.info("========================================");
                });

        } catch (Exception e) {

            log.error("========================================");
            log.error("READ-PROCESS-WRITE FAILED");
            log.error("Error: {}", e.getMessage());
            log.error("Transaction will be rolled back");
            log.error("========================================");

            throw e; // Rollback transaction
        }
    }

    private String processMessage(String value) {

        // Business logic
        log.info("Processing message: {}", value);

        // Example: Transform message
        return "PROCESSED: " + value.toUpperCase();
    }

}
```

---

# 6. Write-Write Pattern (Multiple Topics)

Write to multiple topics atomically.

## Architecture

```text
        PRODUCER
           │
           │  Begin Transaction
           │
           ├──► Topic A (Message 1) [pending]
           │
           ├──► Topic B (Message 2) [pending]
           │
           ├──► Topic C (Message 3) [pending]
           │
           │  Commit Transaction
           │
           └──► All messages visible
```

---

## Write-Write Service

**File:** `WriteWriteService.java`

```java
package edu.anant.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class WriteWriteService {

    private static final String TOPIC_ORDERS = "orders-topic";
    private static final String TOPIC_INVENTORY = "inventory-topic";
    private static final String TOPIC_NOTIFICATIONS = "notifications-topic";

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Transactional
    public void processOrder(String orderId, 
                             String orderDetails,
                             String inventoryUpdate,
                             String notification) {

        log.info("========================================");
        log.info("WRITE-WRITE TRANSACTION START");
        log.info("Order ID: {}", orderId);

        try {

            // Write to Orders Topic
            log.info("Writing to Orders Topic: {}", orderDetails);
            kafkaTemplate.send(TOPIC_ORDERS, orderId, orderDetails);

            // Write to Inventory Topic
            log.info("Writing to Inventory Topic: {}", inventoryUpdate);
            kafkaTemplate.send(TOPIC_INVENTORY, orderId, inventoryUpdate);

            // Write to Notifications Topic
            log.info("Writing to Notifications Topic: {}", notification);
            kafkaTemplate.send(TOPIC_NOTIFICATIONS, orderId, notification);

            log.info("========================================");
            log.info("WRITE-WRITE TRANSACTION COMMITTED");
            log.info("All topics updated atomically");
            log.info("========================================");

        } catch (Exception e) {

            log.error("========================================");
            log.error("WRITE-WRITE TRANSACTION FAILED");
            log.error("Error: {}", e.getMessage());
            log.error("All writes will be rolled back");
            log.error("========================================");

            throw e; // Rollback all writes
        }
    }

}
```

---

# 7. Transaction Configuration

## Producer Transaction Settings

```properties
# Transactional ID (required)
spring.kafka.producer.transaction-id-prefix=my-tx-

# Enable idempotence (required for transactions)
spring.kafka.producer.properties.enable.idempotence=true

# Acknowledgements (recommended: all)
spring.kafka.producer.acks=all

# Retries (recommended: max)
spring.kafka.producer.retries=2147483647

# Max in-flight requests (must be <= 5 for idempotence)
spring.kafka.producer.properties.max.in.flight.requests.per.connection=5

# Transaction timeout
spring.kafka.producer.properties.transaction.timeout.ms=60000

# Delivery timeout
spring.kafka.producer.properties.delivery.timeout.ms=120000
```

---

## Consumer Transaction Settings

```properties
# Isolation level
spring.kafka.consumer.isolation-level=read_committed

# Consumer group
spring.kafka.consumer.group-id=transactional-consumer-group

# Auto offset reset
spring.kafka.consumer.auto-offset-reset=earliest

# Deserializers
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer
```

---

# 8. Common Pitfalls and Solutions

## Pitfall 1: Transaction Timeout

**Problem:**

```text
org.apache.kafka.common.errors.ProducerFencedException: 
Transaction timeout exceeded
```

**Solution:**

```properties
# Increase transaction timeout
spring.kafka.producer.properties.transaction.timeout.ms=120000

# Reduce processing time
# Optimize business logic
```

---

## Pitfall 2: Zombie Fencing

**Problem:**

```text
org.apache.kafka.common.errors.ProducerFencedException: 
The producer attempted to use a transactional id which is currently owned by another producer instance
```

**Solution:**

```properties
# Use unique transactional IDs per instance
spring.kafka.producer.transaction-id-prefix=${spring.application.name}-${server.port}-

# Or use UUID
spring.kafka.producer.transaction-id-prefix=my-app-${random.uuid}
```

---

## Pitfall 3: Mixing Transactional and Non-Transactional Producers

**Problem:**

Using same KafkaTemplate for both transactional and non-transactional operations.

**Solution:**

```java
// Separate KafkaTemplate beans
@Bean("transactionalKafkaTemplate")
public KafkaTemplate<String, String> transactionalKafkaTemplate() {
    // Transactional configuration
}

@Bean("nonTransactionalKafkaTemplate")
public KafkaTemplate<String, String> nonTransactionalKafkaTemplate() {
    // Non-transactional configuration
}
```

---

## Pitfall 4: Not Using read_committed

**Problem:**

Consumer sees uncommitted messages that may be aborted.

**Solution:**

```properties
spring.kafka.consumer.isolation-level=read_committed
```

---

## Pitfall 5: Long-Running Transactions

**Problem:**

Transactions take too long and timeout.

**Solution:**

```properties
# Increase timeout
spring.kafka.producer.properties.transaction.timeout.ms=300000

# Or optimize processing
// Break into smaller transactions
// Process in batches
```

---

# 9. Production Best Practices

## Transactional ID Strategy

| Strategy | Example | Use Case |
|---|---|---|
| **Static ID** | `payment-service-tx` | Single instance |
| **Instance-based** | `payment-service-${server.port}-tx` | Multiple instances |
| **UUID-based** | `payment-service-${random.uuid}` | Dynamic scaling |
| **Partition-based** | `payment-service-partition-${partition}` | Partition-specific |

---

## Monitoring Transactions

Track these metrics:

- Transaction success rate
- Transaction failure rate
- Average transaction duration
- Transaction timeout count
- Zombie fencing events
- Aborted transaction count

---

## Error Handling

```java
@Transactional
public void processWithRetry(String key, String value) {

    int maxRetries = 3;
    int retryCount = 0;

    while (retryCount < maxRetries) {

        try {

            kafkaTemplate.send(TOPIC, key, value).get();
            return; // Success

        } catch (Exception e) {

            retryCount++;

            if (retryCount >= maxRetries) {

                log.error("Transaction failed after {} retries", maxRetries);

                // Save to database for manual processing
                // Send alert
                // Log for investigation

                throw e;
            }

            log.warn("Transaction failed, retrying... ({}/{})", 
                retryCount, maxRetries);
        }
    }
}
```

---

## Performance Optimization

| Optimization | Impact |
|---|---|
| Batch messages in transaction | Better throughput |
| Minimize transaction scope | Reduce timeout risk |
| Use appropriate timeout | Balance reliability vs performance |
| Monitor transaction duration | Identify bottlenecks |
| Use connection pooling | Reduce connection overhead |

---

# 10. Testing Transactions

## Unit Test for Transactions

```java
@SpringBootTest
@Transactional
public class TransactionalProducerTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private TransactionalProducerService producerService;

    @Test
    public void testTransactionalSend() {

        // Arrange
        String key = "test-key";
        String messageA = "message-a";
        String messageB = "message-b";

        // Act
        producerService.sendToMultipleTopics(key, messageA, messageB);

        // Assert
        // Verify both messages sent
        // Verify transaction committed
    }

    @Test
    public void testTransactionalRollback() {

        // Arrange
        String key = "test-key";
        String messageA = "message-a";
        String messageB = null; // Will cause failure

        // Act & Assert
        assertThrows(Exception.class, () -> {
            producerService.sendToMultipleTopics(key, messageA, messageB);
        });

        // Verify transaction rolled back
        // Verify no messages sent
    }

}
```

---

# 11. Interview Questions

## Q1. What are Kafka transactions?

Kafka transactions enable atomic writes across multiple topics and partitions, ensuring all messages succeed or all fail together.

## Q2. What is the difference between at-most-once, at-least-once, and exactly-once?

- **At-most-once**: Message may be lost, never duplicated
- **At-least-once**: Message never lost, may be duplicated
- **Exactly-once**: Message never lost, never duplicated

## Q3. What configuration enables transactions in Kafka producer?

```properties
spring.kafka.producer.transaction-id-prefix=my-tx-
spring.kafka.producer.properties.enable.idempotence=true
```

## Q4. What is read_committed isolation level?

Consumer only sees committed messages, not uncommitted ones that may be aborted.

## Q5. What is zombie fencing?

When a new producer instance takes over a transactional ID from an old instance, fencing out the old one.

## Q6. Why is idempotence required for transactions?

Idempotence ensures no duplicate messages during retries, which is essential for exactly-once semantics.

## Q7. What is the read-process-write pattern?

Consumer reads message, processes it, and produces result, all in one transaction.

## Q8. What happens if a transaction times out?

The transaction is aborted, all pending messages are discarded, and ProducerFencedException is thrown.

## Q9. How do you handle transaction failures?

Catch exceptions, log errors, save for retry or manual processing, and send alerts.

## Q10. When should you use transactions?

For critical operations requiring atomicity across multiple topics, such as financial transactions, order processing, and data pipelines.

---

# 12. Chapter Checklist

- [x] What are Kafka transactions?
- [x] Exactly-once vs at-least-once vs at-most-once
- [x] Transactional producers
- [x] Transactional consumers
- [x] Read committed vs read uncommitted
- [x] Transaction configuration
- [x] Write-write pattern
- [x] Read-process-write pattern
- [x] Common pitfalls and solutions
- [x] Production best practices
- [x] Testing transactions
- [x] Interview questions

---

# Next Chapter

## Chapter — Kafka Performance Tuning and Optimization

Topics:

- Producer performance tuning
- Consumer performance tuning
- Broker performance tuning
- Batch configuration
- Compression strategies
- Partitioning strategies
- Memory optimization
- Network optimization
- Monitoring and metrics
- Performance testing