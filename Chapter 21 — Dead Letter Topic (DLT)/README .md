# Chapter — Dead Letter Topic (DLT)

> In this chapter, we will learn what Dead Letter Topics are, why they are essential for production Kafka applications, how to implement DLT with Spring Kafka, and best practices for error handling and message recovery.

---

## Learning Objectives

After completing this chapter, you will understand:

- What is a Dead Letter Topic (DLT)?
- Why DLT is needed in production systems
- Error handling strategies with DLT
- How to handle failed messages
- Recovery strategies for DLT messages
- Spring Kafka retry configuration
- Dead Letter Topic setup
- Dead Letter Publishing mechanisms
- Error Handler configuration
- Production best practices for DLT

---

# 1. What is Dead Letter Topic (DLT)?

A **Dead Letter Topic (DLT)** is a special Kafka topic where messages are sent when they cannot be processed successfully after all retry attempts.

```text
        CONSUMER
           │
           ▼
    ┌─────────────┐
    │   Process   │
    └─────────────┘
           │
           ├── Success ──► Commit Offset
           │
           ▼
    ┌─────────────┐
    │  Exception  │
    └─────────────┘
           │
           ▼
    ┌─────────────┐
    │   Retry     │
    │  (3 times)  │
    └─────────────┘
           │
           ├── Success ──► Commit Offset
           │
           ▼
    ┌─────────────┐
    │ Send to DLT │
    └─────────────┘
```

---

## DLT Architecture

```text
        Main Topic                Dead Letter Topic
    ┌─────────────┐            ┌─────────────────┐
    │  my-topic   │            │ my-topic.DLT    │
    │             │            │                 │
    │  Message 1  │──Success──►│                 │
    │  Message 2  │──Fail ────►│  Message 2      │
    │  Message 3  │──Success──►│                 │
    │  Message 4  │──Fail ────►│  Message 4      │
    └─────────────┘            └─────────────────┘
                                      │
                                      ▼
                              Manual Review &
                              Reprocessing
```

---

## DLT Message Structure

```text
Dead Letter Message
├── Original Message
├── Exception Information
├── Error Metadata
│   ├── Exception Class
│   ├── Exception Message
│   ├── Stack Trace
│   ├── Original Topic
│   ├── Original Partition
│   ├── Original Offset
│   └── Timestamp
└── Headers
    ├── exception-class
    ├── exception-message
    ├── original-topic
    ├── original-partition
    └── original-offset
```

---

# 2. Why DLT is Needed

## Without DLT

```text
Problem 1: Message Loss
Consumer ──► Exception ──► Message Lost
           (No recovery option)


Problem 2: Infinite Retry Loop
Consumer ──► Exception ──► Retry ──► Exception ──► Retry ──► ...
           (Consumer stuck forever)


Problem 3: No Visibility
- Don't know which messages failed
- Don't know why they failed
- Don't know how many failed
```

---

## With DLT

```text
Solution 1: No Message Loss
Consumer ──► Exception ──► Retry ──► Exception ──► DLT
           (Message preserved for recovery)


Solution 2: No Infinite Loops
Consumer ──► Exception ──► Retry (3x) ──► DLT
           (Consumer moves forward)


Solution 3: Full Visibility
- All failed messages in DLT
- Error information in headers
- Can analyze and reprocess
```

---

## Use Cases for DLT

| Use Case | Description |
|---|---|
| **Deserialization Errors** | Message format doesn't match expected schema |
| **Validation Errors** | Business validation fails |
| **Processing Errors** | Business logic throws exception |
| **External Service Failures** | Downstream service unavailable |
| **Data Quality Issues** | Corrupted or invalid data |
| **Schema Evolution** | Old messages incompatible with new code |

---

# 3. Error Handling with DLT

## Error Handling Flow

```text
        Message Received
               │
               ▼
        ┌─────────────┐
        │   Process   │
        └─────────────┘
               │
        ┌──────┴──────┐
        │             │
    Success       Exception
        │             │
        ▼             ▼
   Commit Offset  Error Handler
                        │
                        ▼
                 ┌─────────────┐
                 │   Retry?    │
                 └─────────────┘
                        │
                ┌───────┴───────┐
                │               │
              Yes             No
                │               │
                ▼               ▼
         Retry (Backoff)   Send to DLT
                │
                ▼
         ┌─────────────┐
         │   Success?  │
         └─────────────┘
                │
        ┌───────┴───────┐
        │               │
      Yes             No
        │               │
        ▼               ▼
   Commit Offset   Max Retries?
                        │
                ┌───────┴───────┐
                │               │
              Yes             No
                │               │
                ▼               ▼
           Send to DLT     Retry Again
```

---

## Error Classification

### Retryable Errors

| Error Type | Examples | Action |
|---|---|---|
| **Transient** | Network timeout, broker unavailable | Retry with backoff |
| **Temporary** | External service down, database connection pool exhausted | Retry with backoff |
| **Resource** | Memory temporarily low, disk I/O spike | Retry with backoff |

---

### Non-Retryable Errors

| Error Type | Examples | Action |
|---|---|---|
| **Data** | Invalid format, null value, schema mismatch | Send to DLT immediately |
| **Business** | Validation failed, business rule violation | Send to DLT immediately |
| **Logic** | NullPointerException, ClassCastException | Send to DLT immediately |

---

## Error Handler Decision Tree

```text
        Exception Occurred
               │
               ▼
        ┌─────────────┐
        │ Is it       │
        │ retryable?  │
        └─────────────┘
               │
        ┌──────┴──────┐
        │             │
       Yes           No
        │             │
        ▼             ▼
   Retry Count    Send to DLT
   < Max?          Immediately
        │
   ┌────┴────┐
   │         │
  Yes       No
   │         │
   ▼         ▼
 Retry    Send to DLT
```

---

# 4. Failed Messages

## Types of Failed Messages

### Type 1: Deserialization Failure

```java
// Expected: {"id": 101, "name": "Anant"}
// Received: {"id": "ABC", "name": "Anant"}

// Exception:
com.fasterxml.jackson.databind.InputMismatchException:
Cannot deserialize value of type `java.lang.Long` from String "ABC"
```

**Action:** Send to DLT immediately (non-retryable)

---

### Type 2: Validation Failure

```java
// Received: {"id": null, "name": ""}

// Validation:
if (employee.getId() == null) {
    throw new IllegalArgumentException("ID cannot be null");
}

if (employee.getName().isEmpty()) {
    throw new IllegalArgumentException("Name cannot be empty");
}
```

**Action:** Send to DLT immediately (non-retryable)

---

### Type 3: Processing Failure

```java
// External service call fails
paymentService.processPayment(order);

// Exception:
java.net.ConnectException:
Connection refused to remote service
```

**Action:** Retry with backoff, then send to DLT if persists

---

### Type 4: Business Logic Failure

```java
// Business rule violation
if (order.getAmount() > customer.getCreditLimit()) {
    throw new BusinessException("Credit limit exceeded");
}
```

**Action:** Send to DLT immediately (non-retryable)

---

## Message Failure Analysis

```text
Failed Message Analysis
├── What failed?
│   ├── Message content
│   ├── Message headers
│   └── Message metadata
├── Why did it fail?
│   ├── Exception type
│   ├── Exception message
│   └── Stack trace
├── When did it fail?
│   ├── Timestamp
│   ├── Consumer group
│   └── Consumer instance
└── Where did it fail?
    ├── Topic
    ├── Partition
    └── Offset
```

---

# 5. Recovery Strategy

## Recovery Options

### Option 1: Manual Reprocessing

```text
DLT Consumer
    │
    ▼
Review Message
    │
    ▼
Fix Root Cause
    │
    ▼
Manually Resend to Main Topic
```

**Use Cases:**

- Data quality issues
- Schema evolution problems
- One-off failures

---

### Option 2: Automatic Reprocessing

```text
DLT Consumer
    │
    ▼
Analyze Error
    │
    ▼
If Fixable ──► Resend to Main Topic
    │
    ▼
If Not Fixable ──► Archive
```

**Use Cases:**

- Temporary service outages (now resolved)
- Transient errors that have cleared

---

### Option 3: Hybrid Approach

```text
DLT Consumer
    │
    ▼
Categorize Error
    │
    ├── Retryable Error ──► Auto Retry
    │
    ├── Data Error ──► Manual Review
    │
    └── Business Error ──► Archive with Alert
```

**Use Cases:**

- Production systems with mixed error types
- Systems with clear error classification

---

## Recovery Workflow

```text
    ┌─────────────────┐
    │  DLT Message    │
    └─────────────────┘
           │
           ▼
    ┌─────────────────┐
    │  Analyze Error  │
    └─────────────────┘
           │
           ├── Data Error ──► Fix Data ──► Resend
           │
           ├── Transient Error ──► Check Service ──► Resend
           │
           ├── Business Error ──► Manual Review ──► Decision
           │
           └── Unknown Error ──► Escalate ──► Archive
```

---

# 6. Spring Kafka Retry with DLT

## @RetryableTopic with DLT

**File:** `RetryableTopicWithDLT.java`

```java
package edu.anant.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class RetryableTopicWithDLT {

    @RetryableTopic(
        attempts = "5",
        backOff = @org.springframework.kafka.annotation.BackOff(
            delay = 1000,
            multiplier = 2.0,
            maxDelay = 10000
        ),
        dltStrategy = DltStrategy.ALWAYS_SEND,
        include = {
            RuntimeException.class,
            IllegalStateException.class
        },
        exclude = {
            IllegalArgumentException.class,
            NullPointerException.class
        }
    )
    @KafkaListener(
        topics = "orders-topic",
        groupId = "orders-consumer-group"
    )
    public void consumeOrders(ConsumerRecord<String, String> record) {

        log.info("Processing order: {}", record.value());

        String value = record.value();

        // Validate message
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("Empty message");
        }

        // Process order
        processOrder(value);

        log.info("Order processed successfully: {}", value);
    }

    private void processOrder(String orderData) {

        // Business logic that may fail
        if (orderData.contains("fail")) {
            throw new RuntimeException("Simulated processing failure");
        }

        // Process order...
    }

}
```

---

## DltStrategy Options

| Strategy | Description |
|---|---|
| `ALWAYS_SEND` | Always send to DLT after max retries |
| `SEND_ON_FAILURE` | Send to DLT only on final failure |
| `FAIL_ON_ERROR` | Don't send to DLT, just fail |
| `NO_DLT` | Disable DLT completely |

---

# 7. Dead Letter Topic Setup

## DLT Topic Configuration

**File:** `DLTTopicConfig.java`

```java
package edu.anant.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class DLTTopicConfig {

    // Orders DLT
    @Bean
    public NewTopic ordersDLT() {

        return TopicBuilder
            .name("orders-topic.DLT")
            .partitions(3)
            .replicas(3)
            .build();
    }

    // Payments DLT
    @Bean
    public NewTopic paymentsDLT() {

        return TopicBuilder
            .name("payments-topic.DLT")
            .partitions(3)
            .replicas(3)
            .build();
    }

    // Notifications DLT
    @Bean
    public NewTopic notificationsDLT() {

        return TopicBuilder
            .name("notifications-topic.DLT")
            .partitions(3)
            .replicas(3)
            .build();
    }

    // Generic DLT (fallback)
    @Bean
    public NewTopic genericDLT() {

        return TopicBuilder
            .name("generic.DLT")
            .partitions(3)
            .replicas(3)
            .build();
    }

}
```

---

## DLT Topic with Retention

**File:** `DLTTopicConfigWithRetention.java`

```java
package edu.anant.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class DLTTopicConfigWithRetention {

    @Bean
    public NewTopic ordersDLT() {

        Map<String, String> configs = new HashMap<>();

        // Retain DLT messages for 30 days for analysis
        configs.put("retention.ms", String.valueOf(30L * 24 * 60 * 60 * 1000));

        return TopicBuilder
            .name("orders-topic.DLT")
            .partitions(3)
            .replicas(3)
            .configs(configs)
            .build();
    }

    @Bean
    public NewTopic paymentsDLT() {

        Map<String, String> configs = new HashMap<>();

        // Retain payment DLT messages for 90 days (compliance)
        configs.put("retention.ms", String.valueOf(90L * 24 * 60 * 60 * 1000));

        return TopicBuilder
            .name("payments-topic.DLT")
            .partitions(3)
            .replicas(3)
            .configs(configs)
            .build();
    }

}
```

---

# 8. Dead Letter Publishing

## DLT Publisher Service

**File:** `DLTPublisherService.java`

```java
package edu.anant.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class DLTPublisherService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void publishToDLT(String originalTopic,
                             String key,
                             String value,
                             Throwable exception,
                             org.apache.kafka.clients.consumer.ConsumerRecord<?, ?> record) {

        String dltTopic = originalTopic + ".DLT";

        log.error("========================================");
        log.error("PUBLISHING TO DLT");
        log.error("DLT Topic   : {}", dltTopic);
        log.error("Original    : {}", originalTopic);
        log.error("Key         : {}", key);
        log.error("Exception   : {}", exception.getClass().getSimpleName());
        log.error("Message     : {}", exception.getMessage());
        log.error("========================================");

        // Create DLT message with original content
        ProducerRecord<String, String> dltRecord = 
            new ProducerRecord<>(dltTopic, key, value);

        // Add error metadata as headers
        addErrorHeaders(dltRecord, exception, record);

        // Send to DLT
        kafkaTemplate.send(dltRecord)
            .whenComplete((result, ex) -> {

                if (ex != null) {
                    log.error("Failed to send to DLT: {}", ex.getMessage());

                    // Critical: DLT send failed
                    // Log to file, send alert, etc.
                    handleDLTSendFailure(originalTopic, key, value, exception, ex);

                } else {
                    log.info("Successfully sent to DLT: {} - Offset: {}", 
                        dltTopic, result.getRecordMetadata().offset());
                }
            });
    }

    private void addErrorHeaders(ProducerRecord<String, String> record,
                                  Throwable exception,
                                  org.apache.kafka.clients.consumer.ConsumerRecord<?, ?> consumerRecord) {

        // Exception information
        record.headers().add("exception-class", 
            exception.getClass().getName().getBytes());

        record.headers().add("exception-message", 
            exception.getMessage().getBytes());

        // Stack trace (truncated to avoid large headers)
        String stackTrace = getTruncatedStackTrace(exception);
        record.headers().add("exception-stacktrace", 
            stackTrace.getBytes());

        // Original message location
        record.headers().add("original-topic", 
            consumerRecord.topic().getBytes());

        record.headers().add("original-partition", 
            String.valueOf(consumerRecord.partition()).getBytes());

        record.headers().add("original-offset", 
            String.valueOf(consumerRecord.offset()).getBytes());

        record.headers().add("original-key", 
            (consumerRecord.key() != null ? consumerRecord.key() : "null").getBytes());

        // Timestamp
        record.headers().add("dlt-timestamp", 
            String.valueOf(System.currentTimeMillis()).getBytes());

        record.headers().add("original-timestamp", 
            String.valueOf(consumerRecord.timestamp()).getBytes());

        // Consumer group
        record.headers().add("consumer-group", 
            "orders-consumer-group".getBytes());
    }

    private String getTruncatedStackTrace(Throwable exception) {

        StringBuilder sb = new StringBuilder();

        // Get first 10 stack trace lines
        int count = 0;
        for (StackTraceElement element : exception.getStackTrace()) {
            if (count >= 10) {
                sb.append("... (truncated)");
                break;
            }
            sb.append(element.toString()).append("\n");
            count++;
        }

        return sb.toString();
    }

    private void handleDLTSendFailure(String originalTopic,
                                       String key,
                                       String value,
                                       Throwable originalException,
                                       Throwable dltException) {

        log.error("========================================");
        log.error("CRITICAL: DLT SEND FAILED");
        log.error("This message could be lost!");
        log.error("Original Topic: {}", originalTopic);
        log.error("Key           : {}", key);
        log.error("Value         : {}", value);
        log.error("Original Error: {}", originalException.getMessage());
        log.error("DLT Error     : {}", dltException.getMessage());
        log.error("========================================");

        // Options:
        // 1. Save to database for recovery
        // 2. Write to local file
        // 3. Send emergency alert
        // 4. Log to separate error tracking system

        // Example: Save to database
        // dltFailureRepository.save(new DLTFailure(...));

        // Example: Send alert
        // alertService.sendCriticalAlert("DLT Send Failed", ...);
    }

}
```

---

## Custom DLT Publisher with Retry

**File:** `ReliableDLTPublisherService.java`

```java
package edu.anant.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class ReliableDLTPublisherService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Retryable(
        maxAttempts = 3,
        backoff = @Backoff(delay = 1000, multiplier = 2.0)
    )
    public void publishToDLTReliable(String originalTopic,
                                      String key,
                                      String value,
                                      Throwable exception,
                                      org.apache.kafka.clients.consumer.ConsumerRecord<?, ?> record) {

        String dltTopic = originalTopic + ".DLT";

        ProducerRecord<String, String> dltRecord = 
            new ProducerRecord<>(dltTopic, key, value);

        addErrorHeaders(dltRecord, exception, record);

        // Send with retry
        kafkaTemplate.send(dltRecord).get(); // Blocking send with retry

        log.info("Successfully sent to DLT with retry: {}", dltTopic);
    }

    private void addErrorHeaders(ProducerRecord<String, String> record,
                                  Throwable exception,
                                  org.apache.kafka.clients.consumer.ConsumerRecord<?, ?> consumerRecord) {

        record.headers().add("exception-class", 
            exception.getClass().getName().getBytes());
        record.headers().add("exception-message", 
            exception.getMessage().getBytes());
        record.headers().add("original-topic", 
            consumerRecord.topic().getBytes());
        record.headers().add("original-offset", 
            String.valueOf(consumerRecord.offset()).getBytes());
        record.headers().add("dlt-timestamp", 
            String.valueOf(System.currentTimeMillis()).getBytes());
    }

}
```

---

# 9. Error Handler Configuration

## DefaultErrorHandler with DLT

**File:** `DLTErrorHandlerConfig.java`

```java
package edu.anant.config;

import edu.anant.service.DLTPublisherService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class DLTErrorHandlerConfig {

    private final DLTPublisherService dltPublisherService;

    @Bean
    public CommonErrorHandler dltErrorHandler() {

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
            // Recovery callback (called after max retries)
            (record, exception) -> {

                log.error("========================================");
                log.error("MESSAGE RECOVERY - SEND TO DLT");
                log.error("Topic      : {}", record.topic());
                log.error("Partition  : {}", record.partition());
                log.error("Offset     : {}", record.offset());
                log.error("Key        : {}", record.key());
                log.error("Value      : {}", record.value());
                log.error("Exception  : {}", exception.getClass().getName());
                log.error("Message    : {}", exception.getMessage());
                log.error("========================================");

                // Send to DLT
                dltPublisherService.publishToDLT(
                    record.topic(),
                    record.key() != null ? record.key().toString() : null,
                    record.value() != null ? record.value().toString() : null,
                    exception,
                    record
                );

            },
            // Backoff configuration
            new FixedBackOff(1000L, 3L) // 1 second delay, 3 retries
        );

        // Configure non-retryable exceptions (send to DLT immediately)
        errorHandler.addNotRetryableExceptions(
            IllegalArgumentException.class,
            NullPointerException.class,
            ClassCastException.class
        );

        return errorHandler;
    }

}
```

---

## Exponential Backoff with DLT

**File:** `ExponentialDLTErrorHandlerConfig.java`

```java
package edu.anant.config;

import edu.anant.service.DLTPublisherService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class ExponentialDLTErrorHandlerConfig {

    private final DLTPublisherService dltPublisherService;

    @Bean
    public CommonErrorHandler exponentialDLTErrorHandler() {

        // Exponential backoff: 1s, 2s, 4s, 8s, 10s (max)
        ExponentialBackOff backOff = new ExponentialBackOff();
        backOff.setInitialInterval(1000);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(10000);
        backOff.setMaxAttempts(5);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
            (record, exception) -> {

                log.error("========================================");
                log.error("MESSAGE FAILED AFTER EXPONENTIAL RETRIES");
                log.error("Sending to DLT: {}", record.topic());
                log.error("========================================");

                dltPublisherService.publishToDLT(
                    record.topic(),
                    record.key() != null ? record.key().toString() : null,
                    record.value() != null ? record.value().toString() : null,
                    exception,
                    record
                );

            },
            backOff
        );

        errorHandler.addNotRetryableExceptions(
            IllegalArgumentException.class,
            NullPointerException.class
        );

        return errorHandler;
    }

}
```

---

## Listener Container Factory with DLT

**File:** `KafkaListenerConfigWithDLT.java`

```java
package edu.anant.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;

import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class KafkaListenerConfigWithDLT {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    private final CommonErrorHandler dltErrorHandler;

    @Bean
    public DefaultKafkaConsumerFactory<String, String> 
    consumerFactoryWithDLT() {

        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-with-dlt-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> 
    kafkaListenerContainerFactoryWithDLT() {

        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactoryWithDLT());
        factory.setCommonErrorHandler(dltErrorHandler);
        factory.setConcurrency(3);

        return factory;
    }

}
```

---

## Consumer Service with DLT

**File:** `ConsumerWithDLTService.java`

```java
package edu.anant.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ConsumerWithDLTService {

    @KafkaListener(
        topics = "orders-topic",
        groupId = "consumer-with-dlt-group",
        containerFactory = "kafkaListenerContainerFactoryWithDLT"
    )
    public void consumeOrders(ConsumerRecord<String, String> record) {

        log.info("========================================");
        log.info("PROCESSING ORDER");
        log.info("Topic      : {}", record.topic());
        log.info("Partition  : {}", record.partition());
        log.info("Offset     : {}", record.offset());
        log.info("Key        : {}", record.key());
        log.info("Value      : {}", record.value());
        log.info("========================================");

        String value = record.value();

        // Validate
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("Empty order data");
        }

        // Process order
        processOrder(value);

        log.info("Order processed successfully: {}", value);
    }

    private void processOrder(String orderData) {

        // Simulate processing that may fail
        if (orderData.contains("error")) {
            throw new RuntimeException("Processing error for order: " + orderData);
        }

        // Business logic
        log.info("Processing order: {}", orderData);
    }

}
```

---

# 10. DLT Consumer for Recovery

## DLT Consumer Service

**File:** `DLTConsumerService.java`

```java
package edu.anant.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class DLTConsumerService {

    @KafkaListener(
        topics = "orders-topic.DLT",
        groupId = "dlt-consumer-group"
    )
    public void consumeDLT(ConsumerRecord<String, String> record) {

        log.error("========================================");
        log.error("DLT MESSAGE RECEIVED");
        log.error("DLT Topic    : {}", record.topic());
        log.error("Partition    : {}", record.partition());
        log.error("Offset       : {}", record.offset());
        log.error("Key          : {}", record.key());
        log.error("Value        : {}", record.value());
        log.error("========================================");

        // Extract error metadata from headers
        record.headers().forEach(header -> {

            String headerKey = header.key();
            String headerValue = new String(header.value());

            log.error("Header - {} : {}", headerKey, headerValue);
        });

        // Analyze and decide action
        analyzeAndReprocess(record);
    }

    private void analyzeAndReprocess(ConsumerRecord<String, String> record) {

        // Extract exception class from headers
        String exceptionClass = getHeader(record, "exception-class");
        String originalTopic = getHeader(record, "original-topic");
        String originalOffset = getHeader(record, "original-offset");

        log.info("Analyzing DLT message from {} (offset {})", 
            originalTopic, originalOffset);

        // Categorize error
        if (exceptionClass != null) {

            if (exceptionClass.contains("IllegalArgumentException")) {
                log.warn("Data validation error - requires manual fix");
                // Action: Notify data team
            }

            else if (exceptionClass.contains("RuntimeException")) {
                log.warn("Processing error - may be retryable");
                // Action: Check if issue resolved, then reprocess
            }

            else if (exceptionClass.contains("ConnectException")) {
                log.warn("External service error - check service status");
                // Action: Check external service, then reprocess if available
            }

            else {
                log.error("Unknown error type - requires investigation");
                // Action: Escalate to engineering team
            }
        }

        // Decision: Reprocess, Archive, or Escalate
        decideAction(record, exceptionClass);
    }

    private void decideAction(ConsumerRecord<String, String> record, 
                              String exceptionClass) {

        // Example: Auto-reprocess transient errors
        if (exceptionClass != null && exceptionClass.contains("ConnectException")) {

            log.info("Attempting automatic reprocessing...");

            try {

                // Check if external service is now available
                if (isExternalServiceAvailable()) {

                    // Reprocess message
                    reprocessMessage(record);
                    log.info("Message reprocessed successfully");

                } else {

                    log.warn("External service still unavailable, keeping in DLT");
                }

            } catch (Exception e) {

                log.error("Reprocessing failed: {}", e.getMessage());
            }
        }

        // For other errors, archive or escalate
        else {

            log.info("Archiving message for manual review");
            archiveForManualReview(record);
        }
    }

    private boolean isExternalServiceAvailable() {

        // Check external service health
        // Return true if available
        return true;
    }

    private void reprocessMessage(ConsumerRecord<String, String> record) {

        // Send back to original topic
        // Or process directly
    }

    private void archiveForManualReview(ConsumerRecord<String, String> record) {

        // Save to database
        // Or write to file
        // Or send to ticketing system
    }

    private String getHeader(ConsumerRecord<String, String> record, String key) {

        return record.headers()
            .lastHeader(key)
            .map(h -> new String(h.value()))
            .orElse(null);
    }

}
```

---

# 11. Production Best Practices

## DLT Naming Convention

```text
Recommended Pattern:
<original-topic>.DLT

Examples:
orders-topic.DLT
payments-topic.DLT
notifications-topic.DLT
```

---

## DLT Retention Policy

| Topic Type | DLT Retention | Reason |
|---|---|---|
| **Orders** | 30 days | Business analysis |
| **Payments** | 90 days | Compliance requirement |
| **Notifications** | 7 days | Low priority |
| **Logs** | 1 day | Debug only |
| **Critical** | 365 days | Audit trail |

---

## DLT Monitoring

Track these metrics:

- DLT message count per topic
- DLT message rate (messages/hour)
- Most common error types
- Average time in DLT before resolution
- Reprocessing success rate
- DLT storage size

---

## DLT Alerting

| Condition | Alert Level | Action |
|---|---|---|
| DLT count > 100 | Warning | Notify team |
| DLT count > 1000 | Critical | Page on-call |
| DLT rate spike | Critical | Investigate immediately |
| DLT send failure | Critical | Emergency response |

---

## DLT Anti-Patterns

| Anti-Pattern | Problem | Solution |
|---|---|---|
| No DLT | Lost messages | Always configure DLT |
| DLT without monitoring | Unknown failures | Monitor DLT metrics |
| Infinite DLT retention | Storage issues | Set appropriate retention |
| No DLT consumer | Messages accumulate | Implement DLT consumer |
| Auto-reprocess all | May cause loops | Careful error classification |

---

# 12. Interview Questions

## Q1. What is a Dead Letter Topic?

A DLT is a Kafka topic where messages are sent after failing all retry attempts, preserving them for analysis and recovery.

---

## Q2. Why is DLT important?

DLT prevents message loss, provides visibility into failures, enables recovery, and helps with debugging and compliance.

---

## Q3. When should you send messages to DLT?

- After exhausting all retry attempts
- For non-retryable errors (validation, data quality)
- When manual intervention is needed

---

## Q4. What information should be included in DLT messages?

- Original message content
- Exception class and message
- Stack trace (truncated)
- Original topic, partition, offset
- Timestamps
- Consumer group information

---

## Q5. How do you configure DLT in Spring Kafka?

Use `DefaultErrorHandler` with a recovery callback that publishes failed messages to a DLT topic.

---

## Q6. What is the difference between retry and DLT?

- **Retry**: Attempt to process again (for transient errors)
- **DLT**: Store failed message for recovery (after retries exhausted or for permanent errors)

---

## Q7. How do you handle DLT messages?

- Analyze error type
- Fix root cause if possible
- Reprocess automatically or manually
- Archive for audit if needed

---

## Q8. What retention policy should DLT have?

Depends on business requirements: 7-30 days for general topics, 90-365 days for compliance-critical topics.

---

## Q9. How do you monitor DLT?

Track DLT message count, rate, error types, reprocessing success rate, and set up alerts for spikes.

---

## Q10. What happens if sending to DLT fails?

This is a critical failure. Log to separate system, save to database, send emergency alert, and investigate immediately.

---

# 13. Chapter Checklist

- [x] What is Dead Letter Topic (DLT)?
- [x] Why DLT is needed
- [x] Error handling strategies
- [x] Failed messages handling
- [x] Recovery strategies
- [x] Spring Kafka retry with DLT
- [x] Dead Letter Topic setup
- [x] Dead Letter Publishing
- [x] Error Handler configuration
- [x] DLT consumer for recovery
- [x] Production best practices
- [x] Interview questions

---

# Next Chapter

## Chapter — Kafka Monitoring and Observability

Topics:

- Key Kafka metrics
- Producer metrics
- Consumer metrics
- Broker metrics
- Monitoring tools (Prometheus, Grafana)
- Logging best practices
- Distributed tracing
- Alerting strategies
- Performance dashboards
- Production monitoring setup
