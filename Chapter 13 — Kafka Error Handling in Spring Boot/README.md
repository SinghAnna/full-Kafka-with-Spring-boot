# Chapter 13 — Kafka Error Handling in Spring Boot

> In this chapter, we will learn how to handle producer and consumer errors in Spring Kafka applications, including retries, backoff, dead letter topics, and production-grade error handling patterns.

---

## Learning Objectives

After completing this chapter, you will understand:

- Types of Kafka errors in Spring Boot
- Producer-side error handling
- Consumer-side error handling
- `ErrorHandler` and `DefaultErrorHandler`
- Retry mechanisms
- `BackOff` and `FixedBackOff`
- Dead Letter Topic (DLT)
- `@RetryableTopic`
- Recovery strategies
- Logging and monitoring errors
- Production best practices

---

# 1. Why Error Handling Matters

Kafka-based systems are asynchronous and distributed. Errors can occur at multiple points:

- Network failures
- Broker unavailability
- Serialization/Deserialization errors
- Schema mismatches
- Business logic exceptions
- Timeout issues
- Permission and authentication errors

Without proper error handling:

- Messages can be lost
- Consumers can stop processing
- Systems can behave inconsistently
- Debugging becomes difficult

Proper error handling ensures:

- No message loss
- System resilience
- Clear visibility into failures
- Automatic recovery where possible

---

# 2. Types of Kafka Errors

## Producer Errors

- Broker not available
- Topic not found
- Serialization failures
- Timeout during send
- Network connectivity issues
- Authorization failures

## Consumer Errors

- Deserialization failures
- Business logic exceptions
- Null pointer exceptions
- Database connection failures
- Timeout during processing
- Schema evolution issues

---

# 3. Producer Error Handling

## Using `whenComplete` Callback

Spring Kafka provides asynchronous callbacks for producer results.

```java
kafkaTemplate
    .send(TOPIC, key, message)
    .whenComplete(this::handleResult);
```

### Example Producer Service with Error Handling

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
public class ReliableProducerService {

    private static final String TOPIC = "reliable-topic";

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void send(String key, String message) {

        kafkaTemplate
            .send(TOPIC, key, message)
            .whenComplete(this::handleResult);
    }

    private void handleResult(
            SendResult<String, String> result,
            Throwable exception
    ) {

        if (exception != null) {

            log.error("========================================");
            log.error("Kafka Send Failed");
            log.error("Topic     : {}", TOPIC);
            log.error("Key       : {}", result != null ? 
                result.getProducerRecord().key() : "unknown");
            log.error("Message   : {}", result != null ? 
                result.getProducerRecord().value() : "unknown");
            log.error("Error     : {}", exception.getMessage());
            log.error("========================================");

            // Additional error handling logic
            // - Save to database for retry
            // - Send to monitoring system
            // - Trigger alert

            return;
        }

        log.info("========================================");
        log.info("Message Sent Successfully");
        log.info("Topic     : {}", result.getRecordMetadata().topic());
        log.info("Partition : {}", result.getRecordMetadata().partition());
        log.info("Offset    : {}", result.getRecordMetadata().offset());
        log.info("Timestamp : {}", result.getRecordMetadata().timestamp());
        log.info("========================================");
    }

}
```

---

## Producer Configuration for Reliability

```properties
# Producer Acknowledgments
spring.kafka.producer.acks=all

# Retry Configuration
spring.kafka.producer.retries=3

# Idempotence (Prevents Duplicate Messages)
spring.kafka.producer.properties.enable.idempotence=true

# Timeout Configuration
spring.kafka.producer.properties.request.timeout.ms=30000
spring.kafka.producer.properties.delivery.timeout.ms=120000
```

### Important Producer Settings

| Setting | Value | Purpose |
|---|---|---|
| `acks` | `all` | Wait for all replicas to acknowledge |
| `retries` | `3` | Retry failed sends |
| `enable.idempotence` | `true` | Prevent duplicate messages |
| `request.timeout.ms` | `30000` | Request timeout |
| `delivery.timeout.ms` | `120000` | Total delivery timeout |

---

# 4. Consumer Error Handling

Spring Kafka provides multiple ways to handle consumer errors.

## Default Behavior

By default, if a consumer throws an exception:

- The message is not acknowledged
- The consumer retries the same message
- This can lead to infinite loops for non-recoverable errors

---

## Custom Error Handler

### Step 1: Create Error Handler Configuration

**File:** `KafkaErrorHandlerConfig.java`

```java
package edu.anant.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Slf4j
@Configuration
public class KafkaErrorHandlerConfig {

    @Bean
    public CommonErrorHandler kafkaErrorHandler() {

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
            (record, exception) -> {

                // This is the recovery callback
                log.error("========================================");
                log.error("Message Processing Failed After Retries");
                log.error("Topic      : {}", record.topic());
                log.error("Partition  : {}", record.partition());
                log.error("Offset     : {}", record.offset());
                log.error("Key        : {}", record.key());
                log.error("Value      : {}", record.value());
                log.error("Exception  : {}", exception.getMessage());
                log.error("========================================");

                // Send to Dead Letter Topic
                // Or save to database for manual review

            },
            new FixedBackOff(1000L, 3L) // 1 second delay, 3 retries
        );

        // Skip certain exceptions that should not be retried
        errorHandler.addNotRetryableExceptions(
            IllegalArgumentException.class,
            NullPointerException.class
        );

        return errorHandler;
    }

}
```

---

## Step 2: Apply Error Handler to Listener Container

**File:** `KafkaConsumerConfig.java`

```java
package edu.anant.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    private final KafkaProperties kafkaProperties;
    private final CommonErrorHandler kafkaErrorHandler;

    public KafkaConsumerConfig(
            KafkaProperties kafkaProperties,
            CommonErrorHandler kafkaErrorHandler
    ) {
        this.kafkaProperties = kafkaProperties;
        this.kafkaErrorHandler = kafkaErrorHandler;
    }

    @Bean
    public DefaultKafkaConsumerFactory<String, String>
    consumerFactory() {

        Map<String, Object> props = new HashMap<>();

        props.put(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            kafkaProperties.getBootstrapServers().get(0)
        );

        props.put(
            ConsumerConfig.GROUP_ID_CONFIG,
            "error-handling-group"
        );

        props.put(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class
        );

        props.put(
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class
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
            String
            > kafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<
                String,
                String
                > factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());

        // Apply custom error handler
        factory.setCommonErrorHandler(kafkaErrorHandler);

        return factory;
    }

}
```

---

## Step 3: Consumer Service with Error Handling

**File:** `ErrorHandlingConsumerService.java`

```java
package edu.anant.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ErrorHandlingConsumerService {

    @KafkaListener(
        topics = "error-handling-topic",
        groupId = "error-handling-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, String> record) {

        log.info("Processing message: {}", record.value());

        String value = record.value();

        // Simulate business logic error
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("Empty message received");
        }

        // Simulate processing
        processMessage(value);

        log.info("Message processed successfully: {}", value);
    }

    private void processMessage(String message) {

        // Business logic here
        // This can throw exceptions

        if (message.contains("error")) {
            throw new RuntimeException("Simulated processing error");
        }

        // Additional processing logic
    }

}
```

---

# 5. Retry Configuration

## FixedBackOff

Retries with a fixed delay between attempts.

```java
new FixedBackOff(1000L, 3L)
```

- `1000L` = 1 second delay between retries
- `3L` = 3 retry attempts

## ExponentialBackOff

Retries with exponentially increasing delays.

```java
import org.springframework.util.backoff.ExponentialBackOff;

ExponentialBackOff backOff = new ExponentialBackOff();
backOff.setInitialInterval(1000); // 1 second
backOff.setMaxInterval(10000); // 10 seconds max
backOff.setMultiplier(2.0); // Double each time
backOff.setMaxAttempts(5);
```

This creates delays like:

```text
Attempt 1: 1 second
Attempt 2: 2 seconds
Attempt 3: 4 seconds
Attempt 4: 8 seconds
Attempt 5: 10 seconds (max reached)
```

---

# 6. Dead Letter Topic (DLT)

When a message fails after all retries, send it to a Dead Letter Topic for manual review.

## Step 1: Create DLT Topic

**File:** `DeadLetterTopicConfig.java`

```java
package edu.anant.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class DeadLetterTopicConfig {

    @Bean
    public NewTopic deadLetterTopic() {

        return TopicBuilder
            .name("error-handling-topic.DLT")
            .partitions(3)
            .replicas(1)
            .build();
    }

}
```

---

## Step 2: Send Failed Messages to DLT

**File:** `KafkaErrorHandlerConfig.java` (Updated)

```java
package edu.anant.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class KafkaErrorHandlerConfig {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Bean
    public CommonErrorHandler kafkaErrorHandler() {

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
            (record, exception) -> {

                // Send to Dead Letter Topic
                sendToDeadLetterTopic(record, exception);

            },
            new FixedBackOff(1000L, 3L)
        );

        errorHandler.addNotRetryableExceptions(
            IllegalArgumentException.class,
            NullPointerException.class
        );

        return errorHandler;
    }

    private void sendToDeadLetterTopic(
            ConsumerRecord<?, ?> record,
            Throwable exception
    ) {

        String dltTopic = record.topic() + ".DLT";

        ProducerRecord<String, String> dltRecord =
            new ProducerRecord<>(
                dltTopic,
                record.key() != null ? record.key().toString() : null,
                record.value() != null ? record.value().toString() : null
            );

        // Add error metadata as headers
        dltRecord.headers().add(
            "exception-class",
            exception.getClass().getName().getBytes()
        );

        dltRecord.headers().add(
            "exception-message",
            exception.getMessage().getBytes()
        );

        dltRecord.headers().add(
            "original-topic",
            record.topic().getBytes()
        );

        dltRecord.headers().add(
            "original-partition",
            String.valueOf(record.partition()).getBytes()
        );

        dltRecord.headers().add(
            "original-offset",
            String.valueOf(record.offset()).getBytes()
        );

        kafkaTemplate.send(dltRecord)
            .whenComplete((result, ex) -> {

                if (ex != null) {
                    log.error("Failed to send to DLT: {}", ex.getMessage());
                } else {
                    log.info("Message sent to DLT: {}", dltTopic);
                }

            });
    }

}
```

---

## Step 3: DLT Consumer for Manual Review

**File:** `DeadLetterConsumerService.java`

```java
package edu.anant.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class DeadLetterConsumerService {

    @KafkaListener(
        topics = "error-handling-topic.DLT",
        groupId = "dlt-consumer-group"
    )
    public void consumeDeadLetter(ConsumerRecord<String, String> record) {

        log.error("========================================");
        log.error("DEAD LETTER MESSAGE RECEIVED");
        log.error("Topic      : {}", record.topic());
        log.error("Partition  : {}", record.partition());
        log.error("Offset     : {}", record.offset());
        log.error("Key        : {}", record.key());
        log.error("Value      : {}", record.value());

        // Extract error headers
        record.headers().forEach(header -> {

            String headerKey = header.key();
            String headerValue = new String(header.value());

            log.error("Header - {} : {}", headerKey, headerValue);

        });

        log.error("========================================");

        // Options:
        // 1. Save to database for manual review
        // 2. Send alert to monitoring system
        // 3. Attempt manual reprocessing
        // 4. Archive for audit purposes
    }

}
```

---

# 7. @RetryableTopic Annotation

Spring Kafka 2.7+ provides `@RetryableTopic` for simpler retry configuration.

## Example with @RetryableTopic

**File:** `RetryableConsumerService.java`

```java
package edu.anant.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicConfigurer;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class RetryableConsumerService {

    @RetryableTopic(
        attempts = "5",
        backOff = @org.springframework.kafka.annotation.BackOff(
            delay = 1000,
            multiplier = 2.0,
            maxDelay = 10000
        ),
        dltStrategy = org.springframework.kafka.retrytopic.DltStrategy.FAIL_ON_ERROR,
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
        topics = "retryable-topic",
        groupId = "retryable-group"
    )
    public void consume(ConsumerRecord<String, String> record) {

        log.info("Processing: {}", record.value());

        String value = record.value();

        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("Empty value");
        }

        if (value.contains("fail")) {
            throw new RuntimeException("Simulated failure");
        }

        log.info("Processed successfully: {}", value);
    }

}
```

---

## @RetryableTopic Configuration

| Attribute | Description | Example |
|---|---|---|
| `attempts` | Number of retry attempts | `"5"` |
| `backOff.delay` | Initial delay in ms | `1000` |
| `backOff.multiplier` | Delay multiplier | `2.0` |
| `backOff.maxDelay` | Maximum delay in ms | `10000` |
| `dltStrategy` | What to do with DLT | `FAIL_ON_ERROR`, `ALWAYS_SEND`, etc. |
| `include` | Exceptions to retry | `{RuntimeException.class}` |
| `exclude` | Exceptions NOT to retry | `{IllegalArgumentException.class}` |

---

# 8. Exception Classification

Not all exceptions should be retried.

## Retryable Exceptions

- Network timeouts
- Temporary broker unavailability
- Database connection pool exhaustion
- External service temporary failures

## Non-Retryable Exceptions

- `IllegalArgumentException`
- `NullPointerException`
- `ClassCastException`
- Schema validation errors
- Business logic validation errors

### Configure Non-Retryable Exceptions

```java
errorHandler.addNotRetryableExceptions(
    IllegalArgumentException.class,
    NullPointerException.class,
    ClassCastException.class
);
```

---

# 9. Error Logging and Monitoring

## Structured Error Logging

```java
log.error("========================================");
log.error("KAFKA ERROR");
log.error("Timestamp   : {}", System.currentTimeMillis());
log.error("Topic       : {}", record.topic());
log.error("Partition   : {}", record.partition());
log.error("Offset      : {}", record.offset());
log.error("Key         : {}", record.key());
log.error("Value       : {}", record.value());
log.error("Exception   : {}", exception.getClass().getName());
log.error("Message     : {}", exception.getMessage());
log.error("Stack Trace : {}", Arrays.toString(exception.getStackTrace()));
log.error("========================================");
```

## Integration with Monitoring Tools

- Send errors to **Sentry**, **Datadog**, **New Relic**
- Log to **ELK Stack** (Elasticsearch, Logstash, Kibana)
- Use **Prometheus + Grafana** for metrics
- Send alerts to **Slack**, **PagerDuty**, **Email**

---

# 10. Production Best Practices

## Producer Best Practices

- Use `acks=all` for critical messages
- Enable idempotence to prevent duplicates
- Configure appropriate timeouts
- Implement retry logic with backoff
- Log all send failures
- Monitor producer metrics

## Consumer Best Practices

- Use appropriate error handlers
- Configure retry with backoff
- Implement Dead Letter Topics
- Classify exceptions (retryable vs non-retryable)
- Log errors with full context
- Monitor consumer lag and errors
- Alert on DLT message accumulation

## DLT Best Practices

- Monitor DLT topics continuously
- Set up alerts for DLT message count
- Review DLT messages regularly
- Implement reprocessing mechanisms
- Archive old DLT messages
- Analyze DLT patterns for root causes

## General Best Practices

- Use correlation IDs for tracing
- Implement circuit breakers for external services
- Use schema validation (Avro/Protobuf)
- Monitor Kafka broker health
- Set up comprehensive logging
- Implement health check endpoints

---

# 11. Complete Error Handling Architecture

```text
                    PRODUCER
                       │
                       ▼
              KafkaTemplate.send()
                       │
                       ▼
              whenComplete Callback
                       │
            ┌──────────┴──────────┐
            ▼                     ▼
        Success                Failure
            │                     │
            ▼                     ▼
        Log Info            Log Error
            │                     │
            ▼                     ▼
        Update DB          Send Alert
                               │
                               ▼
                         Save for Retry


                    CONSUMER
                       │
                       ▼
                  @KafkaListener
                       │
                       ▼
                Process Message
                       │
            ┌──────────┴──────────┐
            ▼                     ▼
        Success                Failure
            │                     │
            ▼                     ▼
        Commit Offset        Error Handler
            │                     │
            ▼                     ▼
        Log Success          Retry (BackOff)
            │                     │
            ▼                     ▼
        Continue             Max Retries?
            │                     │
            │            ┌────────┴────────┐
            │            ▼                 ▼
            │        Yes (DLT)        No (Retry)
            │            │                 │
            │            ▼                 ▼
            │        Send to DLT       Wait & Retry
            │            │
            ▼            ▼
        DLT Consumer  Retry Attempt
            │
            ▼
        Manual Review
```

---

# 12. Common Error Scenarios and Solutions

## Scenario 1: Broker Unavailable

**Problem:**

```text
org.apache.kafka.common.errors.NetworkException: 
The server disconnected before a response was received.
```

**Solution:**

```properties
spring.kafka.producer.retries=5
spring.kafka.producer.properties.request.timeout.ms=60000
spring.kafka.producer.properties.delivery.timeout.ms=180000
```

---

## Scenario 2: Deserialization Error

**Problem:**

```text
org.springframework.kafka.support.serialization.DeserializationException: 
Failed to deserialize
```

**Solution:**

- Validate message format before sending
- Use schema validation
- Add error handler for deserialization exceptions
- Send malformed messages to DLT

---

## Scenario 3: Consumer Stuck in Retry Loop

**Problem:**

Message keeps failing and retrying infinitely.

**Solution:**

```java
errorHandler.addNotRetryableExceptions(
    IllegalArgumentException.class
);

// Or use FixedBackOff with max attempts
new FixedBackOff(1000L, 3L)
```

---

## Scenario 4: DLT Growing Unbounded

**Problem:**

DLT topic accumulates too many messages.

**Solution:**

- Monitor DLT message count
- Set up alerts
- Implement automatic reprocessing for certain error types
- Archive old DLT messages
- Analyze root causes and fix upstream issues

---

# 13. Testing Error Handling

## Test Producer Error

```java
@Test
public void testProducerError() {

    // Simulate broker down scenario
    // Verify error callback is invoked
    // Verify error is logged
    // Verify alert is sent

}
```

## Test Consumer Error

```java
@Test
public void testConsumerError() {

    // Send message that throws exception
    // Verify retry happens
    // Verify DLT receives message after max retries
    // Verify error is logged properly

}
```

## Test DLT Flow

```java
@Test
public void testDeadLetterTopic() {

    // Send message that always fails
    // Wait for retries to complete
    // Verify message appears in DLT
    // Verify error headers are present

}
```

---

# 14. Interview Questions

## Q1. What happens when a consumer throws an exception?

By default, the message is not acknowledged and the consumer retries. Without proper error handling, this can cause infinite retry loops.

## Q2. What is a Dead Letter Topic?

A DLT is a special topic where failed messages are sent after exhausting all retry attempts. It allows manual review and reprocessing.

## Q3. What is the difference between FixedBackOff and ExponentialBackOff?

- `FixedBackOff`: Constant delay between retries
- `ExponentialBackOff`: Delay increases exponentially with each retry

## Q4. Why classify exceptions as retryable vs non-retryable?

Some errors (like validation errors) will never succeed on retry. Retrying them wastes resources. Only transient errors should be retried.

## Q5. What does `@RetryableTopic` do?

It simplifies retry configuration by automatically handling retries, backoff, and DLT routing based on annotations.

## Q6. What information should be logged for Kafka errors?

- Topic, partition, offset
- Message key and value
- Exception type and message
- Timestamp
- Consumer group ID
- Stack trace for debugging

## Q7. How do you prevent message loss in Kafka?

- Use `acks=all` on producer
- Implement proper error handling
- Use DLT for failed messages
- Monitor and alert on errors
- Implement retry mechanisms

## Q8. What is idempotence in Kafka producer?

Idempotence ensures that even if the producer retries a send, duplicate messages are not created in Kafka.

```properties
spring.kafka.producer.properties.enable.idempotence=true
```

---

# 15. Chapter Checklist

- [x] Producer error handling
- [x] Consumer error handling
- [x] `ErrorHandler` and `DefaultErrorHandler`
- [x] Retry mechanisms
- [x] `FixedBackOff` and `ExponentialBackOff`
- [x] Dead Letter Topic (DLT)
- [x] `@RetryableTopic` annotation
- [x] Exception classification
- [x] Error logging best practices
- [x] Monitoring and alerting
- [x] Production best practices
- [x] Common error scenarios
- [x] Testing error handling
- [x] Interview questions

---

# Next Chapter

## Chapter 14 — Kafka Avro and Schema Registry

Topics:

- Why Avro over JSON?
- Schema Registry setup
- Avro schema definition
- Producer with Avro
- Consumer with Avro
- Schema evolution (backward, forward, full compatibility)
- Versioning strategies
- Production considerations