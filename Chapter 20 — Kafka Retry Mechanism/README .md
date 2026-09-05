# Chapter — Kafka Retry Mechanism

> In this chapter, we will learn how to implement retry mechanisms for Kafka producers and consumers, including retry backoff strategies, retry policies, failure handling, and production best practices.

---

## Learning Objectives

After completing this chapter, you will understand:

- Producer retry mechanism
- Consumer retry mechanism
- Retry backoff strategies
- Retry policies and configuration
- Failure handling patterns
- Dead Letter Queue (DLQ)
- Circuit breaker pattern
- Producer retry configuration
- Consumer retry configuration
- Backoff strategy implementation
- Production best practices

---

# 1. Why Retry Mechanism Matters

In distributed systems, failures are inevitable:

- Network timeouts
- Broker unavailability
- Temporary service outages
- Resource contention
- Transient errors

Without proper retry mechanisms:

- Messages can be lost
- Systems become fragile
- Temporary failures become permanent
- User experience degrades

With proper retry mechanisms:

- Transient failures are handled gracefully
- System resilience improves
- Message delivery is reliable
- Better user experience

---

## Types of Failures

```text
Transient Failures (Retry Recommended):
├── Network timeout
├── Broker temporarily unavailable
├── Connection reset
├── Resource temporarily unavailable
└── Rate limiting


Permanent Failures (Do Not Retry):
├── Invalid message format
├── Schema validation error
├── Authentication failure
├── Authorization denied
└── Business logic validation error
```

---

# 2. Producer Retry Mechanism

## How Producer Retry Works

```text
Step 1: Send Message
Producer ──► Kafka Broker


Step 2: Failure Detected
Producer ──► Timeout/Error


Step 3: Retry Attempt 1
Producer ──► Wait (backoff) ──► Kafka Broker


Step 4: Success or Continue Retry
Success ──► Done
Failure ──► Retry Attempt 2 (up to max retries)
```

---

## Producer Retry Configuration

```properties
# Number of retries
spring.kafka.producer.retries=3

# Retry backoff time
spring.kafka.producer.properties.retry.backoff.ms=100

# Delivery timeout (total time for retries)
spring.kafka.producer.properties.delivery.timeout.ms=120000

# Request timeout (per attempt)
spring.kafka.producer.properties.request.timeout.ms=30000
```

---

## Retryable Exceptions

| Exception | Retry? | Reason |
|---|---|---|
| `TimeoutException` | ✅ Yes | Transient network issue |
| `NetworkException` | ✅ Yes | Connection problem |
| `NotLeaderForPartitionException` | ✅ Yes | Leader election in progress |
| `LeaderNotAvailableException` | ✅ Yes | Leader election in progress |
| `UnknownTopicOrPartitionException` | ⚠️ Maybe | Topic may not exist |
| `InvalidTopicException` | ❌ No | Permanent configuration error |
| `RecordTooLargeException` | ❌ No | Message size issue |
| `SerializationException` | ❌ No | Data format error |

---

## Producer Retry Flow

```text
Send Message
    │
    ▼
┌─────────────┐
│   Try Send  │
└─────────────┘
    │
    ├── Success ──► Return Result
    │
    ▼
┌─────────────┐
│   Failure   │
└─────────────┘
    │
    ▼
┌─────────────┐
│ Retry Count │
│    < Max?   │
└─────────────┘
    │
    ├── Yes ──► Wait (Backoff) ──► Try Send Again
    │
    ▼
    No
    │
    ▼
┌─────────────┐
│ Send to DLQ │
│   or Fail   │
└─────────────┘
```

---

# 3. Consumer Retry Mechanism

## How Consumer Retry Works

```text
Step 1: Consume Message
Consumer ◄── Kafka Broker


Step 2: Process Message
Consumer ──► Business Logic


Step 3: Failure Detected
Business Logic ──► Exception


Step 4: Retry Attempt 1
Wait (Backoff) ──► Process Again


Step 5: Success or Continue Retry
Success ──► Commit Offset
Failure ──► Retry Attempt 2 (up to max retries)
```

---

## Consumer Retry Strategies

### Strategy 1: Spring Retry with @Retryable

```java
@Retryable(
    maxAttempts = 3,
    backoff = @BackOff(delay = 1000, multiplier = 2.0)
)
@KafkaListener(topics = "my-topic")
public void consume(String message) {
    // Process message
}
```

---

### Strategy 2: DefaultErrorHandler with BackOff

```java
@Bean
public CommonErrorHandler errorHandler() {
    return new DefaultErrorHandler(
        (record, exception) -> {
            // Send to DLQ after max retries
            sendToDLQ(record, exception);
        },
        new FixedBackOff(1000L, 3L)
    );
}
```

---

### Strategy 3: @RetryableTopic Annotation

```java
@RetryableTopic(
    attempts = "5",
    backOff = @BackOff(delay = 1000, multiplier = 2.0),
    dltStrategy = DltStrategy.ALWAYS_SEND
)
@KafkaListener(topics = "my-topic")
public void consume(String message) {
    // Process message
}
```

---

## Consumer Retry Flow

```text
Receive Message
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
│ Retry Count │
│    < Max?   │
└─────────────┘
    │
    ├── Yes ──► Wait (Backoff) ──► Process Again
    │
    ▼
    No
    │
    ▼
┌─────────────┐
│ Send to DLQ │
└─────────────┘
```

---

# 4. Retry Backoff Strategies

## Fixed Backoff

Constant delay between retries.

```java
new FixedBackOff(1000L, 3L)
// 1 second delay, 3 retry attempts
```

**Timeline:**

```text
Attempt 1: 0ms (initial)
Attempt 2: 1000ms (1 second)
Attempt 3: 2000ms (2 seconds)
Attempt 4: 3000ms (3 seconds)
```

**Use Cases:**

- Simple retry scenarios
- When failure duration is predictable
- Low-traffic systems

---

## Exponential Backoff

Delay increases exponentially with each retry.

```java
ExponentialBackOff backOff = new ExponentialBackOff();
backOff.setInitialInterval(1000); // 1 second
backOff.setMultiplier(2.0); // Double each time
backOff.setMaxInterval(10000); // Max 10 seconds
backOff.setMaxAttempts(5);
```

**Timeline:**

```text
Attempt 1: 0ms (initial)
Attempt 2: 1000ms (1 second)
Attempt 3: 2000ms (2 seconds)
Attempt 4: 4000ms (4 seconds)
Attempt 5: 8000ms (8 seconds)
Attempt 6: 10000ms (10 seconds, max reached)
```

**Use Cases:**

- Transient failures with unpredictable duration
- High-traffic systems
- When you want to avoid overwhelming the system

---

## Linear Backoff

Delay increases linearly with each retry.

```java
ExponentialBackOff backOff = new ExponentialBackOff();
backOff.setInitialInterval(1000); // 1 second
backOff.setMultiplier(1.0); // No multiplication (linear)
backOff.setMaxInterval(5000); // Max 5 seconds
backOff.setMaxAttempts(5);
```

**Timeline:**

```text
Attempt 1: 0ms (initial)
Attempt 2: 1000ms (1 second)
Attempt 3: 2000ms (2 seconds)
Attempt 4: 3000ms (3 seconds)
Attempt 5: 4000ms (4 seconds)
Attempt 6: 5000ms (5 seconds, max reached)
```

**Use Cases:**

- Moderate retry scenarios
- When you want gradual increase
- Balance between fixed and exponential

---

## Backoff Strategy Comparison

| Strategy | Delay Pattern | Pros | Cons | Best For |
|---|---|---|---|---|
| **Fixed** | Constant | Simple, predictable | May overwhelm system | Low traffic |
| **Exponential** | Doubling | Reduces load, adaptive | Complex, longer total time | High traffic |
| **Linear** | Gradual increase | Balanced | Less adaptive | Medium traffic |

---

# 5. Retry Policies

## Retry Policy Components

```text
Retry Policy
├── Max Attempts
├── Backoff Strategy
├── Retryable Exceptions
├── Non-Retryable Exceptions
└── Recovery Action
```

---

## Default Retry Policy

```java
DefaultErrorHandler errorHandler = new DefaultErrorHandler(
    (record, exception) -> {
        // Recovery action after max retries
        log.error("Message processing failed after all retries");
        sendToDLQ(record, exception);
    },
    new FixedBackOff(1000L, 3L)
);

// Add non-retryable exceptions
errorHandler.addNotRetryableExceptions(
    IllegalArgumentException.class,
    NullPointerException.class
);
```

---

## Custom Retry Policy

```java
@Bean
public CommonErrorHandler customErrorHandler() {

    DefaultErrorHandler errorHandler = new DefaultErrorHandler(
        (record, exception) -> {
            log.error("========================================");
            log.error("RECOVERY ACTION");
            log.error("Topic      : {}", record.topic());
            log.error("Partition  : {}", record.partition());
            log.error("Offset     : {}", record.offset());
            log.error("Key        : {}", record.key());
            log.error("Value      : {}", record.value());
            log.error("Exception  : {}", exception.getMessage());
            log.error("========================================");

            // Send to DLQ
            sendToDeadLetterQueue(record, exception);
        },
        new ExponentialBackOff(1000L, 5L)
    );

    // Configure retryable exceptions
    errorHandler.addRetryableExceptions(
        TimeoutException.class,
        NetworkException.class,
        ConnectException.class
    );

    // Configure non-retryable exceptions
    errorHandler.addNotRetryableExceptions(
        IllegalArgumentException.class,
        NullPointerException.class,
        ClassCastException.class
    );

    return errorHandler;
}
```

---

## Retry Policy by Message Type

```java
@Bean
public CommonErrorHandler priorityErrorHandler() {

    DefaultErrorHandler errorHandler = new DefaultErrorHandler(
        (record, exception) -> sendToDLQ(record, exception),
        new FixedBackOff(1000L, 3L)
    );

    // High priority messages - more retries
    // Configure in listener container factory

    return errorHandler;
}
```

---

# 6. Failure Handling Patterns

## Pattern 1: Retry with DLQ

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
    │ Send to DLQ │
    └─────────────┘
```

---

## Pattern 2: Circuit Breaker

```text
        CONSUMER
           │
           ▼
    ┌─────────────┐
    │   Circuit   │
    │   Breaker   │
    └─────────────┘
           │
           ├── CLOSED (Normal) ──► Process Messages
           │
           ├── OPEN (Failing) ──► Skip Processing
           │
           └── HALF-OPEN (Testing) ──► Try One Message
```

---

## Pattern 3: Fallback Handler

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
    │   Fallback  │
    │   Handler   │
    └─────────────┘
           │
           ├── Success ──► Commit Offset
           │
           ▼
    ┌─────────────┐
    │ Send to DLQ │
    └─────────────┘
```

---

# 7. Producer Retry Configuration

## Basic Producer Retry Config

**File:** `ProducerRetryConfig.java`

```java
package edu.anant.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class ProducerRetryConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public DefaultKafkaProducerFactory<String, String> 
    retryProducerFactory() {

        Map<String, Object> props = new HashMap<>();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Retry Configuration
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);

        // Timeout Configuration
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);

        // Reliability
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> retryKafkaTemplate() {

        return new KafkaTemplate<>(retryProducerFactory());
    }

}
```

---

## High Reliability Producer Retry Config

**File:** `HighReliabilityProducerConfig.java`

```java
package edu.anant.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class HighReliabilityProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public DefaultKafkaProducerFactory<String, String> 
    highReliabilityProducerFactory() {

        Map<String, Object> props = new HashMap<>();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Retry Configuration - Maximum retries
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);

        // Timeout Configuration
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 300000);

        // Reliability
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> highReliabilityKafkaTemplate() {

        return new KafkaTemplate<>(highReliabilityProducerFactory());
    }

}
```

---

## Producer Service with Manual Retry

**File:** `RetryProducerService.java`

```java
package edu.anant.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class RetryProducerService {

    private static final String TOPIC = "retry-topic";

    private final KafkaTemplate<String, String> kafkaTemplate;

    // Automatic retry with Spring Retry
    @Retryable(
        maxAttempts = 3,
        backoff = @Backoff(delay = 1000, multiplier = 2.0)
    )
    public void sendWithRetry(String key, String message) {

        log.info("Sending message with retry: {}", message);

        kafkaTemplate.send(TOPIC, key, message)
            .whenComplete((result, exception) -> {

                if (exception != null) {
                    log.error("Failed to send message: {}", exception.getMessage());
                    throw new RuntimeException("Send failed", exception);
                }

                log.info("Message sent successfully - Offset: {}", 
                    result.getRecordMetadata().offset());
            });
    }

    // Manual retry with custom logic
    public void sendWithManualRetry(String key, String message, int maxRetries) {

        int retryCount = 0;
        long backoffTime = 1000; // 1 second

        while (retryCount < maxRetries) {

            try {

                log.info("Attempt {}/{}: Sending message: {}", 
                    retryCount + 1, maxRetries, message);

                SendResult<String, String> result = 
                    kafkaTemplate.send(TOPIC, key, message).get();

                log.info("Message sent successfully - Offset: {}", 
                    result.getRecordMetadata().offset());
                return;

            } catch (Exception e) {

                retryCount++;

                if (retryCount >= maxRetries) {
                    log.error("========================================");
                    log.error("MESSAGE SEND FAILED AFTER {} RETRIES", maxRetries);
                    log.error("Key     : {}", key);
                    log.error("Message : {}", message);
                    log.error("Error   : {}", e.getMessage());
                    log.error("========================================");

                    // Send to DLQ or handle failure
                    handleSendFailure(key, message, e);
                    return;
                }

                log.warn("Attempt {} failed, retrying in {} ms...", 
                    retryCount, backoffTime);

                try {
                    Thread.sleep(backoffTime);
                    backoffTime *= 2; // Exponential backoff
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Retry interrupted", ie);
                }
            }
        }
    }

    private void handleSendFailure(String key, String message, Exception e) {

        // Save to database for retry
        // Send to DLQ
        // Trigger alert
        log.error("Handling send failure for key: {}", key);
    }

}
```

---

# 8. Consumer Retry Configuration

## Basic Consumer Retry Config

**File:** `ConsumerRetryConfig.java`

```java
package edu.anant.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class ConsumerRetryConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public DefaultKafkaConsumerFactory<String, String> 
    retryConsumerFactory() {

        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "retry-consumer-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public CommonErrorHandler retryErrorHandler() {

        return new DefaultErrorHandler(
            (record, exception) -> {
                log.error("========================================");
                log.error("CONSUMER RETRY EXHAUSTED");
                log.error("Topic      : {}", record.topic());
                log.error("Partition  : {}", record.partition());
                log.error("Offset     : {}", record.offset());
                log.error("Key        : {}", record.key());
                log.error("Value      : {}", record.value());
                log.error("Exception  : {}", exception.getMessage());
                log.error("========================================");

                // Send to DLQ
                sendToDLQ(record, exception);
            },
            new FixedBackOff(1000L, 3L) // 1 second delay, 3 retries
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> 
    retryKafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(retryConsumerFactory());
        factory.setCommonErrorHandler(retryErrorHandler());
        factory.setConcurrency(3);

        return factory;
    }

    private void sendToDLQ(org.apache.kafka.clients.consumer.ConsumerRecord<?, ?> record, 
                           Throwable exception) {

        // Implementation to send to DLQ
        log.info("Sending to DLQ: {}", record.key());
    }

}
```

---

## Advanced Consumer Retry with Exponential Backoff

**File:** `AdvancedConsumerRetryConfig.java`

```java
package edu.anant.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
public class AdvancedConsumerRetryConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public DefaultKafkaConsumerFactory<String, String> 
    advancedConsumerFactory() {

        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "advanced-retry-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public CommonErrorHandler advancedErrorHandler() {

        // Exponential backoff: 1s, 2s, 4s, 8s, 10s (max)
        ExponentialBackOff backOff = new ExponentialBackOff();
        backOff.setInitialInterval(1000);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(10000);
        backOff.setMaxAttempts(5);

        return new DefaultErrorHandler(
            (record, exception) -> {
                log.error("========================================");
                log.error("ADVANCED RETRY EXHAUSTED");
                log.error("Topic      : {}", record.topic());
                log.error("Partition  : {}", record.partition());
                log.error("Offset     : {}", record.offset());
                log.error("Key        : {}", record.key());
                log.error("Value      : {}", record.value());
                log.error("Exception  : {}", exception.getMessage());
                log.error("========================================");

                // Send to DLQ
                sendToDLQ(record, exception);
            },
            backOff
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> 
    advancedKafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(advancedConsumerFactory());
        factory.setCommonErrorHandler(advancedErrorHandler());
        factory.setConcurrency(3);

        return factory;
    }

    private void sendToDLQ(org.apache.kafka.clients.consumer.ConsumerRecord<?, ?> record, 
                           Throwable exception) {

        // Implementation to send to DLQ
        log.info("Sending to DLQ: {}", record.key());
    }

}
```

---

## Consumer Service with @RetryableTopic

**File:** `RetryableConsumerService.java`

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
public class RetryableConsumerService {

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
        topics = "retryable-topic",
        groupId = "retryable-consumer-group",
        containerFactory = "advancedKafkaListenerContainerFactory"
    )
    public void consumeWithRetry(ConsumerRecord<String, String> record) {

        log.info("========================================");
        log.info("PROCESSING MESSAGE WITH RETRY");
        log.info("Topic      : {}", record.topic());
        log.info("Partition  : {}", record.partition());
        log.info("Offset     : {}", record.offset());
        log.info("Key        : {}", record.key());
        log.info("Value      : {}", record.value());
        log.info("========================================");

        String value = record.value();

        // Simulate processing that may fail
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("Empty value - will not retry");
        }

        if (value.contains("fail")) {
            throw new RuntimeException("Simulated failure - will retry");
        }

        log.info("Message processed successfully: {}", value);
    }

}
```

---

# 9. Backoff Strategy Implementation

## Custom Backoff Strategy

**File:** `CustomBackoffStrategy.java`

```java
package edu.anant.strategy;

import lombok.extern.slf4j.Slf4j;
import org.springframework.util.backoff.BackOffExecution;

import java.time.Duration;

@Slf4j
public class CustomBackoffStrategy implements BackOffExecution {

    private final long initialInterval;
    private final long maxInterval;
    private final double multiplier;
    private final long jitter;
    private int attempt;

    public CustomBackoffStrategy(long initialInterval, 
                                  long maxInterval, 
                                  double multiplier,
                                  long jitter) {
        this.initialInterval = initialInterval;
        this.maxInterval = maxInterval;
        this.multiplier = multiplier;
        this.jitter = jitter;
        this.attempt = 0;
    }

    @Override
    public long nextBackOff() {

        if (attempt == 0) {
            attempt++;
            return initialInterval;
        }

        // Calculate backoff with exponential increase
        long backoff = (long) (initialInterval * Math.pow(multiplier, attempt - 1));

        // Cap at max interval
        backoff = Math.min(backoff, maxInterval);

        // Add jitter to prevent thundering herd
        long jitterValue = (long) (Math.random() * jitter);
        backoff += jitterValue;

        attempt++;

        log.debug("Attempt {}: Backoff {} ms", attempt, backoff);

        return backoff;
    }

    public void reset() {
        this.attempt = 0;
    }

}
```

---

## Adaptive Backoff Strategy

**File:** `AdaptiveBackoffStrategy.java`

```java
package edu.anant.strategy;

import lombok.extern.slf4j.Slf4j;
import org.springframework.util.backoff.BackOffExecution;

@Slf4j
public class AdaptiveBackoffStrategy implements BackOffExecution {

    private final long minBackoff;
    private final long maxBackoff;
    private long currentBackoff;
    private int consecutiveFailures;
    private int consecutiveSuccesses;

    public AdaptiveBackoffStrategy(long minBackoff, long maxBackoff) {
        this.minBackoff = minBackoff;
        this.maxBackoff = maxBackoff;
        this.currentBackoff = minBackoff;
        this.consecutiveFailures = 0;
        this.consecutiveSuccesses = 0;
    }

    @Override
    public long nextBackOff() {

        // Increase backoff on failures
        consecutiveFailures++;
        consecutiveSuccesses = 0;

        // Double the backoff on each failure
        currentBackoff = currentBackoff * 2;

        // Cap at max
        currentBackoff = Math.min(currentBackoff, maxBackoff);

        log.info("Consecutive failures: {}, Backoff: {} ms", 
            consecutiveFailures, currentBackoff);

        return currentBackoff;
    }

    public void onSuccess() {

        consecutiveSuccesses++;
        consecutiveFailures = 0;

        // Reduce backoff on successes
        if (consecutiveSuccesses >= 3) {
            currentBackoff = currentBackoff / 2;
            currentBackoff = Math.max(currentBackoff, minBackoff);
            consecutiveSuccesses = 0;

            log.info("Backoff reduced to: {} ms", currentBackoff);
        }
    }

    public void reset() {
        this.currentBackoff = minBackoff;
        this.consecutiveFailures = 0;
        this.consecutiveSuccesses = 0;
    }

}
```

---

# 10. Dead Letter Queue (DLQ)

## DLQ Configuration

**File:** `DLQConfig.java`

```java
package edu.anant.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class DLQConfig {

    @Bean
    public NewTopic retryTopicDLQ() {

        return TopicBuilder
            .name("retry-topic.DLT")
            .partitions(3)
            .replicas(3)
            .build();
    }

    @Bean
    public NewTopic retryableTopicDLQ() {

        return TopicBuilder
            .name("retryable-topic.DLT")
            .partitions(3)
            .replicas(3)
            .build();
    }

}
```

---

## DLQ Producer Service

**File:** `DLQProducerService.java`

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
public class DLQProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendToDLQ(String originalTopic, 
                          String key, 
                          String value, 
                          Throwable exception) {

        String dltTopic = originalTopic + ".DLT";

        ProducerRecord<String, String> dltRecord = 
            new ProducerRecord<>(dltTopic, key, value);

        // Add error metadata as headers
        dltRecord.headers().add("exception-class", 
            exception.getClass().getName().getBytes());
        dltRecord.headers().add("exception-message", 
            exception.getMessage().getBytes());
        dltRecord.headers().add("original-topic", 
            originalTopic.getBytes());
        dltRecord.headers().add("timestamp", 
            String.valueOf(System.currentTimeMillis()).getBytes());

        kafkaTemplate.send(dltRecord)
            .whenComplete((result, ex) -> {

                if (ex != null) {
                    log.error("Failed to send to DLQ: {}", ex.getMessage());
                } else {
                    log.info("Message sent to DLQ: {}", dltTopic);
                }
            });
    }

}
```

---

## DLQ Consumer Service

**File:** `DLQConsumerService.java`

```java
package edu.anant.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class DLQConsumerService {

    @KafkaListener(
        topics = ".*\\.DLT",
        groupId = "dlq-consumer-group",
        topicPattern = ".*\\.DLT"
    )
    public void consumeDLQ(ConsumerRecord<String, String> record) {

        log.error("========================================");
        log.error("DLQ MESSAGE RECEIVED");
        log.error("Topic      : {}", record.topic());
        log.error("Partition  : {}", record.partition());
        log.error("Offset     : {}", record.offset());
        log.error("Key        : {}", record.key());
        log.error("Value      : {}", record.value());

        // Extract error metadata from headers
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

# 11. Production Best Practices

## Retry Configuration Guidelines

| Scenario | Max Retries | Backoff | DLQ |
|---|---|---|---|
| **Transient Network Errors** | 5-10 | Exponential | Yes |
| **Broker Unavailable** | 10+ | Exponential | Yes |
| **Validation Errors** | 0 | N/A | Yes |
| **Schema Errors** | 0 | N/A | Yes |
| **Business Logic Errors** | 0-3 | Fixed | Optional |

---

## Monitoring Retry Metrics

Track these metrics:

- Retry count per message
- Success rate after retries
- Average retry duration
- DLQ message count
- Most common retry exceptions
- Backoff effectiveness

---

## Retry Anti-Patterns

| Anti-Pattern | Problem | Solution |
|---|---|---|
| Infinite retries | System hangs forever | Set max retries |
| No backoff | Overwhelms system | Use exponential backoff |
| Retry all exceptions | Wastes resources on permanent failures | Classify exceptions |
| No DLQ | Lost messages | Always have DLQ |
| Fixed backoff for everything | Not adaptive | Use exponential backoff |

---

# 12. Interview Questions

## Q1. What is the difference between producer retry and consumer retry?

- **Producer retry**: Retries sending messages when broker is unavailable
- **Consumer retry**: Retries processing messages when business logic fails

---

## Q2. What is exponential backoff?

A retry strategy where the delay between retries increases exponentially (e.g., 1s, 2s, 4s, 8s).

---

## Q3. Why use jitter in backoff strategies?

To prevent the "thundering herd" problem where many clients retry at the same time, overwhelming the system.

---

## Q4. What exceptions should not be retried?

- `IllegalArgumentException`
- `NullPointerException`
- `ClassCastException`
- Validation errors
- Schema errors

---

## Q5. What is a Dead Letter Queue?

A special topic where messages are sent after exhausting all retry attempts, for manual review and reprocessing.

---

## Q6. How do you configure retry in Spring Kafka?

Use `DefaultErrorHandler` with `BackOff` strategy, or `@RetryableTopic` annotation.

---

## Q7. What is the purpose of `@RetryableTopic`?

It simplifies retry configuration by automatically handling retries, backoff, and DLQ routing based on annotations.

---

## Q8. When should you use DLQ?

For messages that fail after all retry attempts, especially for permanent failures that need manual intervention.

---

## Q9. How do you monitor retry effectiveness?

Track retry count, success rate after retries, average retry duration, and DLQ message count.

---

## Q10. What is the circuit breaker pattern?

A pattern that stops processing when failure rate exceeds a threshold, preventing cascading failures.

---

# 13. Chapter Checklist

- [x] Producer retry mechanism
- [x] Consumer retry mechanism
- [x] Retry backoff strategies
- [x] Retry policies and configuration
- [x] Failure handling patterns
- [x] Dead Letter Queue (DLQ)
- [x] Circuit breaker pattern
- [x] Producer retry configuration
- [x] Consumer retry configuration
- [x] Backoff strategy implementation
- [x] Production best practices
- [x] Interview questions

---

# Next Chapter

## Chapter — Kafka Testing Strategies

Topics:

- Unit testing producers
- Unit testing consumers
- Integration testing with Embedded Kafka
- Testing with TestContainers
- Mocking Kafka for tests
- Testing error handling
- Performance testing
- End-to-end testing