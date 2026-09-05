# Chapter — Kafka Producer Acknowledgements (ACKs)

> In this chapter, we will learn how Kafka producer acknowledgements work, the difference between `acks=0`, `acks=1`, and `acks=all`, and how to configure different ACK modes in Spring Boot applications.

---

## Learning Objectives

After completing this chapter, you will understand:

- What are producer acknowledgements
- How `acks=0` works
- How `acks=1` works
- How `acks=all` works
- Reliability vs performance trade-offs
- Durability guarantees
- How to configure different ACK modes
- How to compare behavior of different ACK settings
- Production best practices for ACKs
- Common pitfalls and solutions

---

# 1. What are Producer Acknowledgements?

When a producer sends a message to Kafka, it needs to know whether the message was successfully received.

**Acknowledgements (ACKs)** are responses from Kafka brokers confirming that a message has been received.

```text
        PRODUCER              KAFKA BROKER
           │                      │
           │  1. Send Message     │
           │─────────────────────►│
           │                      │
           │  2. Acknowledgement  │
           │◄─────────────────────│
           │                      │
```

The `acks` configuration determines how many replicas must acknowledge the message before the producer considers it successful.

---

# 2. ACK Modes Overview

| ACK Mode | Description | Reliability | Performance |
|---|---|---|---|
| `acks=0` | No acknowledgement | Lowest | Highest |
| `acks=1` | Leader acknowledges | Medium | High |
| `acks=all` | All replicas acknowledge | Highest | Lowest |

---

## Visual Comparison

```text
acks=0:
Producer ──► Broker (no wait for response)
           Fast but risky


acks=1:
Producer ──► Leader ──► Producer (ack)
           Fast with some safety


acks=all:
Producer ──► Leader ──► Replica 1
                      ──► Replica 2
                      ──► Replica 3
           ──► Producer (ack after all)
           Slow but safest
```

---

# 3. acks=0 (No Acknowledgement)

## How It Works

```text
        PRODUCER              KAFKA BROKER
           │                      │
           │  Send Message        │
           │─────────────────────►│
           │                      │
           │  (No response)       │
           │                      │
```

Producer sends message and does not wait for any acknowledgement.

---

## Configuration

```java
props.put(ProducerConfig.ACKS_CONFIG, "0");
```

Or in Spring Boot:

```properties
spring.kafka.producer.acks=0
```

---

## Characteristics

| Aspect | Behavior |
|---|---|
| **Acknowledgement** | None |
| **Retry** | Not possible (no error detection) |
| **Message Loss** | Possible |
| **Throughput** | Highest |
| **Latency** | Lowest |
| **Use Case** | Logging, metrics, non-critical data |

---

## When to Use acks=0

✅ Good for:

- Application logs
- Metrics and monitoring data
- Debug information
- Non-critical events
- High-volume, low-importance data

❌ Bad for:

- Financial transactions
- Order processing
- User data
- Critical business events
- Compliance-related data

---

## Example: acks=0 Producer

**File:** `AcksZeroProducerConfig.java`

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
public class AcksZeroProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public DefaultKafkaProducerFactory<String, String> 
    acksZeroProducerFactory() {

        Map<String, Object> props = new HashMap<>();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // No acknowledgements
        props.put(ProducerConfig.ACKS_CONFIG, "0");

        // No retries (no point without acks)
        props.put(ProducerConfig.RETRIES_CONFIG, 0);

        // Disable idempotence (not compatible with acks=0)
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> acksZeroKafkaTemplate() {

        return new KafkaTemplate<>(acksZeroProducerFactory());
    }

}
```

---

## acks=0 Producer Service

**File:** `AcksZeroProducerService.java`

```java
package edu.anant.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class AcksZeroProducerService {

    private static final String TOPIC = "logs-topic";

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void send(String key, String message) {

        log.info("Sending log message (no ack): {}", message);

        // No callback - we won't know if it succeeds or fails
        kafkaTemplate.send(TOPIC, key, message);

        log.info("Log message sent (fire and forget)");
    }

}
```

---

## acks=0 Behavior

```text
Scenario 1: Success
Producer ──► Broker ──► Success (Producer doesn't know)


Scenario 2: Broker Down
Producer ──► X (fails silently)
Producer thinks message was sent but it wasn't


Scenario 3: Network Issue
Producer ──► X (fails silently)
No retry, no error notification
```

---

# 4. acks=1 (Leader Acknowledgement)

## How It Works

```text
        PRODUCER              KAFKA BROKER (Leader)
           │                      │
           │  Send Message        │
           │─────────────────────►│
           │                      │
           │  Write to Leader     │
           │                      │
           │  Acknowledgement     │
           │◄─────────────────────│
           │                      │
```

Producer waits for the leader replica to acknowledge the message.

---

## Configuration

```java
props.put(ProducerConfig.ACKS_CONFIG, "1");
```

Or in Spring Boot:

```properties
spring.kafka.producer.acks=1
```

---

## Characteristics

| Aspect | Behavior |
|---|---|
| **Acknowledgement** | From leader only |
| **Retry** | Possible on error |
| **Message Loss** | Possible if leader fails before replication |
| **Throughput** | High |
| **Latency** | Low |
| **Use Case** | Most general-purpose scenarios |

---

## When to Use acks=1

✅ Good for:

- General application events
- User activity tracking
- Non-critical business events
- Most microservices communication
- Default choice for many applications

❌ Bad for:

- Financial transactions requiring zero loss
- Critical compliance data
- Scenarios where even rare message loss is unacceptable

---

## Example: acks=1 Producer

**File:** `AcksOneProducerConfig.java`

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
public class AcksOneProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public DefaultKafkaProducerFactory<String, String> 
    acksOneProducerFactory() {

        Map<String, Object> props = new HashMap<>();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Leader acknowledgement
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        // Enable retries
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);

        // Optional: Enable idempotence
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> acksOneKafkaTemplate() {

        return new KafkaTemplate<>(acksOneProducerFactory());
    }

}
```

---

## acks=1 Producer Service

**File:** `AcksOneProducerService.java`

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
public class AcksOneProducerService {

    private static final String TOPIC = "events-topic";

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void send(String key, String message) {

        log.info("Sending event message: {}", message);

        kafkaTemplate.send(TOPIC, key, message)
            .whenComplete((result, exception) -> {

                if (exception != null) {
                    log.error("========================================");
                    log.error("MESSAGE SEND FAILED (acks=1)");
                    log.error("Topic: {}", TOPIC);
                    log.error("Key  : {}", key);
                    log.error("Error: {}", exception.getMessage());
                    log.error("========================================");
                    return;
                }

                log.info("========================================");
                log.info("MESSAGE SENT SUCCESSFULLY (acks=1)");
                log.info("Topic     : {}", result.getRecordMetadata().topic());
                log.info("Partition : {}", result.getRecordMetadata().partition());
                log.info("Offset    : {}", result.getRecordMetadata().offset());
                log.info("========================================");
            });
    }

}
```

---

## acks=1 Behavior

```text
Scenario 1: Success
Producer ──► Leader ──► Write to Leader
                      ──► Ack to Producer
                      ──► Replicate to followers (async)


Scenario 2: Leader Fails Before Replication
Producer ──► Leader ──► Write to Leader
                      ──► Ack to Producer ✓
                      ──► Leader crashes ✗
                      ──► Message lost (not replicated yet)


Scenario 3: Network Issue
Producer ──► X (timeout)
           ──► Retry ──► Success
```

---

# 5. acks=all (All Replicas Acknowledge)

## How It Works

```text
        PRODUCER              KAFKA BROKER
           │                      │
           │  Send Message        │
           │─────────────────────►│
           │                      │
           │  Write to Leader     │
           │  Write to Replica 1  │
           │  Write to Replica 2  │
           │  Write to Replica 3  │
           │                      │
           │  Acknowledgement     │
           │  (after all ACK)     │
           │◄─────────────────────│
           │                      │
```

Producer waits for all in-sync replicas (ISR) to acknowledge the message.

---

## Configuration

```java
props.put(ProducerConfig.ACKS_CONFIG, "all");
// or
props.put(ProducerConfig.ACKS_CONFIG, "-1"); // -1 is same as "all"
```

Or in Spring Boot:

```properties
spring.kafka.producer.acks=all
```

---

## Characteristics

| Aspect | Behavior |
|---|---|
| **Acknowledgement** | From all in-sync replicas |
| **Retry** | Possible on error |
| **Message Loss** | Very unlikely (only if all replicas fail) |
| **Throughput** | Lower |
| **Latency** | Higher |
| **Use Case** | Critical data, financial transactions |

---

## When to Use acks=all

✅ Good for:

- Financial transactions
- Order processing
- Payment events
- Critical business data
- Compliance and audit logs
- Any data where loss is unacceptable

❌ Bad for:

- High-volume logging
- Metrics where occasional loss is acceptable
- Scenarios where performance is more important than reliability

---

## Example: acks=all Producer

**File:** `AcksAllProducerConfig.java`

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
public class AcksAllProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public DefaultKafkaProducerFactory<String, String> 
    acksAllProducerFactory() {

        Map<String, Object> props = new HashMap<>();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // All replicas acknowledgement
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // Enable retries
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);

        // Enable idempotence (recommended with acks=all)
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // Transactional ID (optional, for transactions)
        // props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "txn-id-1");

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> acksAllKafkaTemplate() {

        return new KafkaTemplate<>(acksAllProducerFactory());
    }

}
```

---

## acks=all Producer Service

**File:** `AcksAllProducerService.java`

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
public class AcksAllProducerService {

    private static final String TOPIC = "transactions-topic";

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void send(String key, String message) {

        log.info("Sending critical message: {}", message);

        kafkaTemplate.send(TOPIC, key, message)
            .whenComplete((result, exception) -> {

                if (exception != null) {
                    log.error("========================================");
                    log.error("CRITICAL MESSAGE SEND FAILED (acks=all)");
                    log.error("Topic: {}", TOPIC);
                    log.error("Key  : {}", key);
                    log.error("Error: {}", exception.getMessage());
                    log.error("Action: Will retry or save for manual processing");
                    log.error("========================================");

                    // Save to database for retry
                    // Or trigger alert
                    return;
                }

                log.info("========================================");
                log.info("CRITICAL MESSAGE SENT SUCCESSFULLY (acks=all)");
                log.info("Topic     : {}", result.getRecordMetadata().topic());
                log.info("Partition : {}", result.getRecordMetadata().partition());
                log.info("Offset    : {}", result.getRecordMetadata().offset());
                log.info("Replicas  : All in-sync replicas acknowledged");
                log.info("========================================");
            });
    }

}
```

---

## acks=all Behavior

```text
Scenario 1: Success
Producer ──► Leader ──► Write to Leader
                      ──► Replicate to Replica 1 ✓
                      ──► Replicate to Replica 2 ✓
                      ──► Replicate to Replica 3 ✓
                      ──► Ack to Producer


Scenario 2: One Replica Fails
Producer ──► Leader ──► Write to Leader ✓
                      ──► Replicate to Replica 1 ✓
                      ──► Replicate to Replica 2 ✗ (not in ISR)
                      ──► Replicate to Replica 3 ✓
                      ──► Ack to Producer (only ISR replicas)


Scenario 3: All Replicas Available
Producer ──► Leader ──► All replicas acknowledge
                      ──► Message is durable
                      ──► Ack to Producer
```

---

# 6. Reliability vs Performance Trade-offs

## Comparison Table

| Feature | acks=0 | acks=1 | acks=all |
|---|---|---|---|
| **Acknowledgement** | None | Leader only | All ISR replicas |
| **Message Loss Risk** | High | Medium | Very Low |
| **Throughput** | Highest | High | Lower |
| **Latency** | Lowest | Low | Higher |
| **Retry Support** | No | Yes | Yes |
| **Idempotence** | No | Yes | Yes |
| **Use Case** | Logs, metrics | General purpose | Critical data |

---

## Performance Comparison

```text
Throughput (messages/second):

acks=0  ████████████████████  100%
acks=1  ████████████████      80%
acks=all ████████████          60%


Latency (milliseconds):

acks=0  ███                   5ms
acks=1  ██████                10ms
acks=all ████████████          20ms
```

> Note: Actual numbers depend on hardware, network, and cluster configuration.

---

## Durability Guarantee

```text
Durability (likelihood of message persistence):

acks=0  ███                   Low
        (Message can be lost if broker fails before write)

acks=1  ██████████            Medium
        (Message can be lost if leader fails before replication)

acks=all ████████████████████ High
        (Message lost only if all replicas fail simultaneously)
```

---

# 7. Configure Different ACK Modes

## Multi-ACK Producer Configuration

**File:** `MultiAckProducerConfig.java`

```java
package edu.anant.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class MultiAckProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // acks=0 Producer (for logs)
    @Bean("acksZeroKafkaTemplate")
    public KafkaTemplate<String, String> acksZeroKafkaTemplate() {

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "0");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);

        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

    // acks=1 Producer (for general events)
    @Bean("acksOneKafkaTemplate")
    public KafkaTemplate<String, String> acksOneKafkaTemplate() {

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

    // acks=all Producer (for critical data)
    @Bean("acksAllKafkaTemplate")
    @Primary
    public KafkaTemplate<String, String> acksAllKafkaTemplate() {

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

}
```

---

## Application Properties

**File:** `application.properties`

```properties
# Kafka
spring.kafka.bootstrap-servers=localhost:9092

# Default Producer (acks=all)
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.acks=all
spring.kafka.producer.retries=2147483647
spring.kafka.producer.properties.enable.idempotence=true
```

---

# 8. Compare Behavior

## ACK Comparison Service

**File:** `AckComparisonService.java`

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
public class AckComparisonService {

    private final KafkaTemplate<String, String> acksZeroKafkaTemplate;
    private final KafkaTemplate<String, String> acksOneKafkaTemplate;
    private final KafkaTemplate<String, String> acksAllKafkaTemplate;

    // Send with acks=0
    public void sendWithAcks0(String topic, String key, String message) {

        long startTime = System.currentTimeMillis();

        log.info("========================================");
        log.info("SENDING WITH acks=0");
        log.info("Topic: {}", topic);
        log.info("Key  : {}", key);
        log.info("Message: {}", message);

        acksZeroKafkaTemplate.send(topic, key, message);

        long endTime = System.currentTimeMillis();
        log.info("Time taken: {} ms (no callback)", (endTime - startTime));
        log.info("========================================");
    }

    // Send with acks=1
    public void sendWithAcks1(String topic, String key, String message) {

        long startTime = System.currentTimeMillis();

        log.info("========================================");
        log.info("SENDING WITH acks=1");
        log.info("Topic: {}", topic);
        log.info("Key  : {}", key);
        log.info("Message: {}", message);

        acksOneKafkaTemplate.send(topic, key, message)
            .whenComplete((result, exception) -> {

                long endTime = System.currentTimeMillis();
                long duration = endTime - startTime;

                if (exception != null) {
                    log.error("FAILED (acks=1): {}", exception.getMessage());
                    log.error("Time taken: {} ms", duration);
                    return;
                }

                log.info("SUCCESS (acks=1)");
                log.info("Offset: {}", result.getRecordMetadata().offset());
                log.info("Time taken: {} ms", duration);
            });

        log.info("========================================");
    }

    // Send with acks=all
    public void sendWithAcksAll(String topic, String key, String message) {

        long startTime = System.currentTimeMillis();

        log.info("========================================");
        log.info("SENDING WITH acks=all");
        log.info("Topic: {}", topic);
        log.info("Key  : {}", key);
        log.info("Message: {}", message);

        acksAllKafkaTemplate.send(topic, key, message)
            .whenComplete((result, exception) -> {

                long endTime = System.currentTimeMillis();
                long duration = endTime - startTime;

                if (exception != null) {
                    log.error("FAILED (acks=all): {}", exception.getMessage());
                    log.error("Time taken: {} ms", duration);
                    return;
                }

                log.info("SUCCESS (acks=all)");
                log.info("Offset: {}", result.getRecordMetadata().offset());
                log.info("Time taken: {} ms", duration);
            });

        log.info("========================================");
    }

    // Send same message with all three ACK modes for comparison
    public void compareAllAckModes(String topic, String key, String message) {

        log.info("========================================");
        log.info("COMPARING ALL ACK MODES");
        log.info("Topic: {}", topic);
        log.info("Key  : {}", key);
        log.info("Message: {}", message);
        log.info("========================================");

        sendWithAcks0(topic, key, message);
        
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        sendWithAcks1(topic, key, message);
        
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        sendWithAcksAll(topic, key, message);
    }

}
```

---

## REST Controller for ACK Comparison

**File:** `AckComparisonController.java`

```java
package edu.anant.controller;

import edu.anant.service.AckComparisonService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/acks")
@RequiredArgsConstructor
public class AckComparisonController {

    private final AckComparisonService comparisonService;

    // Send with acks=0
    @PostMapping("/acks0")
    public String sendWithAcks0(
            @RequestParam String topic,
            @RequestParam String key,
            @RequestParam String message
    ) {

        comparisonService.sendWithAcks0(topic, key, message);
        return "Message sent with acks=0 (fire and forget)";
    }

    // Send with acks=1
    @PostMapping("/acks1")
    public String sendWithAcks1(
            @RequestParam String topic,
            @RequestParam String key,
            @RequestParam String message
    ) {

        comparisonService.sendWithAcks1(topic, key, message);
        return "Message sent with acks=1 (check logs for result)";
    }

    // Send with acks=all
    @PostMapping("/acks-all")
    public String sendWithAcksAll(
            @RequestParam String topic,
            @RequestParam String key,
            @RequestParam String message
    ) {

        comparisonService.sendWithAcksAll(topic, key, message);
        return "Message sent with acks=all (check logs for result)";
    }

    // Compare all three modes
    @PostMapping("/compare")
    public String compareAll(
            @RequestParam String topic,
            @RequestParam String key,
            @RequestParam String message
    ) {

        comparisonService.compareAllAckModes(topic, key, message);
        return "Messages sent with all ACK modes - check logs for comparison";
    }

}
```

---

## Testing with Postman

### Test acks=0

**Request**

```http
POST http://localhost:8080/api/acks/acks0?topic=test-topic&key=test-1&message=hello-acks0
```

**Response**

```text
Message sent with acks=0 (fire and forget)
```

---

### Test acks=1

**Request**

```http
POST http://localhost:8080/api/acks/acks1?topic=test-topic&key=test-2&message=hello-acks1
```

**Response**

```text
Message sent with acks=1 (check logs for result)
```

---

### Test acks=all

**Request**

```http
POST http://localhost:8080/api/acks/acks-all?topic=test-topic&key=test-3&message=hello-acks-all
```

**Response**

```text
Message sent with acks=all (check logs for result)
```

---

### Compare All Modes

**Request**

```http
POST http://localhost:8080/api/acks/compare?topic=test-topic&key=test-4&message=comparison-test
```

**Response**

```text
Messages sent with all ACK modes - check logs for comparison
```

---

## Expected Log Output

```text
========================================
COMPARING ALL ACK MODES
Topic: test-topic
Key  : test-4
Message: comparison-test
========================================

========================================
SENDING WITH acks=0
Topic: test-topic
Key  : test-4
Message: comparison-test
Time taken: 2 ms (no callback)
========================================

========================================
SENDING WITH acks=1
Topic: test-topic
Key  : test-4
Message: comparison-test
========================================
SUCCESS (acks=1)
Offset: 15
Time taken: 12 ms
========================================

========================================
SENDING WITH acks=all
Topic: test-topic
Key  : test-4
Message: comparison-test
========================================
SUCCESS (acks=all)
Offset: 16
Time taken: 25 ms
========================================
```

---

# 9. Production Best Practices

## Choose ACK Mode Based on Data Criticality

| Data Type | Recommended ACK | Reason |
|---|---|---|
| Application logs | `acks=0` | Performance over reliability |
| User activity | `acks=1` | Good balance |
| Order events | `acks=all` | Business critical |
| Payment transactions | `acks=all` | Zero tolerance for loss |
| Audit logs | `acks=all` | Compliance requirement |
| Metrics | `acks=0` or `acks=1` | Occasional loss acceptable |
| Notifications | `acks=1` | Generally important |

---

## Combine with Other Settings

### For acks=0

```properties
spring.kafka.producer.acks=0
spring.kafka.producer.retries=0
spring.kafka.producer.properties.enable.idempotence=false
```

---

### For acks=1

```properties
spring.kafka.producer.acks=1
spring.kafka.producer.retries=3
spring.kafka.producer.properties.enable.idempotence=true
spring.kafka.producer.properties.request.timeout.ms=30000
```

---

### For acks=all

```properties
spring.kafka.producer.acks=all
spring.kafka.producer.retries=2147483647
spring.kafka.producer.properties.enable.idempotence=true
spring.kafka.producer.properties.request.timeout.ms=60000
spring.kafka.producer.properties.delivery.timeout.ms=120000
```

---

## Monitor ACK Performance

Track these metrics:

- Average latency per ACK mode
- Success rate per ACK mode
- Retry count per ACK mode
- Message loss rate (if measurable)
- Throughput per ACK mode

---

## Common Pitfalls

### Pitfall 1: Using acks=0 for Critical Data

❌ Wrong:

```properties
# Payment transactions with acks=0
spring.kafka.producer.acks=0
```

✅ Correct:

```properties
# Payment transactions with acks=all
spring.kafka.producer.acks=all
```

---

### Pitfall 2: Using acks=all for High-Volume Logs

❌ Wrong:

```properties
# Application logs with acks=all
spring.kafka.producer.acks=all
```

✅ Correct:

```properties
# Application logs with acks=0
spring.kafka.producer.acks=0
```

---

### Pitfall 3: Not Enabling Idempotence with acks=all

❌ Wrong:

```properties
spring.kafka.producer.acks=all
# Idempotence not enabled - duplicates possible on retry
```

✅ Correct:

```properties
spring.kafka.producer.acks=all
spring.kafka.producer.properties.enable.idempotence=true
```

---

### Pitfall 4: Too Few Retries with acks=all

❌ Wrong:

```properties
spring.kafka.producer.acks=all
spring.kafka.producer.retries=0
# No retries - defeats purpose of acks=all
```

✅ Correct:

```properties
spring.kafka.producer.acks=all
spring.kafka.producer.retries=2147483647
# Max retries with idempotence
```

---

# 10. Interview Questions

## Q1. What does the `acks` configuration control?

It controls how many replicas must acknowledge a message before the producer considers it successfully sent.

## Q2. What is the difference between acks=0, acks=1, and acks=all?

- `acks=0`: No acknowledgement
- `acks=1`: Leader replica acknowledges
- `acks=all`: All in-sync replicas acknowledge

## Q3. Which ACK mode provides the highest reliability?

`acks=all` provides the highest reliability as all in-sync replicas must acknowledge.

## Q4. Which ACK mode provides the best performance?

`acks=0` provides the best performance as there's no waiting for acknowledgements.

## Q5. Can you use idempotence with acks=0?

No, idempotence requires acknowledgements to track sequence numbers.

## Q6. When should you use acks=0?

For non-critical data like logs, metrics, or debug information where occasional loss is acceptable.

## Q7. What happens if you use acks=all with only one replica?

It behaves like `acks=1` since there's only one replica to acknowledge.

## Q8. Why enable idempotence with acks=all?

To prevent duplicate messages if the producer retries after a timeout.

## Q9. What is the relationship between acks and min.insync.replicas?

For `acks=all` to guarantee durability, `min.insync.replicas` should be at least 2.

## Q10. How do you choose the right ACK mode?

Based on data criticality: critical data uses `acks=all`, general data uses `acks=1`, logs/metrics use `acks=0`.

---

# 11. Chapter Checklist

- [x] What are producer acknowledgements
- [x] How acks=0 works
- [x] How acks=1 works
- [x] How acks=all works
- [x] Reliability vs performance trade-offs
- [x] Durability guarantees
- [x] Configure different ACK modes
- [x] Compare behavior of different ACKs
- [x] Production best practices
- [x] Common pitfalls and solutions
- [x] Interview questions

---

# Next Chapter

## Chapter — Kafka Transactions and Exactly-Once Semantics

Topics:

- What are Kafka transactions?
- Exactly-once vs at-least-once vs at-most-once
- Transactional producers
- Transactional consumers
- Read committed vs read uncommitted
- Transaction configuration
- Transactional patterns
- Common pitfalls and solutions
- Production best practices