# Chapter 15 — Kafka Multiple Topics

> In this chapter, we will learn how to work with multiple Kafka topics in a single Spring Boot application, including topic design, naming conventions, creating multiple topics, multiple producers, multiple consumers, and consuming from different topics.

---

## Learning Objectives

After completing this chapter, you will understand:

- Why use multiple topics?
- Topic design principles
- Naming conventions for topics
- Topic organization strategies
- How to create multiple topics
- How to configure multiple producers
- How to configure multiple consumers
- How to consume from different topics
- How to route messages to different topics
- Best practices for multi-topic applications
- Common patterns and anti-patterns

---

# 1. Why Multiple Topics?

Real-world applications rarely use just one Kafka topic. Multiple topics help organize different types of data and workflows.

## Use Cases for Multiple Topics

| Scenario | Topics |
|---|---|
| **E-commerce** | `orders`, `payments`, `shipments`, `notifications` |
| **Social Media** | `posts`, `comments`, `likes`, `messages`, `notifications` |
| **Banking** | `transactions`, `fraud-detection`, `alerts`, `audit-logs` |
| **IoT** | `sensor-temperature`, `sensor-humidity`, `sensor-pressure`, `alerts` |
| **Microservices** | `user-events`, `order-events`, `inventory-events`, `email-events` |

---

## Benefits of Multiple Topics

### 1. Separation of Concerns

Different business domains use different topics.

```text
Order Service     → orders-topic
Payment Service   → payments-topic
Email Service     → emails-topic
```

### 2. Independent Scaling

Each topic can have different partition counts based on load.

```text
orders-topic      → 10 partitions (high volume)
emails-topic      → 3 partitions (medium volume)
audit-logs-topic  → 1 partition (low volume)
```

### 3. Different Retention Policies

Different topics can have different retention periods.

```text
orders-topic      → 7 days retention
audit-logs-topic  → 365 days retention
temp-data-topic   → 1 hour retention
```

### 4. Independent Consumer Groups

Different consumer groups can process different topics at their own pace.

```text
Order Consumers   → orders-topic (fast processing)
Analytics         → orders-topic (slow processing)
Audit             → audit-logs-topic (batch processing)
```

### 5. Security and Access Control

Different teams can have access to different topics.

```text
Finance Team      → payments-topic, transactions-topic
Marketing Team    → user-events-topic, campaigns-topic
Operations Team   → alerts-topic, notifications-topic
```

---

# 2. Topic Design Principles

## Domain-Driven Topic Design

Organize topics by business domain.

```text
User Domain
├── user-created
├── user-updated
├── user-deleted

Order Domain
├── order-created
├── order-updated
├── order-shipped
├── order-cancelled

Payment Domain
├── payment-initiated
├── payment-completed
├── payment-failed
```

---

## Event-Based Topic Design

Organize topics by event type.

```text
Commands (Actions to perform)
├── create-user
├── process-payment
├── send-email

Events (Things that happened)
├── user-created
├── payment-processed
├── email-sent

Queries (Data requests)
├── get-user
├── get-order
├── get-inventory
```

---

## Data Volume-Based Design

Organize topics by data volume and importance.

```text
High Volume, Low Importance
├── clickstream-events
├── page-views
├── user-activity-logs

Medium Volume, Medium Importance
├── user-events
├── order-events
├── notification-events

Low Volume, High Importance
├── financial-transactions
├── audit-logs
├── compliance-events
```

---

# 3. Naming Conventions

Good naming conventions make topics self-documenting and easy to manage.

## Recommended Naming Pattern

```text
<domain>-<entity>-<event-type>
```

### Examples

| Domain | Entity | Event Type | Topic Name |
|---|---|---|---|
| `user` | `profile` | `created` | `user-profile-created` |
| `order` | `order` | `shipped` | `order-order-shipped` |
| `payment` | `transaction` | `completed` | `payment-transaction-completed` |
| `notification` | `email` | `sent` | `notification-email-sent` |

---

## Alternative Naming Patterns

### Pattern 1: Entity-Event

```text
<entity>-<event>
```

Examples:

```text
user-created
user-updated
order-placed
order-shipped
payment-received
payment-failed
```

### Pattern 2: Domain-Entity

```text
<domain>.<entity>
```

Examples:

```text
users.profiles
orders.transactions
payments.history
notifications.emails
```

### Pattern 3: Environment-Domain-Entity

```text
<env>-<domain>-<entity>
```

Examples:

```text
prod-users-profiles
dev-orders-transactions
staging-payments-history
```

---

## Naming Best Practices

### Do's ✅

- Use lowercase letters
- Use hyphens as separators
- Keep names descriptive but concise
- Use consistent patterns across all topics
- Include domain or entity context
- Use plural form for entities

```text
✅ user-events
✅ order-notifications
✅ payment-transactions
✅ inventory-updates
```

### Don'ts ❌

- Don't use uppercase letters
- Don't use underscores (inconsistent with Kafka conventions)
- Don't use special characters
- Don't make names too long
- Don't mix naming patterns

```text
❌ UserEvents
❌ order_notifications
❌ payment@transactions
❌ this-is-a-very-long-topic-name-that-is-hard-to-read
❌ user_events (mixing hyphens and underscores)
```

---

# 4. Topic Organization

## By Microservice

```text
User Service Topics
├── user-service.user-created
├── user-service.user-updated
├── user-service.user-deleted

Order Service Topics
├── order-service.order-created
├── order-service.order-updated
├── order-service.order-cancelled

Payment Service Topics
├── payment-service.payment-initiated
├── payment-service.payment-completed
├── payment-service.payment-failed
```

---

## By Data Sensitivity

```text
Public Data
├── product-catalog-updates
├── inventory-status
├── pricing-changes

Internal Data
├── user-behavior-events
├── system-metrics
├── performance-logs

Sensitive Data
├── financial-transactions
├── personal-information-updates
├── authentication-events
```

---

## By Processing Requirements

```text
Real-time Processing
├── live-orders
├── instant-notifications
├── real-time-analytics

Batch Processing
├── daily-reports
├── nightly-aggregations
├── weekly-summaries

Archive/Compliance
├── audit-logs
├── compliance-records
├── historical-data
```

---

# 5. Creating Multiple Topics

## Topic Configuration Class

**File:** `KafkaTopicConfig.java`

```java
package edu.anant.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    // User Events Topic
    @Bean
    public NewTopic userEventsTopic() {

        return TopicBuilder
            .name("user-events")
            .partitions(6)
            .replicas(3)
            .build();
    }

    // Order Events Topic
    @Bean
    public NewTopic orderEventsTopic() {

        return TopicBuilder
            .name("order-events")
            .partitions(12)
            .replicas(3)
            .build();
    }

    // Payment Events Topic
    @Bean
    public NewTopic paymentEventsTopic() {

        return TopicBuilder
            .name("payment-events")
            .partitions(6)
            .replicas(3)
            .build();
    }

    // Notification Events Topic
    @Bean
    public NewTopic notificationEventsTopic() {

        return TopicBuilder
            .name("notification-events")
            .partitions(3)
            .replicas(3)
            .build();
    }

    // Audit Logs Topic
    @Bean
    public NewTopic auditLogsTopic() {

        return TopicBuilder
            .name("audit-logs")
            .partitions(3)
            .replicas(3)
            .build();
    }

    // Email Notifications Topic
    @Bean
    public NewTopic emailNotificationsTopic() {

        return TopicBuilder
            .name("email-notifications")
            .partitions(3)
            .replicas(3)
            .build();
    }

}
```

---

## Topic Configuration with Retention

**File:** `KafkaTopicConfigWithRetention.java`

```java
package edu.anant.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaTopicConfigWithRetention {

    // Orders Topic - 7 days retention
    @Bean
    public NewTopic ordersTopic() {

        Map<String, String> configs = new HashMap<>();
        configs.put("retention.ms", String.valueOf(7 * 24 * 60 * 60 * 1000)); // 7 days

        return TopicBuilder
            .name("orders")
            .partitions(12)
            .replicas(3)
            .configs(configs)
            .build();
    }

    // Audit Logs Topic - 365 days retention
    @Bean
    public NewTopic auditLogsTopic() {

        Map<String, String> configs = new HashMap<>();
        configs.put("retention.ms", String.valueOf(365L * 24 * 60 * 60 * 1000)); // 365 days

        return TopicBuilder
            .name("audit-logs")
            .partitions(3)
            .replicas(3)
            .configs(configs)
            .build();
    }

    // Temporary Data Topic - 1 hour retention
    @Bean
    public NewTopic tempDataTopic() {

        Map<String, String> configs = new HashMap<>();
        configs.put("retention.ms", String.valueOf(60 * 60 * 1000)); // 1 hour

        return TopicBuilder
            .name("temp-data")
            .partitions(6)
            .replicas(3)
            .configs(configs)
            .build();
    }

}
```

---

## Topic Configuration with Cleanup Policy

**File:** `KafkaTopicConfigWithCleanup.java`

```java
package edu.anant.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaTopicConfigWithCleanup {

    // Event Stream Topic - Delete old messages
    @Bean
    public NewTopic eventStreamTopic() {

        Map<String, String> configs = new HashMap<>();
        configs.put("cleanup.policy", "delete");
        configs.put("retention.ms", String.valueOf(7 * 24 * 60 * 60 * 1000));

        return TopicBuilder
            .name("event-stream")
            .partitions(6)
            .replicas(3)
            .configs(configs)
            .build();
    }

    // State Topic - Keep latest value per key (compaction)
    @Bean
    public NewTopic userStateTopic() {

        Map<String, String> configs = new HashMap<>();
        configs.put("cleanup.policy", "compact");
        configs.put("min.compaction.lag.ms", String.valueOf(60 * 60 * 1000)); // 1 hour

        return TopicBuilder
            .name("user-state")
            .partitions(6)
            .replicas(3)
            .configs(configs)
            .build();
    }

    // Hybrid Topic - Both delete and compact
    @Bean
    public NewTopic orderStateTopic() {

        Map<String, String> configs = new HashMap<>();
        configs.put("cleanup.policy", "compact,delete");
        configs.put("retention.ms", String.valueOf(30 * 24 * 60 * 60 * 1000)); // 30 days

        return TopicBuilder
            .name("order-state")
            .partitions(6)
            .replicas(3)
            .configs(configs)
            .build();
    }

}
```

---

# 6. Multiple Producers

## Multiple KafkaTemplate Beans

**File:** `KafkaProducerConfig.java`

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
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // User Events Producer
    @Bean
    public KafkaTemplate<String, String> userEventsKafkaTemplate() {

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

    // Order Events Producer
    @Bean
    public KafkaTemplate<String, String> orderEventsKafkaTemplate() {

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

    // Payment Events Producer
    @Bean
    public KafkaTemplate<String, String> paymentEventsKafkaTemplate() {

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

    // Notification Events Producer
    @Bean
    public KafkaTemplate<String, String> notificationEventsKafkaTemplate() {

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 1);

        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

}
```

---

## Producer Service for Multiple Topics

**File:** `MultiTopicProducerService.java`

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
public class MultiTopicProducerService {

    private static final String USER_EVENTS_TOPIC = "user-events";
    private static final String ORDER_EVENTS_TOPIC = "order-events";
    private static final String PAYMENT_EVENTS_TOPIC = "payment-events";
    private static final String NOTIFICATION_EVENTS_TOPIC = "notification-events";
    private static final String AUDIT_LOGS_TOPIC = "audit-logs";

    private final KafkaTemplate<String, String> userEventsKafkaTemplate;
    private final KafkaTemplate<String, String> orderEventsKafkaTemplate;
    private final KafkaTemplate<String, String> paymentEventsKafkaTemplate;
    private final KafkaTemplate<String, String> notificationEventsKafkaTemplate;
    private final KafkaTemplate<String, String> auditLogsKafkaTemplate;

    // Send User Event
    public void sendUserEvent(String key, String event) {

        log.info("Sending user event to {}: {}", USER_EVENTS_TOPIC, event);

        userEventsKafkaTemplate.send(USER_EVENTS_TOPIC, key, event)
            .whenComplete((result, exception) -> {

                if (exception != null) {
                    log.error("Failed to send user event: {}", exception.getMessage());
                    return;
                }

                log.info("User event sent successfully - Topic: {}, Partition: {}, Offset: {}",
                    result.getRecordMetadata().topic(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset()
                );
            });
    }

    // Send Order Event
    public void sendOrderEvent(String key, String event) {

        log.info("Sending order event to {}: {}", ORDER_EVENTS_TOPIC, event);

        orderEventsKafkaTemplate.send(ORDER_EVENTS_TOPIC, key, event)
            .whenComplete((result, exception) -> {

                if (exception != null) {
                    log.error("Failed to send order event: {}", exception.getMessage());
                    return;
                }

                log.info("Order event sent successfully - Topic: {}, Partition: {}, Offset: {}",
                    result.getRecordMetadata().topic(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset()
                );
            });
    }

    // Send Payment Event
    public void sendPaymentEvent(String key, String event) {

        log.info("Sending payment event to {}: {}", PAYMENT_EVENTS_TOPIC, event);

        paymentEventsKafkaTemplate.send(PAYMENT_EVENTS_TOPIC, key, event)
            .whenComplete((result, exception) -> {

                if (exception != null) {
                    log.error("Failed to send payment event: {}", exception.getMessage());
                    return;
                }

                log.info("Payment event sent successfully - Topic: {}, Partition: {}, Offset: {}",
                    result.getRecordMetadata().topic(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset()
                );
            });
    }

    // Send Notification Event
    public void sendNotificationEvent(String key, String event) {

        log.info("Sending notification event to {}: {}", NOTIFICATION_EVENTS_TOPIC, event);

        notificationEventsKafkaTemplate.send(NOTIFICATION_EVENTS_TOPIC, key, event)
            .whenComplete((result, exception) -> {

                if (exception != null) {
                    log.error("Failed to send notification event: {}", exception.getMessage());
                    return;
                }

                log.info("Notification event sent successfully - Topic: {}, Partition: {}, Offset: {}",
                    result.getRecordMetadata().topic(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset()
                );
            });
    }

    // Send Audit Log
    public void sendAuditLog(String key, String logEntry) {

        log.info("Sending audit log to {}: {}", AUDIT_LOGS_TOPIC, logEntry);

        auditLogsKafkaTemplate.send(AUDIT_LOGS_TOPIC, key, logEntry)
            .whenComplete((result, exception) -> {

                if (exception != null) {
                    log.error("Failed to send audit log: {}", exception.getMessage());
                    return;
                }

                log.info("Audit log sent successfully - Topic: {}, Partition: {}, Offset: {}",
                    result.getRecordMetadata().topic(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset()
                );
            });
    }

}
```

---

## Generic Multi-Topic Producer

**File:** `GenericMultiTopicProducerService.java`

```java
package edu.anant.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class GenericMultiTopicProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    // Send to any topic
    public void sendToTopic(String topic, String key, String message) {

        log.info("Sending message to topic {}: {}", topic, message);

        kafkaTemplate.send(topic, key, message)
            .whenComplete((result, exception) -> {

                if (exception != null) {
                    log.error("Failed to send message to topic {}: {}", 
                        topic, exception.getMessage());
                    return;
                }

                log.info("Message sent to {} - Partition: {}, Offset: {}",
                    topic,
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset()
                );
            });
    }

    // Send to multiple topics (fan-out pattern)
    public void sendToMultipleTopics(String[] topics, String key, String message) {

        for (String topic : topics) {
            sendToTopic(topic, key, message);
        }
    }

}
```

---

# 7. Multiple Consumers

## Multiple Listener Container Factories

**File:** `KafkaConsumerConfig.java`

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
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // User Events Consumer Factory
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> 
    userEventsListenerContainerFactory() {

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "user-events-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        DefaultKafkaConsumerFactory<String, String> consumerFactory = 
            new DefaultKafkaConsumerFactory<>(props);

        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(3);

        return factory;
    }

    // Order Events Consumer Factory
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> 
    orderEventsListenerContainerFactory() {

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-events-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        DefaultKafkaConsumerFactory<String, String> consumerFactory = 
            new DefaultKafkaConsumerFactory<>(props);

        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(6);

        return factory;
    }

    // Payment Events Consumer Factory
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> 
    paymentEventsListenerContainerFactory() {

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "payment-events-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        DefaultKafkaConsumerFactory<String, String> consumerFactory = 
            new DefaultKafkaConsumerFactory<>(props);

        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(3);

        return factory;
    }

    // Notification Events Consumer Factory
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> 
    notificationEventsListenerContainerFactory() {

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "notification-events-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        DefaultKafkaConsumerFactory<String, String> consumerFactory = 
            new DefaultKafkaConsumerFactory<>(props);

        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(2);

        return factory;
    }

}
```

---

## Multiple Consumer Services

**File:** `UserEventsConsumerService.java`

```java
package edu.anant.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class UserEventsConsumerService {

    @KafkaListener(
        topics = "user-events",
        groupId = "user-events-group",
        containerFactory = "userEventsListenerContainerFactory"
    )
    public void consumeUserEvents(ConsumerRecord<String, String> record) {

        log.info("========================================");
        log.info("USER EVENT RECEIVED");
        log.info("Topic      : {}", record.topic());
        log.info("Partition  : {}", record.partition());
        log.info("Offset     : {}", record.offset());
        log.info("Key        : {}", record.key());
        log.info("Value      : {}", record.value());
        log.info("Timestamp  : {}", record.timestamp());
        log.info("========================================");

        // Process user event
        processUserEvent(record.key(), record.value());
    }

    private void processUserEvent(String key, String event) {

        // Business logic for user events
        log.info("Processing user event: {} - {}", key, event);

        // Example: Update user database, send notifications, etc.
    }

}
```

---

**File:** `OrderEventsConsumerService.java`

```java
package edu.anant.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class OrderEventsConsumerService {

    @KafkaListener(
        topics = "order-events",
        groupId = "order-events-group",
        containerFactory = "orderEventsListenerContainerFactory"
    )
    public void consumeOrderEvents(ConsumerRecord<String, String> record) {

        log.info("========================================");
        log.info("ORDER EVENT RECEIVED");
        log.info("Topic      : {}", record.topic());
        log.info("Partition  : {}", record.partition());
        log.info("Offset     : {}", record.offset());
        log.info("Key        : {}", record.key());
        log.info("Value      : {}", record.value());
        log.info("Timestamp  : {}", record.timestamp());
        log.info("========================================");

        // Process order event
        processOrderEvent(record.key(), record.value());
    }

    private void processOrderEvent(String key, String event) {

        // Business logic for order events
        log.info("Processing order event: {} - {}", key, event);

        // Example: Update order status, trigger shipping, etc.
    }

}
```

---

**File:** `PaymentEventsConsumerService.java`

```java
package edu.anant.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class PaymentEventsConsumerService {

    @KafkaListener(
        topics = "payment-events",
        groupId = "payment-events-group",
        containerFactory = "paymentEventsListenerContainerFactory"
    )
    public void consumePaymentEvents(ConsumerRecord<String, String> record) {

        log.info("========================================");
        log.info("PAYMENT EVENT RECEIVED");
        log.info("Topic      : {}", record.topic());
        log.info("Partition  : {}", record.partition());
        log.info("Offset     : {}", record.offset());
        log.info("Key        : {}", record.key());
        log.info("Value      : {}", record.value());
        log.info("Timestamp  : {}", record.timestamp());
        log.info("========================================");

        // Process payment event
        processPaymentEvent(record.key(), record.value());
    }

    private void processPaymentEvent(String key, String event) {

        // Business logic for payment events
        log.info("Processing payment event: {} - {}", key, event);

        // Example: Update payment status, send receipts, etc.
    }

}
```

---

**File:** `NotificationEventsConsumerService.java`

```java
package edu.anant.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class NotificationEventsConsumerService {

    @KafkaListener(
        topics = "notification-events",
        groupId = "notification-events-group",
        containerFactory = "notificationEventsListenerContainerFactory"
    )
    public void consumeNotificationEvents(ConsumerRecord<String, String> record) {

        log.info("========================================");
        log.info("NOTIFICATION EVENT RECEIVED");
        log.info("Topic      : {}", record.topic());
        log.info("Partition  : {}", record.partition());
        log.info("Offset     : {}", record.offset());
        log.info("Key        : {}", record.key());
        log.info("Value      : {}", record.value());
        log.info("Timestamp  : {}", record.timestamp());
        log.info("========================================");

        // Process notification event
        processNotificationEvent(record.key(), record.value());
    }

    private void processNotificationEvent(String key, String event) {

        // Business logic for notification events
        log.info("Processing notification event: {} - {}", key, event);

        // Example: Send email, SMS, push notification, etc.
    }

}
```

---

# 8. Consume from Different Topics

## Single Consumer, Multiple Topics

**File:** `MultiTopicConsumerService.java`

```java
package edu.anant.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class MultiTopicConsumerService {

    // Consume from multiple topics with same listener
    @KafkaListener(
        topics = {"user-events", "order-events", "payment-events"},
        groupId = "multi-topic-group",
        containerFactory = "multiTopicListenerContainerFactory"
    )
    public void consumeFromMultipleTopics(ConsumerRecord<String, String> record) {

        log.info("========================================");
        log.info("MESSAGE RECEIVED FROM ANY TOPIC");
        log.info("Topic      : {}", record.topic());
        log.info("Partition  : {}", record.partition());
        log.info("Offset     : {}", record.offset());
        log.info("Key        : {}", record.key());
        log.info("Value      : {}", record.value());
        log.info("========================================");

        // Route based on topic
        routeMessage(record.topic(), record.key(), record.value());
    }

    private void routeMessage(String topic, String key, String value) {

        switch (topic) {
            case "user-events":
                log.info("Processing user event: {} - {}", key, value);
                // Handle user event
                break;

            case "order-events":
                log.info("Processing order event: {} - {}", key, value);
                // Handle order event
                break;

            case "payment-events":
                log.info("Processing payment event: {} - {}", key, value);
                // Handle payment event
                break;

            default:
                log.warn("Unknown topic: {}", topic);
        }
    }

}
```

---

## Topic Pattern Matching

**File:** `PatternBasedConsumerService.java`

```java
package edu.anant.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class PatternBasedConsumerService {

    // Consume from all topics matching pattern
    @KafkaListener(
        topicPattern = ".*-events",
        groupId = "pattern-consumer-group",
        containerFactory = "patternListenerContainerFactory"
    )
    public void consumeFromPatternTopics(ConsumerRecord<String, String> record) {

        log.info("========================================");
        log.info("PATTERN-BASED MESSAGE RECEIVED");
        log.info("Topic      : {}", record.topic());
        log.info("Partition  : {}", record.partition());
        log.info("Offset     : {}", record.offset());
        log.info("Key        : {}", record.key());
        log.info("Value      : {}", record.value());
        log.info("========================================");

        // Process message
        processMessage(record.topic(), record.key(), record.value());
    }

    private void processMessage(String topic, String key, String value) {

        log.info("Processing message from {}: {} - {}", topic, key, value);

        // Business logic
    }

}
```

---

## Conditional Topic Consumption

**File:** `ConditionalConsumerService.java`

```java
package edu.anant.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ConditionalConsumerService {

    @Value("${app.environment:dev}")
    private String environment;

    // Dev environment - consume all events
    @KafkaListener(
        topics = {"user-events", "order-events", "payment-events"},
        groupId = "dev-consumer-group",
        containerFactory = "devListenerContainerFactory",
        condition = "#{environment == 'dev'}"
    )
    public void consumeInDev(ConsumerRecord<String, String> record) {

        log.info("DEV - Processing: {} - {}", record.topic(), record.value());
    }

    // Production - consume only critical events
    @KafkaListener(
        topics = {"payment-events", "order-events"},
        groupId = "prod-consumer-group",
        containerFactory = "prodListenerContainerFactory",
        condition = "#{environment == 'prod'}"
    )
    public void consumeInProd(ConsumerRecord<String, String> record) {

        log.info("PROD - Processing critical: {} - {}", record.topic(), record.value());
    }

}
```

---

# 9. REST Controller for Multi-Topic Testing

**File:** `MultiTopicController.java`

```java
package edu.anant.controller;

import edu.anant.service.MultiTopicProducerService;
import edu.anant.service.GenericMultiTopicProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/kafka")
@RequiredArgsConstructor
public class MultiTopicController {

    private final MultiTopicProducerService producerService;
    private final GenericMultiTopicProducerService genericProducerService;

    // Send User Event
    @PostMapping("/users")
    public String sendUserEvent(
            @RequestParam String key,
            @RequestParam String event
    ) {

        producerService.sendUserEvent(key, event);
        return "User event sent";
    }

    // Send Order Event
    @PostMapping("/orders")
    public String sendOrderEvent(
            @RequestParam String key,
            @RequestParam String event
    ) {

        producerService.sendOrderEvent(key, event);
        return "Order event sent";
    }

    // Send Payment Event
    @PostMapping("/payments")
    public String sendPaymentEvent(
            @RequestParam String key,
            @RequestParam String event
    ) {

        producerService.sendPaymentEvent(key, event);
        return "Payment event sent";
    }

    // Send Notification Event
    @PostMapping("/notifications")
    public String sendNotificationEvent(
            @RequestParam String key,
            @RequestParam String event
    ) {

        producerService.sendNotificationEvent(key, event);
        return "Notification event sent";
    }

    // Send Audit Log
    @PostMapping("/audit")
    public String sendAuditLog(
            @RequestParam String key,
            @RequestParam String logEntry
    ) {

        producerService.sendAuditLog(key, logEntry);
        return "Audit log sent";
    }

    // Send to Any Topic
    @PostMapping("/send")
    public String sendToAnyTopic(
            @RequestParam String topic,
            @RequestParam String key,
            @RequestParam String message
    ) {

        genericProducerService.sendToTopic(topic, key, message);
        return "Message sent to " + topic;
    }

    // Send to Multiple Topics (Fan-out)
    @PostMapping("/fanout")
    public String sendToMultipleTopics(
            @RequestParam String[] topics,
            @RequestParam String key,
            @RequestParam String message
    ) {

        genericProducerService.sendToMultipleTopics(topics, key, message);
        return "Message sent to " + topics.length + " topics";
    }

}
```

---

# 10. Testing with Postman

## Send User Event

**Request**

```http
POST http://localhost:8080/api/kafka/users?key=user-101&event=user-created-Anant
```

**Response**

```text
User event sent
```

---

## Send Order Event

**Request**

```http
POST http://localhost:8080/api/kafka/orders?key=order-5001&event=order-placed-Laptop
```

**Response**

```text
Order event sent
```

---

## Send Payment Event

**Request**

```http
POST http://localhost:8080/api/kafka/payments?key=payment-9001&event=payment-completed-85000
```

**Response**

```text
Payment event sent
```

---

## Send to Any Topic

**Request**

```http
POST http://localhost:8080/api/kafka/send?topic=custom-topic&key=test-1&message=hello-kafka
```

**Response**

```text
Message sent to custom-topic
```

---

## Send to Multiple Topics (Fan-out)

**Request**

```http
POST http://localhost:8080/api/kafka/fanout?topics=user-events&topics=order-events&topics=audit-logs&key=broadcast-1&message=system-broadcast
```

**Response**

```text
Message sent to 3 topics
```

---

# 11. Application Properties

**File:** `application.properties`

```properties
# Application
spring.application.name=kafka-multi-topic-demo

# Kafka
spring.kafka.bootstrap-servers=localhost:9092

# Consumer Defaults
spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer

# Environment
app.environment=dev
```

---

# 12. Best Practices for Multiple Topics

## Topic Design Best Practices

| Practice | Description |
|---|---|
| **Clear Naming** | Use descriptive, consistent names |
| **Domain Separation** | Separate topics by business domain |
| **Appropriate Partitions** | Set partitions based on expected volume |
| **Retention Policies** | Configure retention based on data importance |
| **Documentation** | Document topic purpose and schema |

---

## Producer Best Practices

| Practice | Description |
|---|---|
| **Dedicated Templates** | Use separate KafkaTemplate for critical topics |
| **Error Handling** | Implement proper error handling per topic |
| **Idempotence** | Enable for financial/critical topics |
| **Acknowledgments** | Use `acks=all` for important data |
| **Retry Logic** | Configure retries based on topic importance |

---

## Consumer Best Practices

| Practice | Description |
|---|---|
| **Separate Consumer Groups** | Different topics should have different groups |
| **Appropriate Concurrency** | Set concurrency based on partition count |
| **Error Handling** | Implement topic-specific error handling |
| **Monitoring** | Monitor consumer lag per topic |
| **Offset Management** | Configure offset reset appropriately |

---

## Common Patterns

### Pattern 1: Event Sourcing

```text
Commands                    Events
├── create-user    ───────►  ├── user-created
├── place-order    ───────►  ├── order-placed
└── process-payment ──────►  └── payment-processed
```

### Pattern 2: CQRS (Command Query Responsibility Segregation)

```text
Commands                 Queries
├── create-user   ◄────┐  ├── user-queries
├── update-user   ◄────┤  ├── user-details
└── delete-user   ◄────┘  └── user-history
```

### Pattern 3: Fan-out

```text
                    ┌──► user-events
source-topic ──────┼──► order-events
                    └──► audit-logs
```

### Pattern 4: Aggregation

```text
user-events ────┐
order-events ───┼──► aggregated-events
payment-events ─┘
```

---

## Anti-Patterns to Avoid

| Anti-Pattern | Problem | Solution |
|---|---|---|
| Too many topics | Hard to manage, monitor | Consolidate related events |
| Too few topics | Mixed concerns, hard to scale | Separate by domain |
| Inconsistent naming | Confusing, error-prone | Establish naming conventions |
| No documentation | Unknown topic purpose | Document all topics |
| Ignoring retention | Storage issues, compliance risks | Set appropriate retention |
| Same consumer group for all topics | Coupled processing | Use separate consumer groups |

---

# 13. Monitoring Multiple Topics

## Key Metrics to Monitor

| Metric | Description |
|---|---|
| **Messages per second** | Throughput per topic |
| **Consumer lag** | Delay in processing per topic |
| **Partition distribution** | Load balance across partitions |
| **Error rates** | Failed messages per topic |
| **Retention usage** | Storage consumption per topic |

---

## Tools for Monitoring

- **Confluent Control Center** - Visual topic monitoring
- **Kafka UI** - Open-source topic browser
- **Prometheus + Grafana** - Metrics and dashboards
- **ELK Stack** - Log aggregation and analysis
- **Custom Dashboards** - Application-specific monitoring

---

# 14. Interview Questions

## Q1. Why use multiple topics instead of one?

Multiple topics provide separation of concerns, independent scaling, different retention policies, and better security controls.

## Q2. What are good naming conventions for Kafka topics?

Use lowercase, hyphens as separators, and descriptive names like `user-events`, `order-notifications`, `payment-transactions`.

## Q3. How do you decide the number of partitions for a topic?

Based on expected message volume, parallelism requirements, and consumer concurrency needs.

## Q4. What is the difference between delete and compact cleanup policy?

- `delete`: Removes old messages based on retention
- `compact`: Keeps only the latest value per key

## Q5. Can one consumer listen to multiple topics?

Yes, using `@KafkaListener(topics = {"topic1", "topic2", "topic3"})`.

## Q6. What is fan-out pattern in Kafka?

Sending the same message to multiple topics for different processing pipelines.

## Q7. How do you handle schema evolution across multiple topics?

Use Schema Registry with compatibility checks, and evolve schemas carefully with default values.

## Q8. What metrics should you monitor for multiple topics?

Messages per second, consumer lag, partition distribution, error rates, and retention usage.

## Q9. When should you use separate KafkaTemplate beans?

For topics with different reliability requirements, acknowledgment settings, or serialization formats.

## Q10. What is topic pattern matching?

Using regex patterns in `@KafkaListener(topicPattern = ".*-events")` to consume from multiple topics matching a pattern.

---

# 15. Chapter Checklist

- [x] Why use multiple topics?
- [x] Topic design principles
- [x] Naming conventions
- [x] Topic organization strategies
- [x] Creating multiple topics
- [x] Multiple producer configuration
- [x] Multiple consumer configuration
- [x] Consuming from different topics
- [x] Pattern-based topic consumption
- [x] REST controller for testing
- [x] Best practices
- [x] Common patterns and anti-patterns
- [x] Monitoring strategies
- [x] Interview questions

---

# Next Chapter

## Chapter 16 — Kafka Security and Authentication

Topics:

- SSL/TLS encryption
- SASL authentication
- ACLs (Access Control Lists)
- Authentication mechanisms (PLAIN, SCRAM, OAuth)
- Authorization and permissions
- Secure producer configuration
- Secure consumer configuration
- Production security best practices