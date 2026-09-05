# Chapter — Kafka Best Practices and Patterns

> In this chapter, we will learn design patterns for Kafka applications, error handling patterns, retry and recovery patterns, message ordering patterns, exactly-once patterns, schema evolution patterns, monitoring patterns, security patterns, and a comprehensive production checklist.

---

## Learning Objectives

After completing this chapter, you will understand:

- Design patterns for Kafka applications
- Error handling patterns
- Retry and recovery patterns
- Message ordering patterns
- Exactly-once processing patterns
- Schema evolution patterns
- Monitoring and observability patterns
- Security patterns
- Production deployment checklist
- Anti-patterns to avoid

---

# 1. Design Patterns for Kafka Applications

## Pattern 1: Event Sourcing

```text
Commands ──► Events ──► Event Store ──► Projections

┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
│  Command │────►│  Event   │────►│  Kafka   │────►│  Read    │
│          │     │  Source  │     │  Topics  │     │  Model   │
└──────────┘     └──────────┘     └──────────┘     └──────────┘
```

**Use Cases:**

- Audit trails
- Temporal queries
- CQRS architectures
- Financial systems

**Example:**

```java
// Command
CreateUserCommand command = new CreateUserCommand("John", "john@example.com");

// Event
UserCreatedEvent event = new UserCreatedEvent(
    UUID.randomUUID(),
    command.getName(),
    command.getEmail(),
    Instant.now()
);

// Publish to Kafka
kafkaTemplate.send("user-events", event.getUserId().toString(), event);
```

---

## Pattern 2: CQRS (Command Query Responsibility Segregation)

```text
         Commands                    Queries
             │                           │
             ▼                           ▼
    ┌─────────────┐              ┌─────────────┐
    │   Write     │              │    Read     │
    │   Model     │              │    Model    │
    └─────────────┘              └─────────────┘
         │                           ▲
         │                           │
         ▼                           │
    ┌─────────────┐                  │
    │   Kafka     │──────────────────┘
    │   Events    │     (Projection)
    └─────────────┘
```

**Use Cases:**

- High read/write ratio systems
- Complex queries
- Scalability requirements

**Example:**

```java
// Write side
@PostMapping("/users")
public String createUser(@RequestBody CreateUserCommand command) {

    UserCreatedEvent event = new UserCreatedEvent(...);
    kafkaTemplate.send("user-events", event);

    return "User created";
}

// Read side (projection)
@KafkaListener(topics = "user-events")
public void projectUserEvent(UserCreatedEvent event) {

    userRepository.save(event);  // Update read model
}
```

---

## Pattern 3: Saga Pattern

```text
Saga: Multi-step transaction across services

Service A ──► Service B ──► Service C
    │             │             │
    ▼             ▼             ▼
 Event 1      Event 2       Event 3
    │             │             │
    └─────────────┴─────────────┘
              │
              ▼
        Compensation
        (if failure)
```

**Use Cases:**

- Distributed transactions
- Microservices coordination
- Long-running processes

**Example:**

```java
// Order Saga
@KafkaListener(topics = "order-created")
public void onOrderCreated(OrderCreatedEvent event) {

    try {

        // Step 1: Reserve inventory
        inventoryService.reserve(event.getOrderId(), event.getItems());
        kafkaTemplate.send("inventory-reserved", event);

        // Step 2: Process payment
        paymentService.process(event.getOrderId(), event.getAmount());
        kafkaTemplate.send("payment-processed", event);

        // Step 3: Ship order
        shippingService.ship(event.getOrderId());
        kafkaTemplate.send("order-shipped", event);

    } catch (Exception e) {

        // Compensation: Rollback
        kafkaTemplate.send("order-failed", event);
        inventoryService.release(event.getOrderId());
        paymentService.refund(event.getOrderId());
    }
}
```

---

## Pattern 4: Outbox Pattern

```text
Database Transaction ──► Outbox Table ──► Kafka

┌──────────┐     ┌──────────┐     ┌──────────┐
│ Business │────►│ Outbox   │────►│  Kafka   │
│  Logic   │     │  Table   │     │ Producer │
└──────────┘     └──────────┘     └──────────┘
     │                │
     └────────────────┘
   (Same transaction)
```

**Use Cases:**

- Atomic database + Kafka writes
- Exactly-once semantics
- Data consistency

**Example:**

```java
@Transactional
public void createOrder(Order order) {

    // Save order to database
    orderRepository.save(order);

    // Save event to outbox table (same transaction)
    OutboxEvent event = new OutboxEvent(
        "orders-topic",
        order.getId().toString(),
        serialize(order)
    );

    outboxRepository.save(event);
}

// Separate process polls outbox and publishes to Kafka
@Scheduled(fixedRate = 1000)
public void publishOutboxEvents() {

    List<OutboxEvent> events = outboxRepository.findUnpublished();

    for (OutboxEvent event : events) {

        kafkaTemplate.send(event.getTopic(), event.getKey(), event.getPayload());

        outboxRepository.markPublished(event.getId());
    }
}
```

---

## Pattern 5: Event Carried State Transfer

```text
Service A ──► Event with State ──► Service B
                                      │
                                      ▼
                                Local Cache
                                (No API call needed)
```

**Use Cases:**

- Reduce service coupling
- Improve performance
- Offline processing

**Example:**

```java
// Producer: Send complete state in event
@KafkaListener(topics = "user-updated")
public void onUserUpdated(UserUpdatedEvent event) {

    // Event contains full user state
    User user = event.getUser();

    // Update local cache
    userCache.put(user.getId(), user);

    // No need to call user service
}
```

---

# 2. Error Handling Patterns

## Pattern 1: Try-Catch with DLQ

```java
@KafkaListener(topics = "orders-topic")
public void consumeOrder(Order order) {

    try {

        processOrder(order);

    } catch (RecoverableException e) {

        // Retryable error
        throw e;  // Let retry handler deal with it

    } catch (NonRecoverableException e) {

        // Non-retryable error
        log.error("Non-recoverable error", e);
        sendToDLQ(order, e);

    } catch (Exception e) {

        // Unknown error
        log.error("Unknown error", e);
        sendToDLQ(order, e);
    }
}
```

---

## Pattern 2: Circuit Breaker

```java
private CircuitBreaker circuitBreaker = 
    CircuitBreaker.ofDefaults("kafkaConsumer");

@KafkaListener(topics = "orders-topic")
public void consumeOrder(Order order) {

    try {

        circuitBreaker.executeRunnable(() -> {

            processOrder(order);
        });

    } catch (CallNotPermittedException e) {

        // Circuit is open
        log.warn("Circuit breaker open, skipping processing");

        // Send to retry topic or DLQ
        sendToRetryTopic(order);
    }
}
```

---

## Pattern 3: Fallback Handler

```java
@KafkaListener(topics = "orders-topic")
public void consumeOrder(Order order) {

    try {

        processOrder(order);

    } catch (ExternalServiceException e) {

        // External service down
        log.error("External service unavailable", e);

        // Fallback: Save for later processing
        orderRepository.saveForRetry(order);

    } catch (Exception e) {

        // Other errors
        sendToDLQ(order, e);
    }
}
```

---

## Pattern 4: Dead Letter Queue with Metadata

```java
private void sendToDLQ(Object message, Throwable exception) {

    ProducerRecord<String, String> dltRecord = 
        new ProducerRecord<>("orders-topic.DLT", serialize(message));

    // Add error metadata
    dltRecord.headers().add("exception-class", exception.getClass().getName().getBytes());
    dltRecord.headers().add("exception-message", exception.getMessage().getBytes());
    dltRecord.headers().add("timestamp", String.valueOf(System.currentTimeMillis()).getBytes());
    dltRecord.headers().add("original-topic", "orders-topic".getBytes());

    kafkaTemplate.send(dltRecord);
}
```

---

# 3. Retry and Recovery Patterns

## Pattern 1: Exponential Backoff with Max Retries

```java
@RetryableTopic(
    attempts = "5",
    backOff = @BackOff(delay = 1000, multiplier = 2.0, maxDelay = 30000),
    dltStrategy = DltStrategy.ALWAYS_SEND
)
@KafkaListener(topics = "orders-topic")
public void consumeOrder(Order order) {

    processOrder(order);
}
```

---

## Pattern 2: Selective Retry

```java
@Bean
public CommonErrorHandler selectiveErrorHandler() {

    DefaultErrorHandler errorHandler = new DefaultErrorHandler(
        (record, exception) -> sendToDLQ(record, exception),
        new FixedBackOff(1000L, 3L)
    );

    // Retry only specific exceptions
    errorHandler.addRetryableExceptions(
        TimeoutException.class,
        ConnectException.class,
        NetworkException.class
    );

    // Don't retry validation errors
    errorHandler.addNotRetryableExceptions(
        IllegalArgumentException.class,
        ValidationException.class
    );

    return errorHandler;
}
```

---

## Pattern 3: Retry with State

```java
private Map<String, Integer> retryCounts = new ConcurrentHashMap<>();

@KafkaListener(topics = "orders-topic")
public void consumeOrder(Order order) {

    String key = order.getId().toString();
    int retryCount = retryCounts.getOrDefault(key, 0);

    if (retryCount >= 3) {

        sendToDLQ(order, new MaxRetriesExceededException());
        retryCounts.remove(key);
        return;
    }

    try {

        processOrder(order);
        retryCounts.remove(key);  // Success, reset count

    } catch (RecoverableException e) {

        retryCounts.put(key, retryCount + 1);
        throw e;  // Trigger retry
    }
}
```

---

## Pattern 4: Scheduled Retry

```java
@KafkaListener(topics = "orders-topic")
public void consumeOrder(Order order) {

    try {

        processOrder(order);

    } catch (RecoverableException e) {

        // Schedule for later retry
        retryScheduler.schedule(
            () -> kafkaTemplate.send("orders-topic", order.getId().toString(), order),
            Duration.ofMinutes(5)
        );
    }
}
```

---

# 4. Message Ordering Patterns

## Pattern 1: Partition Key for Ordering

```java
// Ensure all events for same entity go to same partition
String partitionKey = order.getCustomerId();

kafkaTemplate.send("orders-topic", partitionKey, order);

// Consumer will receive events in order for each customer
@KafkaListener(topics = "orders-topic")
public void consumeOrder(Order order) {

    // Events for same customer are ordered
    processOrder(order);
}
```

---

## Pattern 2: Single Partition for Strict Ordering

```java
// All messages to single partition (strict ordering)
kafkaTemplate.send(
    new ProducerRecord<>("orders-topic", 0, null, order)
);

// Note: Limits parallelism
```

---

## Pattern 3: Sequence Numbers

```java
// Producer: Add sequence number
@KafkaListener(topics = "orders-topic")
public void consumeOrder(Order order) {

    long expectedSequence = lastProcessedSequence + 1;

    if (order.getSequence() < expectedSequence) {

        log.warn("Duplicate or out-of-order message: {}", order.getSequence());
        return;  // Skip
    }

    if (order.getSequence() > expectedSequence) {

        log.warn("Gap in sequence, buffering...");
        bufferMessage(order);
        return;
    }

    processOrder(order);
    lastProcessedSequence = order.getSequence();

    // Process buffered messages
    processBufferedMessages();
}
```

---

## Pattern 4: Event Time Processing

```java
@KafkaListener(topics = "events-topic")
public void consumeEvent(Event event) {

    Instant eventTime = event.getTimestamp();
    Instant processingTime = Instant.now();

    Duration latency = Duration.between(eventTime, processingTime);

    if (latency.toMinutes() > 5) {

        log.warn("Late event detected: {} minutes late", latency.toMinutes());

        // Handle late event differently
        processLateEvent(event);

    } else {

        processEvent(event);
    }
}
```

---

# 5. Exactly-Once Patterns

## Pattern 1: Transactions for Write-Write

```java
@Transactional
public void processOrder(Order order) {

    // Write to database
    orderRepository.save(order);

    // Write to Kafka (same transaction)
    kafkaTemplate.send("orders-topic", order.getId().toString(), order);
}
```

---

## Pattern 2: Idempotent Consumer

```java
private Set<String> processedIds = ConcurrentHashMap.newKeySet();

@KafkaListener(topics = "orders-topic")
public void consumeOrder(Order order) {

    String idempotencyKey = order.getId().toString();

    if (processedIds.contains(idempotencyKey)) {

        log.info("Duplicate message, skipping: {}", idempotencyKey);
        return;
    }

    processOrder(order);

    processedIds.add(idempotencyKey);
}
```

---

## Pattern 3: Idempotent Producer

```properties
# Enable idempotent producer
spring.kafka.producer.properties.enable.idempotence=true
spring.kafka.producer.acks=all
spring.kafka.producer.retries=2147483647
spring.kafka.producer.properties.max.in.flight.requests.per.connection=5
```

---

## Pattern 4: Exactly-Once Streams

```java
Properties props = new Properties();
props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

KafkaStreams streams = new KafkaStreams(builder.build(), props);
```

---

# 6. Schema Evolution Patterns

## Pattern 1: Backward Compatible Changes

```java
// Version 1
public class Order {

    private Long id;
    private String customerId;
    private Double amount;
}

// Version 2 (Backward Compatible)
public class Order {

    private Long id;
    private String customerId;
    private Double amount;
    private String status = "PENDING";  // New field with default
}
```

---

## Pattern 2: Optional Fields

```java
// Use Optional or nullable fields
public class Order {

    private Long id;
    private String customerId;
    private Double amount;
    private String discountCode;  // Optional field
}
```

---

## Pattern 3: Schema Registry with Compatibility

```bash
# Set compatibility level
curl -X PUT http://localhost:8081/config \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "BACKWARD"}'
```

---

## Pattern 4: Versioned Topics

```text
orders-v1-topic  ──► Legacy consumers
orders-v2-topic  ──► New consumers

(Gradual migration)
```

---

# 7. Monitoring Patterns

## Pattern 1: Metrics Collection

```java
@Component
public class KafkaMetrics {

    private final MeterRegistry meterRegistry;

    public KafkaMetrics(MeterRegistry meterRegistry) {

        this.meterRegistry = meterRegistry;

        // Register counters and gauges
        meterRegistry.counter("kafka.messages.processed");
        meterRegistry.counter("kafka.messages.failed");
        meterRegistry.gauge("kafka.consumer.lag", this, KafkaMetrics::getConsumerLag);
    }

    public void recordMessageProcessed() {

        meterRegistry.counter("kafka.messages.processed").increment();
    }

    public void recordMessageFailed() {

        meterRegistry.counter("kafka.messages.failed").increment();
    }

    private double getConsumerLag() {

        // Calculate consumer lag
        return consumerLag;
    }
}
```

---

## Pattern 2: Distributed Tracing

```java
@KafkaListener(topics = "orders-topic")
public void consumeOrder(@Header(X_B3_TRACE_ID) String traceId, Order order) {

    // Continue trace from producer
    Span span = tracer.nextSpan().name("process-order").start();

    try (Tracer.SpanInScope scope = tracer.withSpanInScope(span)) {

        processOrder(order);

    } finally {

        span.end();
    }
}
```

---

## Pattern 3: Health Checks

```java
@RestController
public class KafkaHealthController {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @GetMapping("/health/kafka")
    public ResponseEntity<String> kafkaHealth() {

        try {

            // Send test message
            kafkaTemplate.send("health-check-topic", "test").get(5, TimeUnit.SECONDS);

            return ResponseEntity.ok("Kafka is healthy");

        } catch (Exception e) {

            return ResponseEntity.status(503).body("Kafka is unhealthy: " + e.getMessage());
        }
    }
}
```

---

## Pattern 4: Alerting on DLQ

```java
@Component
public class DLQMonitor {

    private final KafkaConsumer<String, String> consumer;
    private final AlertService alertService;

    @Scheduled(fixedRate = 60000)  // Every minute
    public void checkDLQSize() {

        TopicPartition dltPartition = new TopicPartition("orders-topic.DLT", 0);
        consumer.assign(Collections.singleton(dltPartition));
        consumer.seekToEnd(Collections.singleton(dltPartition));

        long dltSize = consumer.position(dltPartition);

        if (dltSize > 1000) {

            alertService.sendAlert(
                "CRITICAL: DLQ size exceeds threshold",
                "DLQ size: " + dltSize
            );
        }
    }
}
```

---

# 8. Security Patterns

## Pattern 1: SSL/TLS Encryption

```properties
# Producer
spring.kafka.producer.properties.security.protocol=SASL_SSL
spring.kafka.producer.properties.ssl.truststore.location=/path/to/truststore.jks
spring.kafka.producer.properties.ssl.truststore.password=password

# Consumer
spring.kafka.consumer.properties.security.protocol=SASL_SSL
spring.kafka.consumer.properties.ssl.truststore.location=/path/to/truststore.jks
spring.kafka.consumer.properties.ssl.truststore.password=password
```

---

## Pattern 2: SASL Authentication

```properties
# SCRAM-SHA-256
spring.kafka.producer.properties.sasl.mechanism=SCRAM-SHA-256
spring.kafka.producer.properties.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="producer-user" \
  password="producer-secret";
```

---

## Pattern 3: ACLs for Authorization

```bash
# Allow producer to write to specific topic
kafka-acls --bootstrap-server localhost:9092 \
  --command-config client.properties \
  --add \
  --allow-principal User:producer-user \
  --operation Write \
  --topic orders-topic

# Allow consumer to read from specific topic
kafka-acls --bootstrap-server localhost:9092 \
  --command-config client.properties \
  --add \
  --allow-principal User:consumer-user \
  --operation Read \
  --topic orders-topic \
  --group orders-consumer-group
```

---

## Pattern 4: Principle of Least Privilege

```text
Producer Service:
├── Write: orders-topic
└── No read access

Consumer Service:
├── Read: orders-topic
└── No write access

Admin Service:
├── Read/Write: all topics
└── Restricted to admin users
```

---

# 9. Production Checklist

## Pre-Deployment Checklist

- [ ] All topics created with correct partitions and replication
- [ ] Schema Registry configured with compatibility checks
- [ ] Security enabled (SSL, SASL, ACLs)
- [ ] Monitoring and metrics configured
- [ ] Logging configured with appropriate levels
- [ ] Error handling and retry mechanisms tested
- [ ] DLQ topics created and monitored
- [ ] Health checks implemented
- [ ] Alerting configured for critical metrics
- [ ] Backup and recovery procedures documented
- [ ] Performance testing completed
- [ ] Load testing completed
- [ ] Security audit completed
- [ ] Documentation complete

---

## Runtime Checklist

- [ ] Consumer lag monitored and within thresholds
- [ ] DLQ size monitored and alerts configured
- [ ] Broker health monitored (CPU, memory, disk)
- [ ] Under-replicated partitions = 0
- [ ] Offline partitions = 0
- [ ] Error rates within acceptable limits
- [ ] Throughput meets requirements
- [ ] Latency meets SLA requirements
- [ ] State stores backed up (if applicable)
- [ ] Changelog topics retained appropriately

---

## Post-Incident Checklist

- [ ] Root cause identified
- [ ] Fix implemented and tested
- [ ] Monitoring updated to detect similar issues
- [ ] Documentation updated
- [ ] Lessons learned documented
- [ ] Runbook updated
- [ ] Stakeholders informed

---

# 10. Anti-Patterns to Avoid

## Anti-Pattern 1: No Error Handling

```java
// ❌ Bad: No error handling
@KafkaListener(topics = "orders-topic")
public void consumeOrder(Order order) {

    processOrder(order);  // May throw exception
}
```

```java
// ✅ Good: Proper error handling
@KafkaListener(topics = "orders-topic")
public void consumeOrder(Order order) {

    try {

        processOrder(order);

    } catch (Exception e) {

        log.error("Failed to process order", e);
        sendToDLQ(order, e);
    }
}
```

---

## Anti-Pattern 2: Infinite Retries

```java
// ❌ Bad: Infinite retries
@Retryable(maxAttempts = -1)
@KafkaListener(topics = "orders-topic")
public void consumeOrder(Order order) {

    processOrder(order);
}
```

```java
// ✅ Good: Limited retries with DLQ
@RetryableTopic(
    attempts = "5",
    dltStrategy = DltStrategy.ALWAYS_SEND
)
@KafkaListener(topics = "orders-topic")
public void consumeOrder(Order order) {

    processOrder(order);
}
```

---

## Anti-Pattern 3: No Partition Key

```java
// ❌ Bad: No partition key (random distribution)
kafkaTemplate.send("orders-topic", order);
```

```java
// ✅ Good: Partition key for ordering
kafkaTemplate.send("orders-topic", order.getCustomerId(), order);
```

---

## Anti-Pattern 4: Large Messages

```java
// ❌ Bad: Sending large messages (> 1 MB)
kafkaTemplate.send("orders-topic", largeObject);
```

```java
// ✅ Good: Store large data externally, send reference
String objectId = storageService.store(largeObject);
kafkaTemplate.send("orders-topic", objectId);
```

---

## Anti-Pattern 5: No Monitoring

```text
// ❌ Bad: No metrics, no logging, no alerts

Application running blind
```

```text
// ✅ Good: Comprehensive monitoring

Metrics ──► Dashboards ──► Alerts
Logging ──► Aggregation ──► Search
Tracing ──► Visualization ──► Debugging
```

---

## Anti-Pattern 6: Blocking Operations in Consumer

```java
// ❌ Bad: Blocking operations
@KafkaListener(topics = "orders-topic")
public void consumeOrder(Order order) {

    // HTTP call that may take seconds
    restTemplate.postForObject("http://slow-service/api", order);

    // Database call that may hang
    orderRepository.save(order);
}
```

```java
// ✅ Good: Async processing
@KafkaListener(topics = "orders-topic")
public void consumeOrder(Order order) {

    // Quick validation
    validateOrder(order);

    // Async processing
    asyncProcessor.process(order);
}
```

---

# 11. Interview Questions

## Q1. What is the Outbox pattern?

A pattern for ensuring atomic writes to database and Kafka by using an outbox table in the same transaction.

---

## Q2. What is the Saga pattern?

A pattern for managing distributed transactions across multiple services using a sequence of events and compensations.

---

## Q3. How do you ensure message ordering in Kafka?

Use partition keys to ensure related messages go to the same partition, which preserves order within that partition.

---

## Q4. What is exactly-once processing?

A guarantee that each message is processed exactly once, achieved through idempotent producers and transactions.

---

## Q5. What are backward compatible schema changes?

Changes that allow new schema to read data written with old schema, e.g., adding fields with defaults.

---

## Q6. What metrics should you monitor in production?

Consumer lag, message throughput, error rates, DLQ size, broker health, under-replicated partitions.

---

## Q7. What is the Circuit Breaker pattern?

A pattern that stops processing when failure rate exceeds a threshold, preventing cascading failures.

---

## Q8. How do you handle retry in Kafka consumers?

Use Spring Retry or @RetryableTopic with backoff strategies and DLQ for failed messages.

---

## Q9. What security measures should you implement?

SSL/TLS encryption, SASL authentication, ACLs for authorization, principle of least privilege.

---

## Q10. What are common anti-patterns in Kafka applications?

No error handling, infinite retries, no partition keys, large messages, no monitoring, blocking operations.

---

# 12. Chapter Checklist

- [x] Design patterns for Kafka applications
- [x] Error handling patterns
- [x] Retry and recovery patterns
- [x] Message ordering patterns
- [x] Exactly-once processing patterns
- [x] Schema evolution patterns
- [x] Monitoring patterns
- [x] Security patterns
- [x] Production checklist
- [x] Anti-patterns to avoid
- [x] Interview questions

---

# 🎉 Kafka Complete Guide — Summary

You have now completed a comprehensive journey through Apache Kafka with Spring Boot!

## Topics Covered

1. ✅ Sending Java Objects
2. ✅ Kafka Error Handling
3. ✅ Kafka Avro and Schema Registry
4. ✅ Multiple Topics
5. ✅ Kafka Security and Authentication
6. ✅ Producer Acknowledgements (ACKs)
7. ✅ Transactions and Exactly-Once Semantics
8. ✅ Performance Tuning and Optimization
9. ✅ Retry Mechanism
10. ✅ Dead Letter Topic (DLT)
11. ✅ Monitoring and Observability
12. ✅ Kafka Streams (Introduction)
13. ✅ Kafka Streams Advanced Topics
14. ✅ Best Practices and Patterns

---

## Next Steps

- Practice implementing these patterns in real projects
- Build a production-ready Kafka application
- Contribute to open-source Kafka projects
- Stay updated with Kafka releases and features
- Share knowledge with the community

---

**Happy Kafka Streaming! 🚀**
