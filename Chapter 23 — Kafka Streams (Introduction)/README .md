# Chapter — Kafka Streams (Introduction)

> In this chapter, we will learn what Kafka Streams is, how stream processing works, the difference between stateless and stateful operations, KStream vs KTable, stream topology, and how to build basic Kafka Streams applications with transformations and aggregations.

---

## Learning Objectives

After completing this chapter, you will understand:

- What is Kafka Streams?
- Stream processing concepts
- Stateless operations
- Stateful operations
- KStream API
- KTable API
- Stream topology design
- Basic Kafka Streams application
- Stream transformations
- Aggregation examples

---

# 1. What is Kafka Streams?

**Kafka Streams** is a client library for building stream processing applications that process data in real-time from Kafka topics.

```text
Traditional Batch Processing:
Data ──► Store ──► Process Later ──► Results


Stream Processing (Kafka Streams):
Data ──► Process Immediately ──► Results
         (Real-time)
```

---

## Key Features

| Feature | Description |
|---|---|
| **Real-time Processing** | Process messages as they arrive |
| **Scalable** | Scale horizontally across multiple instances |
| **Fault-tolerant** | Automatic recovery from failures |
| **Stateful** | Maintain state with state stores |
| **Exactly-once** | Guarantee exactly-once processing |
| **No External Dependencies** | Only requires Kafka cluster |
| **Embedded Library** | Runs in your application |

---

## Kafka Streams Architecture

```text
    ┌─────────────┐
    │   Kafka     │
    │   Topics    │
    └─────────────┘
         │
         │ Input Streams
         ▼
    ┌─────────────┐
    │   Kafka     │
    │   Streams   │
    │  Application│
    └─────────────┘
         │
         │ Output Streams
         ▼
    ┌─────────────┐
    │   Kafka     │
    │   Topics    │
    └─────────────┘
```

---

## Use Cases

| Use Case | Description |
|---|---|
| **Real-time Analytics** | Calculate metrics as data arrives |
| **Event-driven Applications** | React to events in real-time |
| **Data Pipelines** | Transform and route data |
| **Monitoring** | Detect anomalies and patterns |
| **ETL** | Extract, transform, load in real-time |
| **Session Tracking** | Track user sessions |
| **Fraud Detection** | Detect fraudulent patterns |

---

# 2. Stream Processing

## What is Stream Processing?

Stream processing is the practice of processing data continuously as it arrives, rather than processing batches of data at scheduled intervals.

```text
Batch Processing:
┌─────┐ ┌─────┐ ┌─────┐
│Batch│ │Batch│ │Batch│  Process at scheduled times
└─────┘ └─────┘ └─────┘


Stream Processing:
Message → Message → Message → Message  Process immediately
```

---

## Stream Processing Patterns

### Pattern 1: Filter

```text
Input Stream
    │
    ▼
┌─────────────┐
│  Filter     │  Keep only messages matching condition
└─────────────┘
    │
    ▼
Output Stream
```

---

### Pattern 2: Transform

```text
Input Stream
    │
    ▼
┌─────────────┐
│  Transform  │  Convert message format
└─────────────┘
    │
    ▼
Output Stream
```

---

### Pattern 3: Aggregate

```text
Input Stream
    │
    ▼
┌─────────────┐
│  Aggregate  │  Calculate running totals
└─────────────┘
    │
    ▼
Output Stream (aggregated results)
```

---

### Pattern 4: Join

```text
Stream A ──┐
           ├──► Join ──► Combined Stream
Stream B ──┘
```

---

### Pattern 5: Branch

```text
        Input Stream
             │
             ▼
        ┌─────────┐
        │  Branch │
        └─────────┘
           │   │
           ▼   ▼
      Output1 Output2
```

---

## Stream Processing vs Batch Processing

| Aspect | Stream Processing | Batch Processing |
|---|---|---|
| **Data** | Continuous stream | Fixed-size batches |
| **Latency** | Milliseconds to seconds | Minutes to hours |
| **Processing** | Real-time | Scheduled |
| **State** | Incremental updates | Full recomputation |
| **Use Case** | Real-time analytics, monitoring | Reports, historical analysis |
| **Examples** | Kafka Streams, Flink, Spark Streaming | Hadoop, Spark (batch mode) |

---

# 3. Stateless Operations

## What are Stateless Operations?

Stateless operations process each message independently without maintaining any state between messages.

```text
Message 1 ──► Process ──► Result 1
Message 2 ──► Process ──► Result 2
Message 3 ──► Process ──► Result 3

(No dependency between messages)
```

---

## Stateless Operations Examples

### Filter

```java
KStream<String, Order> orders = ...;

// Keep only orders with amount > 1000
KStream<String, Order> highValueOrders = 
    orders.filter((key, order) -> order.getAmount() > 1000);
```

---

### Map (Transform)

```java
KStream<String, Order> orders = ...;

// Transform Order to OrderSummary
KStream<String, OrderSummary> summaries = 
    orders.mapValues(order -> 
        new OrderSummary(order.getId(), order.getTotal())
    );
```

---

### Peek (Side Effect)

```java
KStream<String, Order> orders = ...;

// Log orders without modifying
orders.peek((key, order) -> 
    log.info("Processing order: {}", order.getId())
);
```

---

### Branch (Split Stream)

```java
KStream<String, Order> orders = ...;

// Split into high-value and low-value orders
KStream<String, Order>[] branches = 
    orders.branch(
        (key, order) -> order.getAmount() > 1000,  // High value
        (key, order) -> order.getAmount() <= 1000  // Low value
    );

KStream<String, Order> highValueOrders = branches;
KStream<String, Order> lowValueOrders = branches;
```

---

## Stateless Operations Characteristics

| Characteristic | Description |
|---|---|
| **No State** | No memory of previous messages |
| **Independent** | Each message processed independently |
| **Scalable** | Easy to scale horizontally |
| **Fast** | Low latency processing |
| **Examples** | Filter, Map, Peek, Branch |

---

# 4. Stateful Operations

## What are Stateful Operations?

Stateful operations maintain state between messages, allowing operations like aggregation, joins, and windowing.

```text
Message 1 ──┐
Message 2 ──┼──► State Store ──► Aggregated Result
Message 3 ──┘

(State maintained across messages)
```

---

## Stateful Operations Examples

### Aggregate (Running Total)

```java
KStream<String, Transaction> transactions = ...;

// Calculate running total per account
KTable<String, Double> accountTotals = 
    transactions
        .groupByKey()
        .aggregate(
            () -> 0.0,  // Initial value
            (key, transaction, total) -> total + transaction.getAmount(),
            Materialized.as("account-totals-store")
        );
```

---

### Count

```java
KStream<String, Event> events = ...;

// Count events per type
KTable<String, Long> eventCounts = 
    events
        .groupByKey()
        .count(Materialized.as("event-counts-store"));
```

---

### Reduce

```java
KStream<String, Metric> metrics = ...;

// Calculate average per sensor
KTable<String, Double> sensorAverages = 
    metrics
        .groupByKey()
        .reduce(
            (value1, value2) -> (value1 + value2) / 2.0,
            Materialized.as("sensor-averages-store")
        );
```

---

### Join (Stream-Stream)

```java
KStream<String, Order> orders = ...;
KStream<String, Payment> payments = ...;

// Join orders with payments
KStream<String, OrderPayment> orderPayments = 
    orders
        .join(
            payments,
            (order, payment) -> new OrderPayment(order, payment),
            JoinWindows.of(Duration.ofMinutes(5))
        );
```

---

## Stateful Operations Characteristics

| Characteristic | Description |
|---|---|
| **State Maintained** | Remembers previous messages |
| **State Store** | Uses local or remote state store |
| **Fault-tolerant** | State is replicated in Kafka |
| **Examples** | Aggregate, Count, Reduce, Join, Window |

---

# 5. KStream

## What is KStream?

**KStream** is the primary abstraction in Kafka Streams representing a stream of records.

```text
KStream = Stream of individual messages

KStream<String, Order>
├── Message 1: Order{id=1, amount=100}
├── Message 2: Order{id=2, amount=200}
├── Message 3: Order{id=3, amount=300}
└── ...
```

---

## KStream Properties

| Property | Description |
|---|---|
| **Record-oriented** | Individual messages |
| **Immutable** | Cannot modify existing records |
| **Append-only** | New records are appended |
| **Time-unbounded** | Continuous stream |
| **Primary use** | Stream processing |

---

## KStream Operations

```java
// Create KStream from topic
KStream<String, String> stream = 
    builder.stream("input-topic");

// Filter
KStream<String, String> filtered = 
    stream.filter((key, value) -> value != null);

// Map
KStream<String, String> mapped = 
    stream.map((key, value) -> new KeyValue<>(key, value.toUpperCase()));

// Transform
KStream<String, String> transformed = 
    stream.transform(() -> new MyTransformer());

// Branch
KStream<String, String>[] branches = 
    stream.branch(
        (key, value) -> value.startsWith("A"),
        (key, value) -> !value.startsWith("A")
    );

// Merge
KStream<String, String> merged = 
    stream1.merge(stream2);

// Print
stream.print(Printed.toSysOut());

// Write to topic
stream.to("output-topic");
```

---

## KStream Example

**File:** `OrderProcessingStream.java`

```java
package edu.anant.streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

@Slf4j
public class OrderProcessingStream {

    public static void main(String[] args) {

        // Configuration
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-processing-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Build topology
        StreamsBuilder builder = new StreamsBuilder();

        // Create KStream from input topic
        KStream<String, String> orders = builder.stream("orders-topic");

        // Filter high-value orders
        KStream<String, String> highValueOrders = 
            orders.filter((key, value) -> {
                double amount = extractAmount(value);
                return amount > 1000;
            });

        // Transform to uppercase
        KStream<String, String> transformed = 
            highValueOrders.mapValues(value -> value.toUpperCase());

        // Print to console (for debugging)
        transformed.print();

        // Write to output topic
        transformed.to("high-value-orders-topic");

        // Start streaming
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        streams.start();

        log.info("Order processing stream started");

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static double extractAmount(String orderJson) {

        // Extract amount from JSON (simplified)
        // In production, use proper JSON parsing
        return 1500.0;
    }

}
```

---

# 6. KTable

## What is KTable?

**KTable** is an abstraction representing a changelog stream, where each record is an update to a key-value pair.

```text
KTable = Table of key-value pairs (latest value per key)

KTable<String, Order>
├── Key: order-1 → Value: Order{id=1, status=SHIPPED}
├── Key: order-2 → Value: Order{id=2, status=PENDING}
├── Key: order-3 → Value: Order{id=3, status=DELIVERED}
└── ...

(Only latest value per key is retained)
```

---

## KTable Properties

| Property | Description |
|---|---|
| **Key-value oriented** | Latest value per key |
| **Updates** | Records are updates to keys |
| **Stateful** | Maintains current state |
| **Materialized** | Can be stored in state store |
| **Primary use** | Aggregations, latest state |

---

## KTable Operations

```java
// Create KTable from topic
KTable<String, String> table = 
    builder.table("state-topic");

// Filter
KTable<String, String> filtered = 
    table.filter((key, value) -> value != null);

// MapValues
KTable<String, String> mapped = 
    table.mapValues(value -> value.toUpperCase());

// GroupByKey
KTable<String, String> grouped = 
    table.groupByKey();

// Aggregate
KTable<String, Long> aggregated = 
    table.groupByKey()
        .count();

// Join (Table-Table)
KTable<String, String> joined = 
    table1.join(
        table2,
        (value1, value2) -> value1 + "-" + value2
    );

// ToStream (convert to KStream)
KStream<String, String> stream = 
    table.toStream();
```

---

## KTable Example

**File:** `OrderStatusTable.java`

```java
package edu.anant.streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

@Slf4j
public class OrderStatusTable {

    public static void main(String[] args) {

        // Configuration
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-status-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Build topology
        StreamsBuilder builder = new StreamsBuilder();

        // Create KTable from order status topic
        KTable<String, String> orderStatuses = 
            builder.table("order-status-topic");

        // Filter only active orders
        KTable<String, String> activeOrders = 
            orderStatuses.filter((key, status) -> 
                !status.equals("CANCELLED")
            );

        // Map status to display format
        KTable<String, String> displayStatuses = 
            activeOrders.mapValues(status -> 
                "Order Status: " + status
            );

        // Print to console
        displayStatuses.toStream().print();

        // Write to output topic
        displayStatuses.to("order-status-display-topic");

        // Start streaming
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        streams.start();

        log.info("Order status table started");

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
```

---

## KStream vs KTable

| Feature | KStream | KTable |
|---|---|---|
| **Data Model** | Stream of records | Table of key-value pairs |
| **Semantics** | Immutable records | Latest value per key |
| **Use Case** | Event processing | State tracking |
| **Storage** | Not stored | Can be materialized |
| **Example** | Order events | Order status |
| **Operations** | Filter, Map, Transform | Aggregate, Count, Join |

---

# 7. Stream Topology

## What is Stream Topology?

A **stream topology** is the graph of stream processing operations that define how data flows through your application.

```text
        Input Topic
             │
             ▼
        ┌─────────┐
        │  Stream │
        └─────────┘
             │
             ├──► Filter ──► Transform ──► Output Topic 1
             │
             └──► Aggregate ──► Output Topic 2
```

---

## Topology Components

```text
Stream Topology
├── Sources (Input topics)
├── Processors (Operations)
│   ├── Stateless (Filter, Map)
│   └── Stateful (Aggregate, Join)
├── Sinks (Output topics)
└── State Stores (For stateful operations)
```

---

## Topology Example

```java
StreamsBuilder builder = new StreamsBuilder();

// Source
KStream<String, String> source = builder.stream("input-topic");

// Processor 1: Filter
KStream<String, String> filtered = 
    source.filter((key, value) -> value != null);

// Processor 2: Transform
KStream<String, String> transformed = 
    filtered.mapValues(value -> value.toUpperCase());

// Processor 3: Branch
KStream<String, String>[] branches = 
    transformed.branch(
        (key, value) -> value.startsWith("A"),
        (key, value) -> !value.startsWith("A")
    );

// Sink 1
branches.to("output-a-topic");

// Sink 2
branches.to("output-b-topic");

// Build topology
Topology topology = builder.build();
```

---

## Topology Visualization

```text
Topology Diagram:

                 input-topic
                      │
                      ▼
                 ┌─────────┐
                 │  Stream │
                 └─────────┘
                      │
                      ▼
                 ┌─────────┐
                 │  Filter │
                 └─────────┘
                      │
                      ▼
                 ┌─────────┐
                 │  Transform │
                 └─────────┘
                      │
            ┌─────────┴─────────┐
            │                   │
            ▼                   ▼
       ┌─────────┐        ┌─────────┐
       │ Branch A│        │ Branch B│
       └─────────┘        └─────────┘
            │                   │
            ▼                   ▼
      output-a-topic      output-b-topic
```

---

# 8. Basic Kafka Streams Application

## Complete Example

**File:** `BasicStreamsApplication.java`

```java
package edu.anant.streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

@Slf4j
public class BasicStreamsApplication {

    public static void main(String[] args) {

        log.info("Starting Kafka Streams application...");

        // Configuration
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "basic-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Build topology
        StreamsBuilder builder = new StreamsBuilder();

        // Create input stream
        KStream<String, String> source = builder.stream("input-topic");

        // Process: Filter, Transform, Branch
        KStream<String, String> processed = 
            source
                .filter((key, value) -> value != null && !value.isEmpty())
                .mapValues(value -> value.trim().toUpperCase());

        // Print to console
        processed.print();

        // Write to output topic
        processed.to("output-topic");

        // Create streams instance
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // Start processing
        streams.start();

        log.info("Kafka Streams application started successfully");

        // Add shutdown hook for graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {

            log.info("Shutting down Kafka Streams application...");
            streams.close();
            log.info("Kafka Streams application stopped");

        }));
    }

}
```

---

## Spring Boot Kafka Streams Application

**File:** `KafkaStreamsConfig.java`

```java
package edu.anant.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.util.Properties;

@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.streams.application-id}")
    private String applicationId;

    @Bean
    public Properties streamsConfiguration() {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Enable exactly-once semantics
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        return props;
    }

    @Bean
    public KStream<String, String> kStream(StreamsBuilder builder) {

        // Create stream from input topic
        KStream<String, String> stream = builder.stream("input-topic");

        // Process: Filter and transform
        stream
            .filter((key, value) -> value != null && !value.isEmpty())
            .mapValues(value -> value.toUpperCase())
            .to("output-topic");

        return stream;
    }

}
```

---

## Application Properties

**File:** `application.properties`

```properties
# Kafka
spring.kafka.bootstrap-servers=localhost:9092

# Kafka Streams
spring.kafka.streams.application-id=my-streams-app
spring.kafka.streams.properties.schema.registry.url=http://localhost:8081

# Logging
logging.level.org.apache.kafka.streams=INFO
logging.level.edu.anant=DEBUG
```

---

# 9. Stream Transformation

## Map Transformation

**File:** `MapTransformationExample.java`

```java
package edu.anant.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class MapTransformationExample {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "map-transform-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> stream = builder.stream("input-topic");

        // Map transformation: Convert to uppercase
        KStream<String, String> uppercased = 
            stream.mapValues(value -> value.toUpperCase());

        // Map transformation: Change key
        KStream<String, String> newKeys = 
            stream.map((key, value) -> {
                String newKey = "prefix-" + key;
                return new KeyValue<>(newKey, value);
            });

        // Map transformation: Extract fields from JSON
        KStream<String, String> extracted = 
            stream.map((key, value) -> {
                // Parse JSON and extract fields
                String id = extractId(value);
                String name = extractName(value);
                return new KeyValue<>(id, name);
            });

        uppercased.to("output-topic");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

    private static String extractId(String json) {
        // Extract ID from JSON
        return "id-123";
    }

    private static String extractName(String json) {
        // Extract name from JSON
        return "John Doe";
    }

}
```

---

## Filter Transformation

**File:** `FilterTransformationExample.java`

```java
package edu.anant.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class FilterTransformationExample {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "filter-transform-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> stream = builder.stream("input-topic");

        // Filter: Keep only non-null values
        KStream<String, String> nonNull = 
            stream.filter((key, value) -> value != null);

        // Filter: Keep only values starting with "A"
        KStream<String, String> startsWithA = 
            stream.filter((key, value) -> value.startsWith("A"));

        // Filter: Keep only high-value orders
        KStream<String, String> highValue = 
            stream.filter((key, value) -> {
                double amount = extractAmount(value);
                return amount > 1000;
            });

        // FilterNot: Remove null values
        KStream<String, String> filtered = 
            stream.filterNot((key, value) -> value == null);

        nonNull.to("output-topic");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

    private static double extractAmount(String orderJson) {
        // Extract amount from JSON
        return 1500.0;
    }

}
```

---

## Transform (Stateful)

**File:** `CustomTransformExample.java`

```java
package edu.anant.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Properties;

public class CustomTransformExample {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "custom-transform-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> stream = builder.stream("input-topic");

        // Custom transformer with state
        KStream<String, String> transformed = 
            stream.transform(() -> new MyCustomTransformer(), "my-state-store");

        transformed.to("output-topic");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

    // Custom transformer implementation
    static class MyCustomTransformer implements Transformer<String, String, KeyValue<String, String>> {

        private ProcessorContext context;
        private KeyValueStore<String, String> stateStore;

        @Override
        @SuppressWarnings("unchecked")
        public void init(ProcessorContext context) {

            this.context = context;
            this.stateStore = context.getStateStore("my-state-store");
        }

        @Override
        public KeyValue<String, String> transform(String key, String value) {

            // Get previous value from state
            String previousValue = stateStore.get(key);

            // Update state
            stateStore.put(key, value);

            // Return transformed value
            String transformed = transformValue(value, previousValue);

            return new KeyValue<>(key, transformed);
        }

        @Override
        public void close() {
            // Cleanup resources
        }

        private String transformValue(String currentValue, String previousValue) {

            // Custom transformation logic
            return currentValue.toUpperCase();
        }
    }

}
```

---

# 10. Aggregation Example

## Count Aggregation

**File:** `CountAggregationExample.java`

```java
package edu.anant.streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.state.Materialized;

import java.util.Properties;

@Slf4j
public class CountAggregationExample {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "count-aggregation-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // Input stream of page views
        KStream<String, String> pageViews = builder.stream("page-views-topic");

        // Group by page URL (key)
        KGroupedStream<String, String> groupedByPage = 
            pageViews.groupByKey();

        // Count page views per URL
        KTable<String, Long> pageCount = 
            groupedByPage.count(
                Materialized.as("page-count-store")
            );

        // Print counts to console
        pageCount.toStream().print();

        // Write to output topic
        pageCount.toStream().to("page-counts-topic");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        log.info("Count aggregation started");
    }

}
```

---

## Sum Aggregation

**File:** `SumAggregationExample.java`

```java
package edu.anant.streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Properties;

@Slf4j
public class SumAggregationExample {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "sum-aggregation-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Double().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // Input stream of transactions
        KStream<String, Double> transactions = builder.stream("transactions-topic");

        // Group by account ID
        KGroupedStream<String, Double> groupedByAccount = 
            transactions.groupByKey();

        // Calculate running sum per account
        KTable<String, Double> accountTotals = 
            groupedByAccount.aggregate(
                () -> 0.0,  // Initial value
                (accountId, transactionAmount, currentTotal) -> 
                    currentTotal + transactionAmount,  // Adder
                Materialized.as("account-totals-store")
            );

        // Print totals to console
        accountTotals.toStream().print();

        // Write to output topic
        accountTotals.toStream().to("account-totals-topic");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        log.info("Sum aggregation started");
    }

}
```

---

## Complex Aggregation

**File:** `ComplexAggregationExample.java`

```java
package edu.anant.streams;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.Properties;

@Slf4j
public class ComplexAggregationExample {

    @Data
    @AllArgsConstructor
    public static class OrderStats {

        private Long count;
        private Double totalAmount;
        private Double averageAmount;
        private Double minAmount;
        private Double maxAmount;

        public static OrderStats initial() {
            return new OrderStats(0L, 0.0, 0.0, Double.MAX_VALUE, Double.MIN_VALUE);
        }

        public OrderStats addOrder(Double amount) {

            Long newCount = this.count + 1;
            Double newTotal = this.totalAmount + amount;
            Double newAverage = newTotal / newCount;
            Double newMin = Math.min(this.minAmount, amount);
            Double newMax = Math.max(this.maxAmount, amount);

            return new OrderStats(newCount, newTotal, newAverage, newMin, newMax);
        }
    }

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "complex-aggregation-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Double().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // Input stream of order amounts
        KStream<String, Double> orders = builder.stream("orders-topic");

        // Group by customer ID
        KGroupedStream<String, Double> groupedByCustomer = 
            orders.groupByKey();

        // Calculate complex statistics per customer
        KTable<String, OrderStats> customerStats = 
            groupedByCustomer.aggregate(
                OrderStats::initial,  // Initializer
                (customerId, orderAmount, stats) -> 
                    stats.addOrder(orderAmount),  // Aggregator
                Materialized.as("customer-stats-store")
            );

        // Print stats to console
        customerStats.toStream().print();

        // Write to output topic
        customerStats.toStream().to("customer-stats-topic");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        log.info("Complex aggregation started");
    }

}
```

---

# 11. Interview Questions

## Q1. What is Kafka Streams?

Kafka Streams is a client library for building real-time stream processing applications that process data from Kafka topics.

---

## Q2. What is the difference between KStream and KTable?

- **KStream**: Stream of individual records (event stream)
- **KTable**: Table of key-value pairs (changelog stream, latest value per key)

---

## Q3. What are stateless operations?

Operations that process each message independently without maintaining state, e.g., filter, map, transform.

---

## Q4. What are stateful operations?

Operations that maintain state between messages, e.g., aggregate, count, reduce, join.

---

## Q5. What is a stream topology?

The graph of stream processing operations that defines how data flows through a Kafka Streams application.

---

## Q6. What is the purpose of Materialized?

It specifies that a state store should be created to materialize the result of a stateful operation.

---

## Q7. How do you handle state in Kafka Streams?

Using state stores (in-memory or persistent) that are automatically managed and replicated by Kafka Streams.

---

## Q8. What is exactly-once processing in Kafka Streams?

A guarantee that each message is processed exactly once, even in case of failures, achieved through transactions.

---

## Q9. How do you scale Kafka Streams applications?

By running multiple instances with the same application ID; Kafka Streams automatically partitions work across instances.

---

## Q10. What are common use cases for Kafka Streams?

Real-time analytics, event-driven applications, data pipelines, monitoring, fraud detection, session tracking.

---

# 12. Chapter Checklist

- [x] What is Kafka Streams?
- [x] Stream processing concepts
- [x] Stateless operations
- [x] Stateful operations
- [x] KStream API
- [x] KTable API
- [x] Stream topology design
- [x] Basic Kafka Streams application
- [x] Stream transformations
- [x] Aggregation examples
- [x] Interview questions

---

# Next Chapter

## Chapter — Kafka Streams Advanced Topics

Topics:

- Windowing operations
- Joins (Stream-Stream, Stream-Table, Table-Table)
- Global KTable
- Interactive queries
- State stores (in-memory, persistent)
- Fault tolerance
- Testing Kafka Streams
- Production deployment
- Performance tuning
