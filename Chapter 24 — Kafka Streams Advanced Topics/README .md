# Chapter — Kafka Streams Advanced Topics

> In this chapter, we will learn advanced Kafka Streams concepts including windowing operations, joins, global KTable, interactive queries, state stores, fault tolerance, testing, production deployment, and performance tuning.

---

## Learning Objectives

After completing this chapter, you will understand:

- Windowing operations (time, session, count)
- Joins (Stream-Stream, Stream-Table, Table-Table)
- Global KTable and its use cases
- Interactive queries with state stores
- State stores (in-memory, persistent, versioned)
- Fault tolerance and state recovery
- Testing Kafka Streams applications
- Production deployment strategies
- Performance tuning best practices

---

# 1. Windowing Operations

## What is Windowing?

Windowing groups records by time windows for stateful operations like aggregations.

```text
Without Windowing:
All messages ──► Aggregate ──► Single result (forever)


With Windowing:
Messages in Window 1 ──► Aggregate ──► Result 1
Messages in Window 2 ──► Aggregate ──► Result 2
Messages in Window 3 ──► Aggregate ──► Result 3
```

---

## Types of Windows

### Time Windows (Fixed)

```text
Window 1: [00:00 - 00:05)
Window 2: [00:05 - 00:10)
Window 3: [00:10 - 00:15)

(Fixed-size, non-overlapping windows)
```

---

### Hopping Windows

```text
Window 1: [00:00 - 00:10)  ◄───┐
Window 2:    [00:05 - 00:15)  ◄───┼── Overlapping
Window 3:       [00:10 - 00:20) ◄───┘

(Fixed-size, overlapping windows)
```

---

### Session Windows

```text
Session 1: [00:00 - 00:03)  (Activity burst)
Session 2: [00:10 - 00:12) (Activity burst)
Session 3: [00:20 - 00:25) (Activity burst)

(Dynamic windows based on activity gaps)
```

---

## Time Window Example

**File:** `TimeWindowExample.java`

```java
package edu.anant.streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import java.time.Duration;
import java.util.Properties;

@Slf4j
public class TimeWindowExample {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "time-window-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // Input stream of page views
        KStream<String, Long> pageViews = builder.stream("page-views-topic");

        // Group by page URL
        KTable<Windowed<String>, Long> viewsPerWindow = 
            pageViews
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGap(Duration.ofMinutes(5)))
                .count();

        // Print windowed counts
        viewsPerWindow.toStream().print();

        // Write to output topic
        viewsPerWindow.toStream().to("page-views-windowed-topic");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        log.info("Time window aggregation started");
    }

}
```

---

## Hopping Window Example

**File:** `HoppingWindowExample.java`

```java
package edu.anant.streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import java.time.Duration;
import java.util.Properties;

@Slf4j
public class HoppingWindowExample {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "hopping-window-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // Input stream of transactions
        KStream<String, Long> transactions = builder.stream("transactions-topic");

        // Group by account ID
        KTable<Windowed<String>, Long> txPerWindow = 
            transactions
                .groupByKey()
                .windowedBy(
                    TimeWindows.of(Duration.ofMinutes(10))
                        .advanceBy(Duration.ofMinutes(5))  // Hop every 5 minutes
                )
                .count();

        // Print windowed counts
        txPerWindow.toStream().print();

        // Write to output topic
        txPerWindow.toStream().to("transactions-windowed-topic");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        log.info("Hopping window aggregation started");
    }

}
```

---

## Session Window Example

**File:** `SessionWindowExample.java`

```java
package edu.anant.streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;

import java.time.Duration;
import java.util.Properties;

@Slf4j
public class SessionWindowExample {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "session-window-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // Input stream of user activities
        KStream<String, String> userActivities = builder.stream("user-activity-topic");

        // Group by user ID
        KTable<Windowed<String>, Long> sessionsPerUser = 
            userActivities
                .groupByKey()
                .windowedBy(SessionWindows.withInactivityGap(Duration.ofMinutes(5)))
                .count();

        // Print session counts
        sessionsPerUser.toStream().print();

        // Write to output topic
        sessionsPerUser.toStream().to("user-sessions-topic");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        log.info("Session window aggregation started");
    }

}
```

---

## Window Types Comparison

| Window Type | Size | Overlap | Use Case |
|---|---|---|---|
| **Time (Fixed)** | Fixed | No | Hourly/daily aggregations |
| **Hopping** | Fixed | Yes | Sliding window analytics |
| **Session** | Dynamic | No | User session tracking |
| **Sliding** | Fixed | Yes | Continuous monitoring |

---

# 2. Joins

## Join Types in Kafka Streams

```text
Join Types
├── Stream-Stream Join
├── Stream-Table Join
├── Table-Table Join
└── Global KTable Join
```

---

## Stream-Stream Join

Joins two KStreams within a time window.

**File:** `StreamStreamJoinExample.java`

```java
package edu.anant.streams;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;

import java.time.Duration;
import java.util.Properties;

@Slf4j
public class StreamStreamJoinExample {

    @Data
    @AllArgsConstructor
    public static class OrderPayment {

        private String orderId;
        private Double orderAmount;
        private String paymentStatus;
        private Double paymentAmount;
    }

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-stream-join-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // Orders stream
        KStream<String, Double> orders = builder.stream("orders-topic");

        // Payments stream
        KStream<String, String> payments = builder.stream("payments-topic");

        // Join orders with payments (within 5 minutes)
        KStream<String, OrderPayment> orderPayments = 
            orders.join(
                payments,
                (orderAmount, paymentStatus) -> 
                    new OrderPayment(
                        "order-id",
                        orderAmount,
                        paymentStatus,
                        orderAmount  // Assuming payment matches order
                    ),
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)),
                StreamJoined.with(Serdes.String(), Serdes.Double(), Serdes.String())
            );

        // Print joined results
        orderPayments.print();

        // Write to output topic
        orderPayments.to("order-payments-joined-topic");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        log.info("Stream-stream join started");
    }

}
```

---

## Stream-Table Join

Joins a KStream with a KTable (enrichment pattern).

**File:** `StreamTableJoinExample.java`

```java
package edu.anant.streams;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.Properties;

@Slf4j
public class StreamTableJoinExample {

    @Data
    @AllArgsConstructor
    public static class EnrichedOrder {

        private String orderId;
        private Double amount;
        private String customerName;
        private String customerEmail;
    }

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-table-join-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // Orders stream (key: customerId, value: orderAmount)
        KStream<String, Double> orders = builder.stream("orders-topic");

        // Customers table (key: customerId, value: customerName)
        KTable<String, String> customers = builder.table("customers-topic");

        // Join orders with customer information
        KStream<String, EnrichedOrder> enrichedOrders = 
            orders.join(
                customers,
                (ValueJoiner<Double, String, EnrichedOrder>) (orderAmount, customerName) -> 
                    new EnrichedOrder(
                        "order-id",
                        orderAmount,
                        customerName,
                        customerName + "@example.com"
                    )
            );

        // Print enriched orders
        enrichedOrders.print();

        // Write to output topic
        enrichedOrders.to("enriched-orders-topic");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        log.info("Stream-table join started");
    }

}
```

---

## Table-Table Join

Joins two KTables.

**File:** `TableTableJoinExample.java`

```java
package edu.anant.streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.Properties;

@Slf4j
public class TableTableJoinExample {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "table-table-join-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // Orders table (key: orderId, value: orderAmount)
        KTable<String, Double> orders = builder.table("orders-state-topic");

        // Payments table (key: orderId, value: paymentStatus)
        KTable<String, String> payments = builder.table("payments-state-topic");

        // Join orders with payments
        KTable<String, String> orderPaymentStatus = 
            orders.join(
                payments,
                (ValueJoiner<Double, String, String>) (orderAmount, paymentStatus) -> 
                    "Order: " + orderAmount + ", Payment: " + paymentStatus
            );

        // Print joined results
        orderPaymentStatus.toStream().print();

        // Write to output topic
        orderPaymentStatus.toStream().to("order-payment-status-topic");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        log.info("Table-table join started");
    }

}
```

---

## Join Types Comparison

| Join Type | Input 1 | Input 2 | Output | Use Case |
|---|---|---|---|---|
| **Stream-Stream** | KStream | KStream | KStream | Correlating events |
| **Stream-Table** | KStream | KTable | KStream | Enrichment |
| **Table-Table** | KTable | KTable | KTable | Merging state |
| **Global KTable** | KStream | GlobalKTable | KStream | Large table lookups |

---

# 3. Global KTable

## What is Global KTable?

**GlobalKTable** is a replicated table available on all instances, useful for large read-only datasets.

```text
Normal KTable:
Partition 0 ──► Instance 0
Partition 1 ──► Instance 1
Partition 2 ──► Instance 2


Global KTable:
Entire Table ──► Instance 0
Entire Table ──► Instance 1
Entire Table ──► Instance 2

(Full replication on all instances)
```

---

## Global KTable Use Cases

| Use Case | Description |
|---|---|
| **Large reference data** | Product catalog, customer info |
| **Read-only datasets** | Configuration, rules |
| **Enrichment** | Joining streams with large tables |
| **Lookups** | Fast key-based lookups |

---

## Global KTable Example

**File:** `GlobalKTableExample.java`

```java
package edu.anant.streams;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.Properties;

@Slf4j
public class GlobalKTableExample {

    @Data
    @AllArgsConstructor
    public static class EnrichedOrder {

        private String orderId;
        private Double amount;
        private String productName;
        private String productCategory;
    }

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "global-ktable-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // Orders stream (key: productId, value: orderAmount)
        KStream<String, Double> orders = builder.stream("orders-topic");

        // Products global table (key: productId, value: productName)
        GlobalKTable<String, String> products = 
            builder.globalTable("products-topic");

        // Join orders with product information
        KStream<String, EnrichedOrder> enrichedOrders = 
            orders.join(
                products,
                (KeyValueMapper<String, Double, String>) (orderId, orderAmount) -> orderId,  // Key selector
                (orderAmount, productName) -> 
                    new EnrichedOrder(
                        "order-id",
                        orderAmount,
                        productName,
                        "Electronics"  // Hardcoded category
                    )
            );

        // Print enriched orders
        enrichedOrders.print();

        // Write to output topic
        enrichedOrders.to("enriched-orders-topic");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        log.info("Global KTable join started");
    }

}
```

---

# 4. Interactive Queries

## What are Interactive Queries?

Interactive queries allow you to query the state store directly from outside the Streams application.

```text
Traditional:
Application ──► Process ──► Output Topic ──► Query


Interactive Queries:
Application ──► State Store ──► Query API ──► Direct Query Result
```

---

## Interactive Query Example

**File:** `InteractiveQueryExample.java`

```java
package edu.anant.streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.Properties;

@Slf4j
@RestController
public class InteractiveQueryExample {

    private KafkaStreams streams;
    private ReadOnlyKeyValueStore<String, Long> store;

    public InteractiveQueryExample() {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "interactive-query-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());

        // Configure interactive queries
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:8081");

        StreamsBuilder builder = new StreamsBuilder();

        // Input stream
        KStream<String, Long> events = builder.stream("events-topic");

        // Aggregate into state store
        KTable<String, Long> counts = 
            events
                .groupByKey()
                .count(Materialized.as("event-counts-store"));

        streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Get state store
        store = streams.store(
            "event-counts-store",
            QueryableStoreTypes.keyValueStore()
        );
    }

    @GetMapping("/counts/{key}")
    public Long getCount(@PathVariable String key) {

        try {

            Long count = store.get(key);
            return count != null ? count : 0L;

        } catch (InvalidStateStoreException e) {

            log.warn("State store not ready");
            return 0L;
        }
    }

    @GetMapping("/counts")
    public Iterable<KeyValueIterator<String, Long>> getAllCounts() {

        try {

            return store.all();

        } catch (InvalidStateStoreException e) {

            log.warn("State store not ready");
            return null;
        }
    }

    @GetMapping("/metadata")
    public Iterable<StreamsMetadata> getMetadata() {

        return streams.allMetadata();
    }

    @GetMapping("/metadata/{key}")
    public StreamsMetadata getMetadataForKey(@PathVariable String key) {

        return streams.metadataForKey("event-counts-store", key);
    }

}
```

---

## Interactive Query Architecture

```text
    ┌─────────────┐
    │   Client    │
    └─────────────┘
         │
         │ HTTP Request
         ▼
    ┌─────────────┐
    │   REST API  │
    └─────────────┘
         │
         │ Query
         ▼
    ┌─────────────┐
    │ State Store │
    │  (Local)    │
    └─────────────┘
         │
         │ If not local
         ▼
    ┌─────────────┐
    │ Remote      │
    │ Instance    │
    └─────────────┘
```

---

# 5. State Stores

## State Store Types

### In-Memory State Store

```java
Materialized.as("my-store")
    .withKeySerde(Serdes.String())
    .withValueSerde(Serdes.Long())
    .withCachingEnabled()  // Enable caching
```

**Characteristics:**

- Fast access
- Lost on restart (recovered from changelog)
- Low latency

---

### Persistent State Store (RocksDB)

```java
Materialized.as("my-store")
    .withKeySerde(Serdes.String())
    .withValueSerde(Serdes.Long())
    .withCachingDisabled()  // Disable caching
```

**Characteristics:**

- Survives restarts
- Slower than in-memory
- Fault-tolerant

---

### Versioned State Store

```java
Materialized.as("my-store")
    .withKeySerde(Serdes.String())
    .withValueSerde(Serdes.Long())
    .withVersioned()  // Enable versioning
```

**Characteristics:**

- Keeps history of values
- Supports time-travel queries
- Higher storage requirements

---

## State Store Configuration

**File:** `StateStoreConfigExample.java`

```java
package edu.anant.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;

public class StateStoreConfigExample {

    public void configureStateStores(StreamsBuilder builder) {

        KStream<String, Long> stream = builder.stream("input-topic");

        // In-memory state store with caching
        KTable<String, Long> inMemoryStore = 
            stream
                .groupByKey()
                .count(
                    Materialized.as("in-memory-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long())
                        .withCachingEnabled()
                );

        // Persistent state store (RocksDB)
        KTable<String, Long> persistentStore = 
            stream
                .groupByKey()
                .count(
                    Materialized.as("persistent-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long())
                        .withCachingDisabled()
                );

        // Custom state store configuration
        KTable<String, Long> customStore = 
            stream
                .groupByKey()
                .count(
                    Materialized.as(
                        Stores.persistentKeyValueStore("custom-store")
                    )
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long())
                        .withRetention(Duration.ofDays(7))
                );
    }

}
```

---

## State Store Changelog

```text
State Store ──► Changelog Topic (Kafka)
                    │
                    │ For fault tolerance
                    ▼
              State recovery on restart
```

---

# 6. Fault Tolerance

## How Fault Tolerance Works

```text
1. State changes are written to changelog topic
2. On failure, state is recovered from changelog
3. Processing resumes from last committed offset
```

---

## Fault Tolerance Configuration

**File:** `FaultToleranceConfig.java`

```java
package edu.anant.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

public class FaultToleranceConfig {

    public KafkaStreams createFaultTolerantStreams() {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "fault-tolerant-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());

        // Enable exactly-once processing
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        // Configure commit interval
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        // Configure number of standby replicas
        props.put(StreamsConfig.NUM_STANDBY_STREAMS_CONFIG, 1);

        // Configure state store cleanup
        props.put(StreamsConfig.STATESTORE_CLEANUP_INTERVAL_MS_CONFIG, 60000);

        // Configure max task idle
        props.put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, 1000);

        StreamsBuilder builder = new StreamsBuilder();

        // Build topology
        Topology topology = buildTopology(builder);

        return new KafkaStreams(topology, props);
    }

    private Topology buildTopology(StreamsBuilder builder) {

        // Define your topology
        return builder.build();
    }

}
```

---

## State Recovery Process

```text
Step 1: Application starts
Step 2: State stores are restored from changelog topics
Step 3: Processing resumes from last committed offset
Step 4: State is kept in sync with changelog
```

---

# 7. Testing Kafka Streams

## Unit Testing

**File:** `KafkaStreamsTest.java`

```java
package edu.anant.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KafkaStreamsTest {

    private Properties props;
    private StreamsBuilder builder;
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Long> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;

    @BeforeEach
    public void setUp() {

        props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());

        builder = new StreamsBuilder();

        // Build test topology
        builder.stream("input-topic")
            .filter((key, value) -> value > 100)
            .mapValues(value -> value * 2)
            .to("output-topic");

        testDriver = new TopologyTestDriver(builder.build(), props);

        inputTopic = testDriver.createInputTopic("input-topic");
        outputTopic = testDriver.createOutputTopic("output-topic", 1);
    }

    @AfterEach
    public void tearDown() {

        if (testDriver != null) {
            testDriver.close();
        }
    }

    @Test
    public void testFilterAndTransform() {

        // Send test data
        inputTopic.pipeInput("key1", 50L);   // Should be filtered out
        inputTopic.pipeInput("key2", 150L);  // Should pass filter
        inputTopic.pipeInput("key3", 200L);  // Should pass filter

        // Verify output
        assertEquals(2, outputTopic.getQueueSize());

        var record1 = outputTopic.readKeyValue();
        assertEquals("key2", record1.key);
        assertEquals(300L, record1.value);  // 150 * 2

        var record2 = outputTopic.readKeyValue();
        assertEquals("key3", record2.key);
        assertEquals(400L, record2.value);  // 200 * 2
    }

    @Test
    public void testAggregation() {

        // Test aggregation logic
        // Similar pattern as above
    }

}
```

---

## Integration Testing

**File:** `KafkaStreamsIntegrationTest.java`

```java
package edu.anant.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Testcontainers
public class KafkaStreamsIntegrationTest {

    @Container
    public static KafkaContainer kafka = new KafkaContainer("confluentinc/cp-kafka:7.5.0");

    private KafkaStreams streams;

    @BeforeEach
    public void setUp() {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "integration-test-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        builder.stream("input-topic")
            .filter((key, value) -> value > 100)
            .to("output-topic");

        streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

    @AfterEach
    public void tearDown() {

        if (streams != null) {
            streams.close();
        }
    }

    @Test
    public void testEndToEnd() throws Exception {

        // Produce test messages
        // Consume from output topic
        // Verify results

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(10, TimeUnit.SECONDS);
    }

}
```

---

# 8. Production Deployment

## Deployment Checklist

- [ ] Exactly-once processing enabled
- [ ] State stores configured properly
- [ ] Changelog topics created
- [ ] Monitoring and metrics configured
- [ ] Logging configured
- [ ] Error handling implemented
- [ ] Health checks implemented
- [ ] Scaling strategy defined
- [ ] Backup and recovery tested
- [ ] Security configured (SSL, ACLs)

---

## Deployment Configuration

**File:** `ProductionConfig.java`

```java
package edu.anant.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class ProductionConfig {

    public KafkaStreams createProductionStreams() {

        Properties props = new Properties();

        // Basic configuration
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "production-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());

        // Exactly-once processing
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        // Performance tuning
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024); // 10 MB

        // Fault tolerance
        props.put(StreamsConfig.NUM_STANDBY_STREAMS_CONFIG, 1);
        props.put(StreamsConfig.STATESTORE_CLEANUP_INTERVAL_MS_CONFIG, 60000);

        // Security (if enabled)
        props.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("ssl.truststore.location", "/path/to/truststore.jks");
        props.put("ssl.truststore.password", "password");

        // Monitoring
        props.put(StreamsConfig.METRIC_REPORTERS_CONFIG, "io.confluent.metrics.reporter.ConfluentMetricsReporter");
        props.put("confluent.metrics.reporter.bootstrap.servers", "kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092");

        return new KafkaStreams(buildTopology(), props);
    }

    private org.apache.kafka.streams.Topology buildTopology() {

        StreamsBuilder builder = new StreamsBuilder();

        // Define production topology
        builder.stream("input-topic")
            .filter((key, value) -> value != null)
            .groupByKey()
            .count()
            .toStream()
            .to("output-topic");

        return builder.build();
    }

}
```

---

## Scaling Strategy

```text
Initial Deployment:
3 instances ──► Handle baseline load


Scale Up:
6 instances ──► Handle increased load


Scale Down:
3 instances ──► Return to baseline
```

---

# 9. Performance Tuning

## Performance Configuration

**File:** `PerformanceTuningConfig.java`

```java
package edu.anant.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class PerformanceTuningConfig {

    public KafkaStreams createOptimizedStreams() {

        Properties props = new Properties();

        // Basic configuration
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "optimized-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());

        // Threading
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 8);  // Match CPU cores

        // Caching
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 100 * 1024 * 1024);  // 100 MB

        // Commit interval
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        // Buffer sizes
        props.put(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024 * 1024);  // 1 MB
        props.put(StreamsConfig.PRODUCER_PREFIX + ProducerConfig.BATCH_SIZE_CONFIG, 131072);  // 128 KB
        props.put(StreamsConfig.PRODUCER_PREFIX + ProducerConfig.LINGER_MS_CONFIG, 20);

        // Exactly-once (if needed)
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        // Standby replicas for fast failover
        props.put(StreamsConfig.NUM_STANDBY_STREAMS_CONFIG, 2);

        return new KafkaStreams(buildTopology(), props);
    }

    private org.apache.kafka.streams.Topology buildTopology() {

        StreamsBuilder builder = new StreamsBuilder();

        // Optimized topology
        builder.stream("input-topic")
            .filter((key, value) -> value != null)
            .groupByKey()
            .count()
            .toStream()
            .to("output-topic");

        return builder.build();
    }

}
```

---

## Performance Best Practices

| Practice | Recommendation |
|---|---|
| **Threads** | Match number of CPU cores |
| **Caching** | Enable for stateful operations |
| **Batch Size** | 128 KB for high throughput |
| **Linger Time** | 20-50 ms for batching |
| **Commit Interval** | 1000 ms for balance |
| **Standby Replicas** | 1-2 for fast failover |
| **Partition Count** | Match or exceed thread count |

---

# 10. Interview Questions

## Q1. What is windowing in Kafka Streams?

Windowing groups records by time windows for stateful operations like aggregations over time periods.

---

## Q2. What are the different types of windows?

Time windows (fixed), hopping windows (overlapping), session windows (dynamic based on activity gaps).

---

## Q3. What is the difference between Stream-Stream and Stream-Table join?

- **Stream-Stream**: Joins two streams within a time window
- **Stream-Table**: Enriches stream with table data (no time window needed)

---

## Q4. What is Global KTable?

A fully replicated table available on all instances, useful for large read-only datasets.

---

## Q5. What are interactive queries?

A feature that allows querying state stores directly from outside the Streams application via REST API.

---

## Q6. How does Kafka Streams achieve fault tolerance?

Through state store changelog topics that replicate state to Kafka for recovery on failure.

---

## Q7. What is exactly-once processing?

A guarantee that each message is processed exactly once, even in case of failures, using transactions.

---

## Q8. How do you test Kafka Streams applications?

Using TopologyTestDriver for unit tests and TestContainers for integration tests.

---

## Q9. What are state stores?

Local storage (in-memory or RocksDB) used by Kafka Streams to maintain state for stateful operations.

---

## Q10. How do you scale Kafka Streams applications?

By running multiple instances with the same application ID; work is automatically partitioned across instances.

---

# 11. Chapter Checklist

- [x] Windowing operations
- [x] Joins (Stream-Stream, Stream-Table, Table-Table)
- [x] Global KTable
- [x] Interactive queries
- [x] State stores
- [x] Fault tolerance
- [x] Testing Kafka Streams
- [x] Production deployment
- [x] Performance tuning
- [x] Interview questions

---

# Next Chapter

## Chapter — Kafka Best Practices and Patterns

Topics:

- Design patterns for Kafka applications
- Error handling patterns
- Retry and recovery patterns
- Message ordering patterns
- Exactly-once patterns
- Schema evolution patterns
- Monitoring patterns
- Security patterns
- Production checklist
