# Chapter — Kafka Performance Tuning and Optimization

> In this chapter, we will learn how to tune and optimize Kafka producers, consumers, and brokers for maximum throughput, minimum latency, and efficient resource utilization.

---

## Learning Objectives

After completing this chapter, you will understand:

- Producer performance tuning
- Consumer performance tuning
- Broker performance tuning
- Batch configuration
- Compression strategies
- Partitioning strategies
- Memory optimization
- Network optimization
- Monitoring and metrics
- Performance testing methodologies

---

# 1. Why Performance Tuning Matters

Kafka is designed for high throughput and low latency, but default configurations may not be optimal for all use cases.

## Performance Goals

| Goal | Description |
|---|---|
| **High Throughput** | Maximize messages per second |
| **Low Latency** | Minimize end-to-end delay |
| **Resource Efficiency** | Optimize CPU, memory, disk, network |
| **Scalability** | Handle growing load efficiently |
| **Reliability** | Maintain performance under failures |

---

## Performance Trade-offs

```text
Throughput vs Latency:

Higher Throughput ──► Larger batches ──► Higher Latency
Lower Latency ──► Smaller batches ──► Lower Throughput


Reliability vs Performance:

acks=all ──► Higher Reliability ──► Lower Performance
acks=1 ──► Lower Reliability ──► Higher Performance


Resource Usage vs Performance:

More Memory ──► Better Performance ──► Higher Cost
Less Memory ──► Lower Performance ──► Lower Cost
```

---

# 2. Producer Performance Tuning

## Key Producer Settings

### batch.size

Controls how many bytes of messages are batched together.

```properties
# Default: 16384 (16 KB)
# Recommended: 32768 to 131072 (32 KB to 128 KB)
spring.kafka.producer.properties.batch.size=65536
```

**Impact:**

- Larger batch size → Higher throughput, higher latency
- Smaller batch size → Lower throughput, lower latency

---

### linger.ms

How long to wait before sending a batch.

```properties
# Default: 0 (send immediately)
# Recommended: 5 to 100 ms
spring.kafka.producer.properties.linger.ms=20
```

**Impact:**

- Higher linger → More batching → Higher throughput
- Lower linger → Less batching → Lower latency

---

### buffer.memory

Total memory for buffering unsent messages.

```properties
# Default: 33554432 (32 MB)
# Recommended: 67108864 to 134217728 (64 MB to 128 MB)
spring.kafka.producer.properties.buffer.memory=67108864
```

**Impact:**

- More memory → Better handling of bursts
- Less memory → Risk of blocking on send()

---

### compression.type

Compression algorithm for messages.

```properties
# Options: none, gzip, snappy, lz4, zstd
# Recommended: lz4 or snappy (good balance)
spring.kafka.producer.properties.compression.type=lz4
```

**Impact:**

| Compression | CPU Usage | Compression Ratio | Speed |
|---|---|---|---|
| **none** | None | 1x | Fastest |
| **gzip** | High | 3-5x | Slow |
| **snappy** | Medium | 2-3x | Fast |
| **lz4** | Low-Medium | 2-3x | Very Fast |
| **zstd** | Medium | 3-5x | Fast |

---

### max.in.flight.requests.per.connection

Maximum unacknowledged requests.

```properties
# Default: 5
# For idempotence: must be <= 5
# For max throughput: 5
spring.kafka.producer.properties.max.in.flight.requests.per.connection=5
```

**Impact:**

- Higher → More parallelism → Higher throughput
- Lower → Less memory usage → Lower throughput

---

## Optimized Producer Configuration

**File:** `HighThroughputProducerConfig.java`

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
public class HighThroughputProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public DefaultKafkaProducerFactory<String, String> 
    highThroughputProducerFactory() {

        Map<String, Object> props = new HashMap<>();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Reliability
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // Batching - High Throughput
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 131072); // 128 KB
        props.put(ProducerConfig.LINGER_MS_CONFIG, 50); // Wait up to 50ms
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 134217728); // 128 MB

        // Compression
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        // Network
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> highThroughputKafkaTemplate() {

        return new KafkaTemplate<>(highThroughputProducerFactory());
    }

}
```

---

## Low Latency Producer Configuration

**File:** `LowLatencyProducerConfig.java`

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
public class LowLatencyProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public DefaultKafkaProducerFactory<String, String> 
    lowLatencyProducerFactory() {

        Map<String, Object> props = new HashMap<>();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Reliability with low latency
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // Batching - Low Latency
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384); // 16 KB (default)
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0); // Send immediately
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432); // 32 MB

        // No compression (reduce CPU)
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");

        // Network
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> lowLatencyKafkaTemplate() {

        return new KafkaTemplate<>(lowLatencyProducerFactory());
    }

}
```

---

## Producer Performance Comparison

| Configuration | Throughput | Latency | CPU | Network |
|---|---|---|---|---|
| **Default** | Medium | Medium | Low | Medium |
| **High Throughput** | High | High | Medium | Low |
| **Low Latency** | Low | Low | Low | High |
| **Compressed** | Medium | Medium | High | Very Low |

---

# 3. Consumer Performance Tuning

## Key Consumer Settings

### fetch.min.bytes

Minimum bytes to fetch in one request.

```properties
# Default: 1
# Recommended: 1024 to 1048576 (1 KB to 1 MB)
spring.kafka.consumer.properties.fetch.min.bytes=524288
```

**Impact:**

- Higher → Larger fetches → Higher throughput
- Lower → Smaller fetches → Lower latency

---

### fetch.max.wait.ms

Maximum time to wait for fetch.min.bytes.

```properties
# Default: 500 ms
# Recommended: 100 to 500 ms
spring.kafka.consumer.properties.fetch.max.wait.ms=200
```

**Impact:**

- Higher → More batching → Higher throughput
- Lower → Less waiting → Lower latency

---

### max.partition.fetch.bytes

Maximum bytes per partition per fetch.

```properties
# Default: 1048576 (1 MB)
# Recommended: 1048576 to 10485760 (1 MB to 10 MB)
spring.kafka.consumer.properties.max.partition.fetch.bytes=5242880
```

**Impact:**

- Higher → More data per fetch → Higher throughput
- Lower → Less memory usage → Lower throughput

---

### max.poll.records

Maximum records returned in one poll.

```properties
# Default: 500
# Recommended: 500 to 2000
spring.kafka.consumer.max-poll-records=1000
```

**Impact:**

- Higher → More processing per poll → Higher throughput
- Lower → Faster processing per poll → Lower latency

---

### session.timeout.ms

Timeout for consumer session.

```properties
# Default: 45000 (45 seconds)
# Recommended: 10000 to 30000 (10-30 seconds)
spring.kafka.consumer.properties.session.timeout.ms=15000
```

**Impact:**

- Higher → More tolerant of pauses → Slower failure detection
- Lower → Faster failure detection → Risk of unnecessary rebalances

---

### heartbeat.interval.ms

How often to send heartbeats.

```properties
# Default: 3000 (3 seconds)
# Recommended: 1/3 of session.timeout.ms
spring.kafka.consumer.properties.heartbeat.interval.ms=5000
```

**Rule of thumb:**

```text
heartbeat.interval.ms = session.timeout.ms / 3
```

---

## Optimized Consumer Configuration

**File:** `HighThroughputConsumerConfig.java`

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
public class HighThroughputConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public DefaultKafkaConsumerFactory<String, String> 
    highThroughputConsumerFactory() {

        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "high-throughput-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Fetching - High Throughput
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1048576); // 1 MB
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500); // 500 ms
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 10485760); // 10 MB

        // Polling
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2000);

        // Session Management
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000); // 30 seconds
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000); // 10 seconds

        // Concurrency
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000); // 5 minutes

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> 
    highThroughputKafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(highThroughputConsumerFactory());
        factory.setConcurrency(6); // Match partition count

        return factory;
    }

}
```

---

## Low Latency Consumer Configuration

**File:** `LowLatencyConsumerConfig.java`

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
public class LowLatencyConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public DefaultKafkaConsumerFactory<String, String> 
    lowLatencyConsumerFactory() {

        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "low-latency-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Fetching - Low Latency
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1); // Fetch immediately
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 100); // Wait max 100ms
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1048576); // 1 MB

        // Polling
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100); // Small batches

        // Session Management
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000); // 10 seconds
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000); // 3 seconds

        // Concurrency
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 60000); // 1 minute

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> 
    lowLatencyKafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(lowLatencyConsumerFactory());
        factory.setConcurrency(3);

        return factory;
    }

}
```

---

## Consumer Performance Comparison

| Configuration | Throughput | Latency | Memory | Rebalance Risk |
|---|---|---|---|---|
| **Default** | Medium | Medium | Medium | Low |
| **High Throughput** | High | High | High | Medium |
| **Low Latency** | Low | Low | Low | High |

---

# 4. Broker Performance Tuning

## Key Broker Settings

### num.partitions

Default number of partitions for new topics.

```properties
# Default: 1
# Recommended: 3 to 12 (based on throughput needs)
num.partitions=6
```

---

### log.segment.bytes

Size of log segment files.

```properties
# Default: 1073741824 (1 GB)
# Recommended: 536870912 to 1073741824 (512 MB to 1 GB)
log.segment.bytes=1073741824
```

**Impact:**

- Larger segments → Fewer files → Better I/O
- Smaller segments → Faster cleanup → More files

---

### log.retention.ms

How long to retain messages.

```properties
# Default: 604800000 (7 days)
# Adjust based on requirements
log.retention.ms=604800000
```

---

### log.retention.check.interval.ms

How often to check for log retention.

```properties
# Default: 300000 (5 minutes)
# Recommended: 300000 (5 minutes)
log.retention.check.interval.ms=300000
```

---

### num.io.threads

Number of I/O threads.

```properties
# Default: 8
# Recommended: Number of disks * 2
num.io.threads=8
```

---

### num.network.threads

Number of network threads.

```properties
# Default: 3
# Recommended: Number of CPUs
num.network.threads=4
```

---

### socket.send.buffer.bytes

Socket send buffer size.

```properties
# Default: 102400 (100 KB)
# Recommended: 102400 to 1048576 (100 KB to 1 MB)
socket.send.buffer.bytes=102400
```

---

### socket.receive.buffer.bytes

Socket receive buffer size.

```properties
# Default: 102400 (100 KB)
# Recommended: 102400 to 1048576 (100 KB to 1 MB)
socket.receive.buffer.bytes=102400
```

---

### replica.fetch.max.bytes

Maximum bytes for replica fetch.

```properties
# Default: 1048576 (1 MB)
# Recommended: 10485760 (10 MB) for large messages
replica.fetch.max.bytes=10485760
```

---

## Optimized Broker Configuration

**File:** `server.properties`

```properties
# Basic Configuration
broker.id=0
listeners=PLAINTEXT://:9092
log.dirs=/var/kafka-logs

# Network Threads
num.network.threads=4
num.io.threads=8

# Socket Buffer Sizes
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

# Log Configuration
log.segment.bytes=1073741824
log.retention.hours=168
log.retention.check.interval.ms=300000

# Replication
num.partitions=6
default.replication.factor=3
min.insync.replicas=2
replica.fetch.max.bytes=10485760

# Performance
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2

# Memory
message.max.bytes=10485760
replica.fetch.max.bytes=10485760
```

---

# 5. Partitioning Strategies

## Partition Count Guidelines

| Throughput | Partitions |
|---|---|
| Low (< 1K msg/s) | 3-6 |
| Medium (1K-10K msg/s) | 6-12 |
| High (10K-100K msg/s) | 12-24 |
| Very High (> 100K msg/s) | 24-48+ |

---

## Partition Key Selection

### Good Partition Keys

- User ID (for user-specific ordering)
- Order ID (for order processing)
- Device ID (for IoT data)
- Session ID (for session data)

### Bad Partition Keys

- Timestamp (causes hot partitions)
- Random values (uneven distribution)
- Boolean values (only 2 partitions used)

---

## Partition Distribution

```text
Good Distribution:
Partition 0: ████████  1000 messages
Partition 1: ████████  1000 messages
Partition 2: ████████  1000 messages
Partition 3: ████████  1000 messages


Bad Distribution (Hot Partition):
Partition 0: ████████████████████████████  4000 messages
Partition 1: ██  200 messages
Partition 2: ██  200 messages
Partition 3: ██  200 messages
```

---

## Partitioning Configuration

**File:** `PartitioningConfig.java`

```java
package edu.anant.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class PartitioningConfig {

    // High Throughput Topic
    @Bean
    public NewTopic highThroughputTopic() {

        return TopicBuilder
            .name("high-throughput-topic")
            .partitions(24)
            .replicas(3)
            .build();
    }

    // Medium Throughput Topic
    @Bean
    public NewTopic mediumThroughputTopic() {

        return TopicBuilder
            .name("medium-throughput-topic")
            .partitions(12)
            .replicas(3)
            .build();
    }

    // Low Throughput Topic
    @Bean
    public NewTopic lowThroughputTopic() {

        return TopicBuilder
            .name("low-throughput-topic")
            .partitions(6)
            .replicas(3)
            .build();
    }

}
```

---

# 6. Compression Strategies

## Compression Comparison

| Algorithm | CPU Usage | Compression Ratio | Speed | Best For |
|---|---|---|---|---|
| **none** | None | 1x | Fastest | Low latency |
| **gzip** | High | 3-5x | Slow | Archive, cold data |
| **snappy** | Medium | 2-3x | Fast | General purpose |
| **lz4** | Low-Medium | 2-3x | Very Fast | High throughput |
| **zstd** | Medium | 3-5x | Fast | Best compression/speed |

---

## Compression Configuration

**File:** `CompressionProducerConfig.java`

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
public class CompressionProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // LZ4 Compression (Recommended for most cases)
    @Bean("lz4KafkaTemplate")
    public KafkaTemplate<String, String> lz4KafkaTemplate() {

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 131072);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);

        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

    // Snappy Compression (Good alternative)
    @Bean("snappyKafkaTemplate")
    public KafkaTemplate<String, String> snappyKafkaTemplate() {

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 131072);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);

        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

    // Zstd Compression (Best compression)
    @Bean("zstdKafkaTemplate")
    public KafkaTemplate<String, String> zstdKafkaTemplate() {

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 131072);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);

        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

}
```

---

# 7. Memory Optimization

## JVM Heap Settings

### Producer JVM

```bash
# For high throughput producer
-Xms2g -Xmx2g -XX:MaxMetaspaceSize=256m

# For low latency producer
-Xms1g -Xmx1g -XX:MaxMetaspaceSize=128m
```

---

### Consumer JVM

```bash
# For high throughput consumer
-Xms4g -Xmx4g -XX:MaxMetaspaceSize=512m

# For low latency consumer
-Xms2g -Xmx2g -XX:MaxMetaspaceSize=256m
```

---

### Broker JVM

```bash
# For production broker (8+ cores, 32+ GB RAM)
-Xms6g -Xmx6g -XX:MetaspaceSize=96m -XX:G1HeapRegionSize=4M

# For development broker
-Xms1g -Xmx1g -XX:MetaspaceSize=96m
```

---

## Garbage Collection Tuning

### G1GC (Recommended)

```bash
-XX:+UseG1GC
-XX:MaxGCPauseMillis=20
-XX:InitiatingHeapOccupancyPercent=35
-XX:+ParallelRefProcEnabled
```

---

### ZGC (Low Latency)

```bash
-XX:+UseZGC
-XX:ZCollectionInterval=5
-XX:ZAllocationSpikeTolerance=5
```

---

# 8. Network Optimization

## Network Configuration

### Producer Network Settings

```properties
# Send buffer
spring.kafka.producer.properties.send.buffer.bytes=131072

# Receive buffer
spring.kafka.producer.properties.receive.buffer.bytes=131072

# Request timeout
spring.kafka.producer.properties.request.timeout.ms=30000

# Max block time
spring.kafka.producer.properties.max.block.ms=60000
```

---

### Consumer Network Settings

```properties
# Fetch max bytes
spring.kafka.consumer.properties.fetch.max.bytes=52428800

# Fetch max wait
spring.kafka.consumer.properties.fetch.max.wait.ms=500

# Session timeout
spring.kafka.consumer.properties.session.timeout.ms=30000

# Request timeout
spring.kafka.consumer.properties.request.timeout.ms=30000
```

---

## Network Bandwidth Estimation

```text
Formula:
Bandwidth = (Message Size × Messages/Second) / Compression Ratio


Example:
Message Size = 1 KB
Messages/Second = 100,000
Compression Ratio = 2 (with lz4)

Bandwidth = (1 KB × 100,000) / 2
          = 50,000 KB/s
          = 50 MB/s
          = 400 Mbps
```

---

# 9. Monitoring and Metrics

## Key Producer Metrics

| Metric | Description | Target |
|---|---|---|
| `record-send-rate` | Messages sent per second | Monitor trend |
| `records-per-request` | Average batch size | 100-1000 |
| `request-latency-avg` | Average request latency | < 100ms |
| `request-latency-max` | Maximum request latency | < 1000ms |
| `record-error-rate` | Error rate | < 0.1% |
| `batch-size-avg` | Average batch size in bytes | Close to batch.size |
| `compression-rate-avg` | Average compression ratio | 2-5x |

---

## Key Consumer Metrics

| Metric | Description | Target |
|---|---|---|
| `records-consumed-rate` | Messages consumed per second | Monitor trend |
| `records-lag-max` | Maximum consumer lag | < 10,000 |
| `fetch-latency-avg` | Average fetch latency | < 100ms |
| `fetch-latency-max` | Maximum fetch latency | < 1000ms |
| `fetch-size-avg` | Average fetch size | Close to fetch.max.bytes |
| `records-per-request-avg` | Average records per fetch | 100-1000 |

---

## Key Broker Metrics

| Metric | Description | Target |
|---|---|---|
| `MessagesInPerSec` | Messages received per second | Monitor trend |
| `BytesInPerSec` | Bytes received per second | Monitor trend |
| `BytesOutPerSec` | Bytes sent per second | Monitor trend |
| `RequestHandlerAvgIdlePercent` | Request handler idle time | > 30% |
| `NetworkProcessorAvgIdlePercent` | Network processor idle time | > 30% |
| `UnderReplicatedPartitions` | Under-replicated partitions | 0 |
| `ActiveControllerCount` | Active controllers | 1 |

---

## Monitoring Configuration

**File:** `MonitoringConfig.java`

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
public class MonitoringConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public DefaultKafkaProducerFactory<String, String> 
    monitoredProducerFactory() {

        Map<String, Object> props = new HashMap<>();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Enable metrics
        props.put(ProducerConfig.METRIC_REPORTERS_CONFIG, 
            "org.apache.kafka.common.metrics.JmxReporter");

        // Performance settings
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> monitoredKafkaTemplate() {

        return new KafkaTemplate<>(monitoredProducerFactory());
    }

}
```

---

# 10. Performance Testing

## Load Testing Strategy

### Step 1: Baseline Measurement

```bash
# Measure current performance
# - Throughput (messages/second)
# - Latency (p50, p95, p99)
# - Error rate
# - Resource utilization
```

---

### Step 2: Incremental Tuning

```bash
# Change one parameter at a time
# - batch.size
# - linger.ms
# - compression.type
# - fetch.min.bytes
# Measure impact after each change
```

---

### Step 3: Stress Testing

```bash
# Push system to limits
# - Maximum throughput
# - Maximum concurrent connections
# - Maximum message size
# Identify breaking points
```

---

### Step 4: Stability Testing

```bash
# Run at target load for extended period
# - 24-48 hours
# Monitor for:
# - Memory leaks
# - Performance degradation
# - Error accumulation
```

---

## Performance Testing Tools

| Tool | Purpose |
|---|---|
| **kafkacat** | Command-line producer/consumer |
| **kafka-producer-perf-test** | Official Kafka performance test |
| **kafka-consumer-perf-test** | Official Kafka consumer test |
| **JMeter** | Load testing with Kafka plugins |
| **Gatling** | Performance testing framework |

---

## Kafka Producer Performance Test

```bash
# Run producer performance test
kafka-producer-perf-test \
  --topic test-topic \
  --num-records 1000000 \
  --record-size 1024 \
  --throughput 10000 \
  --bootstrap-server localhost:9092
```

---

## Kafka Consumer Performance Test

```bash
# Run consumer performance test
kafka-consumer-perf-test \
  --topic test-topic \
  --messages 1000000 \
  --bootstrap-server localhost:9092 \
  --group test-group
```

---

# 11. Production Best Practices

## Producer Best Practices

| Practice | Recommendation |
|---|---|
| **Batch Size** | 64-128 KB for throughput |
| **Linger Time** | 20-50 ms for throughput |
| **Compression** | lz4 or snappy |
| **Idempotence** | Always enable |
| **Retries** | 3+ with backoff |
| **Monitoring** | Track latency and error rates |

---

## Consumer Best Practices

| Practice | Recommendation |
|---|---|
| **Fetch Min Bytes** | 512 KB - 1 MB |
| **Fetch Max Wait** | 200-500 ms |
| **Max Poll Records** | 500-2000 |
| **Concurrency** | Match partition count |
| **Processing Time** | Keep under max.poll.interval.ms |
| **Monitoring** | Track consumer lag |

---

## Broker Best Practices

| Practice | Recommendation |
|---|---|
| **Partitions** | 6-24 per topic |
| **Replication** | 3 replicas minimum |
| **Min In-Sync Replicas** | 2 for critical topics |
| **Retention** | Based on business needs |
| **Log Segments** | 512 MB - 1 GB |
| **Monitoring** | Track all key metrics |

---

## Performance Checklist

- [ ] Producer batch size optimized
- [ ] Producer linger time configured
- [ ] Compression enabled
- [ ] Consumer fetch size optimized
- [ ] Consumer poll records configured
- [ ] Partition count appropriate
- [ ] Replication factor set
- [ ] Monitoring in place
- [ ] Alerts configured
- [ ] Performance tested

---

# 12. Interview Questions

## Q1. What is the difference between batch.size and linger.ms?

- `batch.size`: Maximum bytes to batch together
- `linger.ms`: Maximum time to wait before sending a batch

---

## Q2. Which compression algorithm would you choose for high throughput?

**lz4** - It provides good compression with very low CPU overhead.

---

## Q3. How do you reduce consumer lag?

- Increase fetch.min.bytes and fetch.max.wait.ms
- Increase max.poll.records
- Increase consumer concurrency
- Optimize message processing logic
- Add more consumer instances

---

## Q4. What is the optimal number of partitions?

Depends on throughput requirements:

- Low: 3-6 partitions
- Medium: 6-12 partitions
- High: 12-24 partitions
- Very High: 24+ partitions

---

## Q5. Why is idempotence important for producers?

It prevents duplicate messages when retries occur, ensuring at-least-once semantics without duplicates.

---

## Q6. What metrics would you monitor for Kafka performance?

- Producer: record-send-rate, request-latency-avg, record-error-rate
- Consumer: records-consumed-rate, records-lag-max, fetch-latency-avg
- Broker: MessagesInPerSec, BytesInPerSec, UnderReplicatedPartitions

---

## Q7. How do you tune Kafka for low latency?

- Small batch.size (16 KB)
- linger.ms = 0
- No compression
- acks=1
- Small fetch.min.bytes
- Low max.poll.records

---

## Q8. How do you tune Kafka for high throughput?

- Large batch.size (128 KB)
- linger.ms = 50
- lz4 compression
- acks=1
- Large fetch.min.bytes (1 MB)
- High max.poll.records (2000)

---

## Q9. What is consumer lag and why is it important?

Consumer lag is the difference between the latest message in Kafka and the message the consumer has processed. High lag indicates the consumer cannot keep up with the producer.

---

## Q10. How do you handle performance degradation in production?

- Check metrics and logs
- Identify bottlenecks (CPU, memory, network, disk)
- Review recent configuration changes
- Scale horizontally (add brokers, increase partitions)
- Optimize batch sizes and compression
- Review consumer processing logic

---

# 13. Chapter Checklist

- [x] Producer performance tuning
- [x] Consumer performance tuning
- [x] Broker performance tuning
- [x] Batch configuration
- [x] Compression strategies
- [x] Partitioning strategies
- [x] Memory optimization
- [x] Network optimization
- [x] Monitoring and metrics
- [x] Performance testing methodologies
- [x] Production best practices
- [x] Interview questions

---

# Next Chapter

## Chapter — Kafka Streams and Stateful Processing

Topics:

- What is Kafka Streams?
- KStream vs KTable
- Stateful operations
- Aggregations and joins
- Windowing
- Time concepts
- State stores
- Interactive queries
- Production considerations
- Real-world examples
