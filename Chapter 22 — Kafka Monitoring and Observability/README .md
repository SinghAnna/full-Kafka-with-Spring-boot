# Chapter — Kafka Monitoring and Observability

> In this chapter, we will learn how to monitor Kafka clusters, producers, and consumers using key metrics, monitoring tools like Prometheus and Grafana, logging best practices, distributed tracing, and production alerting strategies.

---

## Learning Objectives

After completing this chapter, you will understand:

- Key Kafka metrics to monitor
- Producer metrics and monitoring
- Consumer metrics and monitoring
- Broker metrics and monitoring
- Monitoring tools (Prometheus, Grafana)
- Logging best practices
- Distributed tracing with Kafka
- Alerting strategies
- Performance dashboards
- Production monitoring setup

---

# 1. Why Monitoring Matters

Kafka is a critical component in modern architectures. Without proper monitoring:

- Performance degradation goes unnoticed
- Failures are detected too late
- Capacity planning is guesswork
- Troubleshooting is difficult
- SLA compliance is impossible

With proper monitoring:

- Issues are detected proactively
- Performance is optimized continuously
- Capacity is planned accurately
- Troubleshooting is faster
- SLA compliance is measurable

---

## Monitoring Pyramid

```text
            ┌─────────────┐
            │  Business   │
            │   Metrics   │
            └─────────────┘
                   ▲
            ┌─────────────┐
            │ Application │
            │   Metrics   │
            └─────────────┘
                   ▲
            ┌─────────────┐
            │   Kafka     │
            │   Metrics   │
            └─────────────┘
                   ▲
            ┌─────────────┐
            │  Infrastr.  │
            │   Metrics   │
            └─────────────┘
```

---

# 2. Key Kafka Metrics

## Four Golden Signals

| Signal | Description | Kafka Example |
|---|---|---|
| **Latency** | Time to process requests | Produce/consume latency |
| **Throughput** | Messages processed per second | Messages/sec |
| **Errors** | Failed requests | Produce/consume errors |
| **Saturation** | Resource utilization | Disk, CPU, memory usage |

---

## Metric Categories

```text
Kafka Metrics
├── Producer Metrics
├── Consumer Metrics
├── Broker Metrics
├── Topic Metrics
├── Partition Metrics
└── ZooKeeper Metrics
```

---

# 3. Producer Metrics

## Key Producer Metrics

| Metric Name | Description | Target |
|---|---|---|
| `record-send-rate` | Messages sent per second | Monitor trend |
| `records-per-request` | Average batch size | 100-1000 |
| `record-error-rate` | Error rate | < 0.1% |
| `request-latency-avg` | Average request latency | < 100ms |
| `request-latency-max` | Maximum request latency | < 1000ms |
| `batch-size-avg` | Average batch size in bytes | Close to batch.size |
| `compression-rate-avg` | Average compression ratio | 2-5x |
| `bufferpool-wait-ratio` | Time waiting for buffer | < 1% |
| `record-retry-rate` | Retry rate | < 1% |

---

## Producer Metric Monitoring

**File:** `ProducerMetricsService.java`

```java
package edu.anant.monitoring;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Map;

@Slf4j
@Service
public class ProducerMetricsService {

    private final Producer<String, String> producer;

    public ProducerMetricsService() {

        // Create producer with metrics
        this.producer = createProducerWithMetrics();
    }

    private Producer<String, String> createProducerWithMetrics() {

        // Producer configuration with metrics
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Add custom metrics reporter
        props.put("metric.reporters", 
            "edu.anant.monitoring.CustomMetricsReporter");

        return new KafkaProducer<>(props);
    }

    @PostConstruct
    public void startMonitoring() {

        Metrics metrics = producer.metrics();

        // Log metrics periodically
        new Thread(() -> {

            while (true) {

                try {

                    Thread.sleep(10000); // Every 10 seconds

                    log.info("========================================");
                    log.info("PRODUCER METRICS");

                    metrics.metrics().forEach((name, metric) -> {

                        log.info("{}: {}", name, metric.metricValue());
                    });

                    log.info("========================================");

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

        }).start();
    }

}
```

---

## Producer Metrics Dashboard

```text
Producer Dashboard
├── Throughput
│   ├── Messages sent/sec
│   ├── Bytes sent/sec
│   └── Batches sent/sec
├── Latency
│   ├── Request latency (avg)
│   ├── Request latency (max)
│   └── Request latency (p99)
├── Errors
│   ├── Error rate
│   ├── Retry rate
│   └── Failed requests
└── Batching
    ├── Batch size (avg)
    ├── Compression ratio
    └── Buffer wait time
```

---

# 4. Consumer Metrics

## Key Consumer Metrics

| Metric Name | Description | Target |
|---|---|---|
| `records-consumed-rate` | Messages consumed per second | Monitor trend |
| `records-lag-max` | Maximum consumer lag | < 10,000 |
| `records-lag-avg` | Average consumer lag | < 5,000 |
| `fetch-latency-avg` | Average fetch latency | < 100ms |
| `fetch-latency-max` | Maximum fetch latency | < 1000ms |
| `fetch-size-avg` | Average fetch size | Close to fetch.max.bytes |
| `records-per-request-avg` | Average records per fetch | 100-1000 |
| `commit-latency-avg` | Average commit latency | < 50ms |
| `rebalance-latency-avg` | Average rebalance latency | < 1000ms |

---

## Consumer Lag Monitoring

Consumer lag is the most critical consumer metric.

```text
Consumer Lag = Latest Offset - Committed Offset


Example:
Latest Offset    : 100,000
Committed Offset : 95,000
Consumer Lag     : 5,000 messages
```

---

## Consumer Lag Alerting

| Lag Value | Alert Level | Action |
|---|---|---|
| < 1,000 | Normal | No action |
| 1,000 - 5,000 | Warning | Monitor closely |
| 5,000 - 10,000 | High | Investigate |
| > 10,000 | Critical | Page on-call |
| > 100,000 | Emergency | Immediate action |

---

## Consumer Metrics Service

**File:** `ConsumerMetricsService.java`

```java
package edu.anant.monitoring;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.*;

@Slf4j
@Service
public class ConsumerMetricsService {

    private final Consumer<String, String> consumer;

    public ConsumerMetricsService() {

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "metrics-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
            "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
            "org.apache.kafka.common.serialization.StringDeserializer");

        this.consumer = new KafkaConsumer<>(props);
    }

    @PostConstruct
    public void startMonitoring() {

        // Subscribe to topics
        consumer.subscribe(Arrays.asList("orders-topic", "payments-topic"));

        // Monitor consumer lag
        new Thread(() -> {

            while (true) {

                try {

                    Thread.sleep(5000); // Every 5 seconds

                    // Get current positions
                    Map<TopicPartition, Long> positions = consumer.position(consumer.assignment());

                    // Get end offsets
                    Map<TopicPartition, Long> endOffsets = consumer.endOffsets(consumer.assignment());

                    // Calculate lag
                    Map<TopicPartition, Long> lag = new HashMap<>();

                    endOffsets.forEach((tp, endOffset) -> {

                        Long position = positions.get(tp);
                        Long topicLag = endOffset - (position != null ? position : 0);

                        lag.put(tp, topicLag);

                        log.info("========================================");
                        log.info("CONSUMER LAG");
                        log.info("Topic      : {}", tp.topic());
                        log.info("Partition  : {}", tp.partition());
                        log.info("End Offset : {}", endOffset);
                        log.info("Position   : {}", position);
                        log.info("Lag        : {}", topicLag);
                        log.info("========================================");

                        // Alert on high lag
                        if (topicLag > 10000) {
                            log.error("CRITICAL: High consumer lag detected!");
                            sendAlert(tp, topicLag);
                        }
                    });

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

        }).start();
    }

    private void sendAlert(TopicPartition tp, Long lag) {

        // Send alert to monitoring system
        // Example: PagerDuty, Slack, Email
        log.error("ALERT: Consumer lag {} for {}-{} exceeds threshold", 
            lag, tp.topic(), tp.partition());
    }

}
```

---

## Consumer Metrics Dashboard

```text
Consumer Dashboard
├── Throughput
│   ├── Messages consumed/sec
│   ├── Bytes consumed/sec
│   └── Fetch rate
├── Lag
│   ├── Consumer lag (max)
│   ├── Consumer lag (avg)
│   └── Lag by partition
├── Latency
│   ├── Fetch latency (avg)
│   ├── Fetch latency (max)
│   └── Commit latency
├── Errors
│   ├── Error rate
│   └── Failed fetches
└── Rebalances
    ├── Rebalance count
    └── Rebalance latency
```

---

# 5. Broker Metrics

## Key Broker Metrics

| Metric Name | Description | Target |
|---|---|---|
| `MessagesInPerSec` | Messages received per second | Monitor trend |
| `BytesInPerSec` | Bytes received per second | Monitor trend |
| `BytesOutPerSec` | Bytes sent per second | Monitor trend |
| `RequestHandlerAvgIdlePercent` | Request handler idle time | > 30% |
| `NetworkProcessorAvgIdlePercent` | Network processor idle time | > 30% |
| `UnderReplicatedPartitions` | Under-replicated partitions | 0 |
| `ActiveControllerCount` | Active controllers | 1 |
| `OfflinePartitionsCount` | Offline partitions | 0 |
| `LeaderElectionRateAndTimeMs` | Leader election rate | Monitor spikes |
| `UncleanLeaderElectionsPerSec` | Unclean leader elections | 0 |

---

## Broker Resource Metrics

| Metric | Description | Target |
|---|---|---|
| **Disk Usage** | Percentage of disk used | < 80% |
| **CPU Usage** | CPU utilization | < 70% |
| **Memory Usage** | Heap memory usage | < 80% |
| **Network I/O** | Network throughput | Monitor trend |
| **Disk I/O** | Disk read/write rate | Monitor trend |
| **GC Time** | Garbage collection time | < 5% |

---

## Broker Metrics Monitoring

**File:** `BrokerMetricsService.java`

```java
package edu.anant.monitoring;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

@Slf4j
@Service
public class BrokerMetricsService {

    @PostConstruct
    public void startMonitoring() {

        // Monitor JVM metrics
        new Thread(() -> {

            MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

            while (true) {

                try {

                    Thread.sleep(10000); // Every 10 seconds

                    MemoryUsage heapUsage = memoryMXBean.getHeapMemoryUsage();

                    long used = heapUsage.getUsed();
                    long max = heapUsage.getMax();
                    double usagePercent = (used * 100.0) / max;

                    log.info("========================================");
                    log.info("JVM MEMORY METRICS");
                    log.info("Used Memory   : {} MB", used / (1024 * 1024));
                    log.info("Max Memory    : {} MB", max / (1024 * 1024));
                    log.info("Usage Percent : {}%", usagePercent);
                    log.info("========================================");

                    // Alert on high memory usage
                    if (usagePercent > 80) {
                        log.error("CRITICAL: High memory usage detected!");
                        sendMemoryAlert(usagePercent);
                    }

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

        }).start();
    }

    private void sendMemoryAlert(double usagePercent) {

        // Send alert to monitoring system
        log.error("ALERT: Memory usage at {}%", usagePercent);
    }

}
```

---

## Broker Metrics Dashboard

```text
Broker Dashboard
├── Throughput
│   ├── Messages in/sec
│   ├── Messages out/sec
│   ├── Bytes in/sec
│   └── Bytes out/sec
├── Performance
│   ├── Request handler idle %
│   ├── Network processor idle %
│   └── Request latency
├── Replication
│   ├── Under-replicated partitions
│   ├── Offline partitions
│   └── Leader election rate
├── Resources
│   ├── CPU usage
│   ├── Memory usage
│   ├── Disk usage
│   └── Network I/O
└── Health
    ├── Active controller count
    ├── Broker status
    └── Cluster health
```

---

# 6. Monitoring Tools

## Prometheus + Grafana Stack

### Architecture

```text
    ┌─────────────┐
    │   Kafka     │
    │   Brokers   │
    └─────────────┘
         │
         │ JMX Metrics
         ▼
    ┌─────────────┐
    │   JMX       │
    │   Exporter  │
    └─────────────┘
         │
         │ HTTP (Prometheus format)
         ▼
    ┌─────────────┐
    │ Prometheus  │
    │   Server    │
    └─────────────┘
         │
         │ Query
         ▼
    ┌─────────────┐
    │   Grafana   │
    │ Dashboards  │
    └─────────────┘
```

---

### JMX Exporter Configuration

**File:** `jmx_prometheus_exporter.yaml`

```yaml
lowercaseOutputName: true

rules:
  - pattern: kafka.server<type=(.+), name=(.+), clientId=(.+), topic=(.+), partition=(.*)><>Value
    name: kafka_server_$1_$2
    type: GAUGE
    labels:
      clientId: "$3"
      topic: "$4"
      partition: "$5"

  - pattern: kafka.server<type=(.+), name=(.+), clientId=(.+), brokerHost=(.+), brokerPort=(.+)><>Value
    name: kafka_server_$1_$2
    type: GAUGE
    labels:
      clientId: "$3"
      broker: "$4:$5"

  - pattern: kafka.server<type=(.+), name=(.+), clientId=(.+), broker=(.+)><>Value
    name: kafka_server_$1_$2
    type: GAUGE
    labels:
      clientId: "$3"
      broker: "$4"

  - pattern: kafka.server<type=(.+), name=(.+), clientId=(.+)><>Value
    name: kafka_server_$1_$2
    type: GAUGE
    labels:
      clientId: "$3"

  - pattern: kafka.server<type=(.+), name=(.+)><>Value
    name: kafka_server_$1_$2
    type: GAUGE

  - pattern: kafka.producer<type=(.+), name=(.+), clientId=(.+), topic=(.+), partition=(.*)><>Value
    name: kafka_producer_$1_$2
    type: GAUGE
    labels:
      clientId: "$3"
      topic: "$4"
      partition: "$5"

  - pattern: kafka.consumer<type=(.+), name=(.+), clientId=(.+), topic=(.+), partition=(.*)><>Value
    name: kafka_consumer_$1_$2
    type: GAUGE
    labels:
      clientId: "$3"
      topic: "$4"
      partition: "$5"
```

---

### Prometheus Configuration

**File:** `prometheus.yml`

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'kafka-brokers'
    static_configs:
      - targets:
          - 'kafka-broker-1:9092'
          - 'kafka-broker-2:9092'
          - 'kafka-broker-3:9092'
    metrics_path: '/metrics'

  - job_name: 'jmx-exporter'
    static_configs:
      - targets:
          - 'jmx-exporter:5556'

  - job_name: 'kafka-consumer'
    static_configs:
      - targets:
          - 'consumer-app:8080'
    metrics_path: '/actuator/prometheus'

  - job_name: 'kafka-producer'
    static_configs:
      - targets:
          - 'producer-app:8080'
    metrics_path: '/actuator/prometheus'
```

---

### Grafana Dashboard JSON

**File:** `kafka-dashboard.json`

```json
{
  "dashboard": {
    "title": "Kafka Monitoring Dashboard",
    "panels": [
      {
        "title": "Messages In Per Second",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(kafka_server_BrokerTopicMetrics_MessagesInPerSec_Count[1m])",
            "legendFormat": "{{topic}}"
          }
        ]
      },
      {
        "title": "Consumer Lag",
        "type": "graph",
        "targets": [
          {
            "expr": "kafka_consumer_consumer_lag",
            "legendFormat": "{{topic}}-{{partition}}"
          }
        ]
      },
      {
        "title": "Under Replicated Partitions",
        "type": "singlestat",
        "targets": [
          {
            "expr": "kafka_server_ReplicaManager_UnderReplicatedPartitions",
            "legendFormat": "Under-replicated"
          }
        ]
      },
      {
        "title": "Request Handler Idle %",
        "type": "graph",
        "targets": [
          {
            "expr": "kafka_server_RequestHandlerAvgIdlePercent",
            "legendFormat": "Idle %"
          }
        ]
      }
    ]
  }
}
```

---

## Alternative Monitoring Tools

| Tool | Type | Best For |
|---|---|---|
| **Confluent Control Center** | Commercial | Enterprise monitoring |
| **Kafka Manager (CMAK)** | Open-source | Cluster management |
| **Kafdrop** | Open-source | Simple web UI |
| **Lens (Kafka Lens)** | Open-source | Developer tooling |
| **Burrow** | Open-source | Consumer lag monitoring |
| **Datadog** | SaaS | Cloud monitoring |
| **New Relic** | SaaS | Application performance |
| **Dynatrace** | SaaS | Full-stack monitoring |

---

# 7. Logging Best Practices

## Structured Logging

**File:** `StructuredLoggingConfig.java`

```java
package edu.anant.logging;

import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.encoder.LayoutEncoder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class StructuredLoggingConfig {

    @Bean
    public ConsoleAppender<ILoggingEvent> jsonConsoleAppender() {

        ConsoleAppender<ILoggingEvent> appender = new ConsoleAppender<>();
        appender.setName("JSON_CONSOLE");

        LayoutEncoder<ILoggingEvent> encoder = new PatternLayoutEncoder() {
            {
                setPattern(
                    "{\"timestamp\":\"%d{yyyy-MM-dd HH:mm:ss.SSS}\", " +
                    "\"level\":\"%level\", " +
                    "\"thread\":\"%thread\", " +
                    "\"logger\":\"%logger{36}\", " +
                    "\"message\":\"%msg\", " +
                    "\"context\":\"%X{context}\"}%n"
                );
            }
        };

        appender.setEncoder(encoder);
        return appender;
    }

}
```

---

## Log Levels for Kafka

| Level | Use Case | Example |
|---|---|---|
| **ERROR** | Failures requiring action | Message processing failed |
| **WARN** | Potential issues | High consumer lag detected |
| **INFO** | Important events | Message sent successfully |
| **DEBUG** | Detailed debugging | Batch size: 1024 bytes |
| **TRACE** | Very detailed tracing | Network request sent |

---

## Logging Configuration

**File:** `logback-spring.xml`

```xml
<configuration>

    <appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>

    <appender name="FILE" class="ch.qos.logback.core.rolling.RollingFileAppender">
        <file>logs/kafka-application.log</file>
        <rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
            <fileNamePattern>logs/kafka-application-%d{yyyy-MM-dd}.log</fileNamePattern>
            <maxHistory>30</maxHistory>
        </rollingPolicy>
        <encoder>
            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>

    <appender name="ERROR_FILE" class="ch.qos.logback.core.rolling.RollingFileAppender">
        <file>logs/kafka-errors.log</file>
        <rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
            <fileNamePattern>logs/kafka-errors-%d{yyyy-MM-dd}.log</fileNamePattern>
            <maxHistory>90</maxHistory>
        </rollingPolicy>
        <encoder>
            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
        </encoder>
        <filter class="ch.qos.logback.classic.filter.ThresholdFilter">
            <level>ERROR</level>
        </filter>
    </appender>

    <root level="INFO">
        <appender-ref ref="CONSOLE" />
        <appender-ref ref="FILE" />
        <appender-ref ref="ERROR_FILE" />
    </root>

    <!-- Kafka specific logging -->
    <logger name="org.apache.kafka" level="WARN" />
    <logger name="org.springframework.kafka" level="INFO" />
    <logger name="edu.anant" level="DEBUG" />

</configuration>
```

---

## Log Aggregation

```text
Application Logs
       │
       ▼
┌─────────────┐
│   Filebeat  │
│   or        │
│   Fluentd   │
└─────────────┘
       │
       ▼
┌─────────────┐
│    ELK      │
│  (Elastic,  │
│   Logstash, │
│   Kibana)   │
└─────────────┘
       │
       ▼
┌─────────────┐
│  Kibana     │
│ Dashboards  │
└─────────────┘
```

---

# 8. Distributed Tracing

## Tracing Architecture

```text
    ┌─────────────┐
    │   Client    │
    │  (Producer) │
    └─────────────┘
         │
         │ Trace ID
         ▼
    ┌─────────────┐
    │    Kafka    │
    │   Broker    │
    └─────────────┘
         │
         │ Trace ID
         ▼
    ┌─────────────┐
    │   Client    │
    │ (Consumer)  │
    └─────────────┘
```

---

## OpenTelemetry Integration

**File:** `OpenTelemetryConfig.java`

```java
package edu.anant.tracing;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenTelemetryConfig {

    @Bean
    public OpenTelemetry openTelemetry() {

        return OpenTelemetry.noop(); // Configure with actual OTel setup
    }

    @Bean
    public Tracer tracer(OpenTelemetry openTelemetry) {

        return openTelemetry.getTracer("kafka-tracer");
    }

    public void traceProducerSend(String topic, String key, String value) {

        Tracer tracer = tracer(openTelemetry());

        Span span = tracer.spanBuilder("kafka-produce")
            .setAttribute("topic", topic)
            .setAttribute("key", key)
            .setAttribute("value.length", value.length())
            .startSpan();

        try (Scope scope = span.makeCurrent()) {

            // Send message
            kafkaTemplate.send(topic, key, value);

            span.setAttribute("success", true);

        } catch (Exception e) {

            span.setAttribute("success", false);
            span.recordException(e);
            throw e;

        } finally {

            span.end();
        }
    }

    public void traceConsumerReceive(String topic, String key, String value) {

        Tracer tracer = tracer(openTelemetry());

        Span span = tracer.spanBuilder("kafka-consume")
            .setAttribute("topic", topic)
            .setAttribute("key", key)
            .setAttribute("value.length", value.length())
            .startSpan();

        try (Scope scope = span.makeCurrent()) {

            // Process message
            processMessage(key, value);

            span.setAttribute("success", true);

        } catch (Exception e) {

            span.setAttribute("success", false);
            span.recordException(e);
            throw e;

        } finally {

            span.end();
        }
    }

}
```

---

## Trace Context Propagation

```text
Producer Side:
┌─────────────────────────────────┐
│  Span: kafka-produce            │
│  Trace ID: abc123               │
│  Span ID: span-001              │
│  Attributes:                    │
│    - topic: orders-topic        │
│    - key: order-101             │
│    - success: true              │
└─────────────────────────────────┘


Consumer Side:
┌─────────────────────────────────┐
│  Span: kafka-consume            │
│  Trace ID: abc123 (same)        │
│  Span ID: span-002              │
│  Parent Span ID: span-001       │
│  Attributes:                    │
│    - topic: orders-topic        │
│    - key: order-101             │
│    - success: true              │
└─────────────────────────────────┘
```

---

# 9. Alerting Strategies

## Alert Categories

| Category | Examples | Response Time |
|---|---|---|
| **Critical** | Broker down, data loss | Immediate (< 5 min) |
| **High** | High consumer lag, high error rate | Fast (< 15 min) |
| **Medium** | Performance degradation | Normal (< 1 hour) |
| **Low** | Warnings, trends | Next business day |

---

## Alert Configuration

**File:** `AlertingConfig.java`

```java
package edu.anant.alerting;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class AlertingConfig {

    public void checkAndAlert(ConsumerLagMetrics lagMetrics) {

        if (lagMetrics.getMaxLag() > 100000) {

            sendCriticalAlert(
                "CRITICAL: Consumer lag exceeds 100,000",
                "Topic: " + lagMetrics.getTopic(),
                "Lag: " + lagMetrics.getMaxLag()
            );

        } else if (lagMetrics.getMaxLag() > 10000) {

            sendHighAlert(
                "HIGH: Consumer lag exceeds 10,000",
                "Topic: " + lagMetrics.getTopic(),
                "Lag: " + lagMetrics.getMaxLag()
            );

        } else if (lagMetrics.getMaxLag() > 5000) {

            sendMediumAlert(
                "MEDIUM: Consumer lag exceeds 5,000",
                "Topic: " + lagMetrics.getTopic(),
                "Lag: " + lagMetrics.getMaxLag()
            );
        }
    }

    public void checkAndAlert(BrokerHealthMetrics brokerMetrics) {

        if (brokerMetrics.getUnderReplicatedPartitions() > 0) {

            sendCriticalAlert(
                "CRITICAL: Under-replicated partitions detected",
                "Count: " + brokerMetrics.getUnderReplicatedPartitions()
            );
        }

        if (brokerMetrics.getOfflinePartitions() > 0) {

            sendCriticalAlert(
                "CRITICAL: Offline partitions detected",
                "Count: " + brokerMetrics.getOfflinePartitions()
            );
        }

        if (brokerMetrics.getCpuUsage() > 90) {

            sendHighAlert(
                "HIGH: Broker CPU usage exceeds 90%",
                "Usage: " + brokerMetrics.getCpuUsage() + "%"
            );
        }

        if (brokerMetrics.getDiskUsage() > 85) {

            sendHighAlert(
                "HIGH: Broker disk usage exceeds 85%",
                "Usage: " + brokerMetrics.getDiskUsage() + "%"
            );
        }
    }

    private void sendCriticalAlert(String title, String... details) {

        log.error("========================================");
        log.error("CRITICAL ALERT");
        log.error("Title: {}", title);

        for (String detail : details) {
            log.error("Detail: {}", detail);
        }

        log.error("========================================");

        // Send to PagerDuty, Slack, Email, etc.
        // notificationService.sendCritical(title, details);
    }

    private void sendHighAlert(String title, String... details) {

        log.error("========================================");
        log.error("HIGH ALERT");
        log.error("Title: {}", title);

        for (String detail : details) {
            log.error("Detail: {}", detail);
        }

        log.error("========================================");

        // notificationService.sendHigh(title, details);
    }

    private void sendMediumAlert(String title, String... details) {

        log.warn("========================================");
        log.warn("MEDIUM ALERT");
        log.warn("Title: {}", title);

        for (String detail : details) {
            log.warn("Detail: {}", detail);
        }

        log.warn("========================================");

        // notificationService.sendMedium(title, details);
    }

}
```

---

## Alert Channels

| Channel | Use Case | Response Time |
|---|---|---|
| **PagerDuty** | Critical alerts | Immediate |
| **Slack** | High/Medium alerts | Fast |
| **Email** | Low alerts, summaries | Normal |
| **SMS** | Critical when PagerDuty unavailable | Immediate |
| **Webhook** | Custom integrations | Varies |

---

# 10. Performance Dashboards

## Producer Dashboard

```text
┌─────────────────────────────────────────────────┐
│           PRODUCER PERFORMANCE DASHBOARD        │
├─────────────────────────────────────────────────┤
│                                                 │
│  Messages Sent/sec                              │
│  ████████████████████  10,000 msg/s            │
│                                                 │
│  Request Latency (ms)                           │
│  Avg: 25ms  P95: 50ms  P99: 100ms              │
│                                                 │
│  Error Rate                                     │
│  0.05%  ████████                                │
│                                                 │
│  Batch Size (bytes)                             │
│  Avg: 65,536  Target: 65,536                   │
│                                                 │
│  Compression Ratio                              │
│  3.2x  ████████████████                        │
│                                                 │
└─────────────────────────────────────────────────┘
```

---

## Consumer Dashboard

```text
┌─────────────────────────────────────────────────┐
│           CONSUMER PERFORMANCE DASHBOARD        │
├─────────────────────────────────────────────────┤
│                                                 │
│  Messages Consumed/sec                          │
│  ████████████████████  9,500 msg/s             │
│                                                 │
│  Consumer Lag                                   │
│  Max: 2,500  Avg: 1,200                        │
│  Status: ✓ HEALTHY                              │
│                                                 │
│  Fetch Latency (ms)                             │
│  Avg: 15ms  P95: 30ms  P99: 50ms               │
│                                                 │
│  Rebalances                                     │
│  Count: 2 (last 24h)                           │
│  Avg Duration: 250ms                           │
│                                                 │
│  Error Rate                                     │
│  0.02%  ████                                   │
│                                                 │
└─────────────────────────────────────────────────┘
```

---

## Broker Dashboard

```text
┌─────────────────────────────────────────────────┐
│            BROKER HEALTH DASHBOARD              │
├─────────────────────────────────────────────────┤
│                                                 │
│  Cluster Status: ✓ HEALTHY                      │
│  Brokers: 3/3 Online                            │
│                                                 │
│  Messages In/sec                                │
│  ████████████████████  30,000 msg/s            │
│                                                 │
│  Messages Out/sec                               │
│  ████████████████████  28,500 msg/s            │
│                                                 │
│  Under-Replicated Partitions                    │
│  0  ✓                                           │
│                                                 │
│  Offline Partitions                             │
│  0  ✓                                           │
│                                                 │
│  Request Handler Idle %                         │
│  65%  ████████████████                         │
│                                                 │
│  Disk Usage                                     │
│  45%  ██████████                                │
│                                                 │
│  CPU Usage                                      │
│  35%  ████████                                  │
│                                                 │
└─────────────────────────────────────────────────┘
```

---

# 11. Production Monitoring Setup

## Monitoring Checklist

- [ ] JMX metrics exposed
- [ ] Prometheus scraping configured
- [ ] Grafana dashboards created
- [ ] Consumer lag monitoring enabled
- [ ] Broker health monitoring enabled
- [ ] Alerting rules configured
- [ ] Log aggregation setup
- [ ] Distributed tracing enabled
- [ ] Performance baselines established
- [ ] Alert thresholds defined
- [ ] Escalation procedures documented
- [ ] Monitoring documentation complete

---

## Monitoring Stack Architecture

```text
    ┌─────────────┐
    │   Kafka     │
    │   Cluster   │
    └─────────────┘
         │
         ├──► JMX Exporter ──► Prometheus ──► Grafana
         │
         ├──► Application Logs ──► Filebeat ──► ELK
         │
         ├──► OpenTelemetry ──► Jaeger/Zipkin
         │
         └──► Alert Manager ──► PagerDuty/Slack/Email
```

---

# 12. Interview Questions

## Q1. What are the most important Kafka metrics to monitor?

Consumer lag, messages in/out per second, under-replicated partitions, request latency, error rates, and broker resource utilization.

---

## Q2. How do you monitor consumer lag?

Use Kafka's built-in consumer lag metrics, tools like Burrow, or custom monitoring with Prometheus and Grafana.

---

## Q3. What is the target for under-replicated partitions?

Zero. Any under-replicated partitions indicate replication issues.

---

## Q4. How do you set up alerting for Kafka?

Use Prometheus Alertmanager with rules for consumer lag, broker health, error rates, and resource utilization.

---

## Q5. What logging level should you use in production?

INFO for normal operations, with ERROR logs captured separately. DEBUG only when troubleshooting.

---

## Q6. What is distributed tracing and why is it important for Kafka?

Distributed tracing tracks requests across services through Kafka, helping identify bottlenecks and failures in the entire flow.

---

## Q7. How do you monitor Kafka broker health?

Monitor CPU, memory, disk usage, under-replicated partitions, offline partitions, request handler idle time, and network throughput.

---

## Q8. What tools can you use for Kafka monitoring?

Prometheus + Grafana, Confluent Control Center, Kafdrop, Burrow, ELK Stack, Datadog, New Relic.

---

## Q9. How do you handle high consumer lag alerts?

Investigate consumer processing speed, check for bottlenecks, scale consumers, optimize processing logic, and check for external dependencies.

---

## Q10. What is a good consumer lag threshold for alerting?

Depends on use case, but typically: Warning at 5,000, High at 10,000, Critical at 100,000 messages.

---

# 13. Chapter Checklist

- [x] Key Kafka metrics to monitor
- [x] Producer metrics and monitoring
- [x] Consumer metrics and monitoring
- [x] Broker metrics and monitoring
- [x] Monitoring tools (Prometheus, Grafana)
- [x] Logging best practices
- [x] Distributed tracing
- [x] Alerting strategies
- [x] Performance dashboards
- [x] Production monitoring setup
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
- Testing error handling and DLT
- Performance testing
- End-to-end testing
- Testing best practices
