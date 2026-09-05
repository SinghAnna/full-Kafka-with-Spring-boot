# Chapter 14 — Kafka Avro and Schema Registry

> In this chapter, we will learn why Avro is preferred over JSON in production, how to set up Schema Registry, define Avro schemas, and implement Avro-based producers and consumers in Spring Boot Kafka applications.

---

## Learning Objectives

After completing this chapter, you will understand:

- Limitations of JSON serialization
- Why Avro is better for production
- What is Schema Registry
- How to set up Confluent Schema Registry
- How to define Avro schemas
- How to generate Java classes from Avro
- How to configure Avro producer
- How to configure Avro consumer
- Schema evolution strategies
- Backward, forward, and full compatibility
- Versioning best practices
- Production considerations

---

# 1. Why Not JSON in Production?

JSON is great for learning and development, but has limitations in production:

## JSON Limitations

| Issue | Description |
|---|---|
| No Schema Validation | JSON does not enforce structure |
| Large Message Size | Field names repeated in every message |
| No Type Safety | Types are not strictly enforced |
| Schema Evolution Difficult | Adding/removing fields can break consumers |
| Performance | Slower serialization/deserialization |
| No Built-in Versioning | Hard to track schema changes |

## Example JSON Message

```json
{
  "id": 101,
  "name": "Anant",
  "email": "anant@gmail.com",
  "department": "Engineering",
  "salary": 85000.0
}
```

Problems:

- Field names (`id`, `name`, `email`) are repeated in every message
- No guarantee that `id` is always a number
- No guarantee that required fields are present
- Difficult to evolve schema safely

---

# 2. What is Avro?

**Avro** is a row-oriented remote procedure call and data serialization framework developed within Apache's Hadoop project.

## Avro Advantages

| Feature | Benefit |
|---|---|
| Schema-based | Enforces structure and types |
| Compact Binary Format | Smaller message size |
| Fast Serialization | Better performance |
| Schema Evolution | Safe addition/removal of fields |
| Built-in Versioning | Track schema changes |
| Language Support | Java, Python, Go, .NET, etc. |
| Schema Registry Integration | Centralized schema management |

## Avro Message Format

Avro stores only values in binary format:

```text
101, "Anant", "anant@gmail.com", "Engineering", 85000.0
```

Schema is stored separately in Schema Registry.

Result:

- Smaller message size (no field names repeated)
- Faster processing
- Type safety enforced

---

# 3. What is Schema Registry?

**Schema Registry** is a centralized service that stores and manages Avro schemas.

## Why Schema Registry?

| Need | Solution |
|---|---|
| Schema Storage | Central repository for all schemas |
| Version Control | Track schema changes over time |
| Compatibility Check | Ensure new schemas don't break existing consumers |
| Schema Discovery | Consumers can fetch schemas dynamically |
| Governance | Enforce schema standards across teams |

## Schema Registry Architecture

```text
        PRODUCER                SCHEMA REGISTRY              CONSUMER
           │                           │                        │
           │  1. Register Schema       │                        │
           │──────────────────────────►│                        │
           │                           │                        │
           │  2. Get Schema ID         │                        │
           │──────────────────────────►│                        │
           │                           │                        │
           │  3. Send Message + ID     │                        │
           │──────────────────────────►│                        │
           │          Kafka            │                        │
           │◄──────────────────────────│                        │
           │                           │                        │
           │                           │  4. Fetch Schema + ID  │
           │                           │◄───────────────────────│
           │                           │                        │
           │                           │  5. Deserialize Message│
           │                           │◄───────────────────────│
```

---

# 4. Setting Up Schema Registry

## Using Docker (Confluent Platform)

**File:** `docker-compose.yml`

```yaml
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  schema-registry:
    image: confluentinc/cp-schema-registry:7.5.0
    depends_on:
      - kafka
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:29092
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081

  kafka-ui:
    image: provectus/kafdrop:latest
    depends_on:
      - kafka
    ports:
      - "9000:9000"
    environment:
      KAFKA_BROKERCONNECT: kafka:29092
```

## Start Schema Registry

```bash
docker-compose up -d
```

Verify Schema Registry is running:

```bash
curl http://localhost:8081/
```

Expected response:

```json
{
  "version": "7.5.0",
  "commit": "abc123"
}
```

---

# 5. Define Avro Schema

## Employee Schema

**File:** `src/main/resources/avro/Employee.avsc`

```json
{
  "type": "record",
  "name": "Employee",
  "namespace": "edu.anant.avro",
  "fields": [
    {
      "name": "id",
      "type": "long"
    },
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "email",
      "type": "string"
    },
    {
      "name": "department",
      "type": "string"
    },
    {
      "name": "salary",
      "type": "double"
    }
  ]
}
```

## Schema Fields Explanation

| Field | Type | Description |
|---|---|---|
| `type` | `record` | This is a record type |
| `name` | `Employee` | Name of the schema |
| `namespace` | `edu.anant.avro` | Java package for generated classes |
| `fields` | Array | List of fields in the record |

## Avro Data Types

| Avro Type | Java Type |
|---|---|
| `boolean` | `boolean` |
| `int` | `int` |
| `long` | `long` |
| `float` | `float` |
| `double` | `double` |
| `string` | `String` |
| `bytes` | `ByteBuffer` |
| `null` | `null` |
| `array` | `List<T>` |
| `map` | `Map<String, T>` |
| `record` | Generated Java class |
| `enum` | Generated Java enum |

---

# 6. Generate Java Classes from Avro

## Maven Configuration

**File:** `pom.xml`

```xml
<dependencies>
    <!-- Avro -->
    <dependency>
        <groupId>org.apache.avro</groupId>
        <artifactId>avro</artifactId>
        <version>1.11.3</version>
    </dependency>

    <!-- Kafka Avro Serializer -->
    <dependency>
        <groupId>io.confluent</groupId>
        <artifactId>kafka-avro-serializer</artifactId>
        <version>7.5.0</version>
    </dependency>

    <!-- Spring Kafka -->
    <dependency>
        <groupId>org.springframework.kafka</groupId>
        <artifactId>spring-kafka</artifactId>
    </dependency>
</dependencies>

<build>
    <plugins>
        <!-- Avro Maven Plugin -->
        <plugin>
            <groupId>org.apache.avro</groupId>
            <artifactId>avro-maven-plugin</artifactId>
            <version>1.11.3</version>
            <executions>
                <execution>
                    <phase>generate-sources</phase>
                    <goals>
                        <goal>schema</goal>
                    </goals>
                    <configuration>
                        <sourceDirectory>${project.basedir}/src/main/resources/avro</sourceDirectory>
                        <outputDirectory>${project.build.directory}/generated-sources/avro</outputDirectory>
                    </configuration>
                </execution>
            </executions>
        </plugin>
    </plugins>
</build>

<repositories>
    <!-- Confluent Repository -->
    <repository>
        <id>confluent</id>
        <url>https://packages.confluent.io/maven/</url>
    </repository>
</repositories>
```

## Generate Classes

```bash
mvn clean compile
```

This generates Java classes in:

```text
target/generated-sources/avro/edu/anant/avro/Employee.java
```

## Generated Employee Class (Simplified)

```java
package edu.anant.avro;

public class Employee extends org.apache.avro.specific.SpecificRecordBase 
    implements org.apache.avro.specific.SpecificRecord {

    private long id;
    private String name;
    private String email;
    private String department;
    private double salary;

    public Employee() {}

    public Employee(Long id, String name, String email, 
                    String department, Double salary) {
        this.id = id;
        this.name = name;
        this.email = email;
        this.department = department;
        this.salary = salary;
    }

    // Getters and setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }

    public String getDepartment() { return department; }
    public void setDepartment(String department) { this.department = department; }

    public Double getSalary() { return salary; }
    public void setSalary(Double salary) { this.salary = salary; }

    // Avro-specific methods
    public static org.apache.avro.Schema getClassSchema() {
        return SCHEMA$;
    }
}
```

---

# 7. Register Schema in Schema Registry

## Using REST API

```bash
curl -X POST http://localhost:8081/subjects/employee-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schema": "{\"type\":\"record\",\"name\":\"Employee\",\"namespace\":\"edu.anant.avro\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"email\",\"type\":\"string\"},{\"name\":\"department\",\"type\":\"string\"},{\"name\":\"salary\",\"type\":\"double\"}]}"
  }'
```

## Response

```json
{
  "id": 1,
  "version": 1
}
```

- `id`: Schema ID in Schema Registry
- `version`: Version number of this schema

---

## Using Spring Boot Application

Schemas are automatically registered when producers start sending messages.

---

# 8. Producer Configuration with Avro

**File:** `KafkaAvroProducerConfig.java`

```java
package edu.anant.config;

import edu.anant.avro.Employee;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
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
public class KafkaAvroProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${confluent.schema.registry.url}")
    private String schemaRegistryUrl;

    @Bean
    public DefaultKafkaProducerFactory<String, Employee> 
    avroProducerFactory() {

        Map<String, Object> props = new HashMap<>();

        props.put(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            bootstrapServers
        );

        props.put(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class
        );

        props.put(
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            KafkaAvroSerializer.class
        );

        // Schema Registry URL
        props.put(
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
            schemaRegistryUrl
        );

        // Auto-register schemas
        props.put(
            AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS,
            true
        );

        // Use latest schema version
        props.put(
            AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION,
            true
        );

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, Employee> avroKafkaTemplate() {

        return new KafkaTemplate<>(avroProducerFactory());
    }

}
```

---

## Application Properties

**File:** `application.properties`

```properties
# Kafka
spring.kafka.bootstrap-servers=localhost:9092

# Schema Registry
confluent.schema.registry.url=http://localhost:8081

# Producer
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=io.confluent.kafka.serializers.KafkaAvroSerializer
```

---

# 9. Producer Service with Avro

**File:** `AvroProducerService.java`

```java
package edu.anant.service;

import edu.anant.avro.Employee;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class AvroProducerService {

    private static final String TOPIC = "employee-avro-topic";

    private final KafkaTemplate<String, Employee> kafkaTemplate;

    public void send(Employee employee) {

        kafkaTemplate
            .send(TOPIC, employee)
            .whenComplete(this::handleResult);
    }

    public void send(String key, Employee employee) {

        kafkaTemplate
            .send(TOPIC, key, employee)
            .whenComplete(this::handleResult);
    }

    private void handleResult(
            SendResult<String, Employee> result,
            Throwable exception
    ) {

        if (exception != null) {
            log.error("========================================");
            log.error("Avro Message Send Failed");
            log.error("Error: {}", exception.getMessage());
            log.error("========================================");
            return;
        }

        log.info("========================================");
        log.info("AVRO EMPLOYEE SENT SUCCESSFULLY");
        log.info("Topic     : {}", result.getRecordMetadata().topic());
        log.info("Partition : {}", result.getRecordMetadata().partition());
        log.info("Offset    : {}", result.getRecordMetadata().offset());
        log.info("Key       : {}", result.getProducerRecord().key());
        log.info("Employee ID: {}", result.getProducerRecord().value().getId());
        log.info("Employee Name: {}", result.getProducerRecord().value().getName());
        log.info("========================================");
    }

}
```

---

# 10. Consumer Configuration with Avro

**File:** `KafkaAvroConsumerConfig.java`

```java
package edu.anant.config;

import edu.anant.avro.Employee;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
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
public class KafkaAvroConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${confluent.schema.registry.url}")
    private String schemaRegistryUrl;

    @Bean
    public DefaultKafkaConsumerFactory<String, Employee> 
    avroConsumerFactory() {

        Map<String, Object> props = new HashMap<>();

        props.put(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            bootstrapServers
        );

        props.put(
            ConsumerConfig.GROUP_ID_CONFIG,
            "employee-avro-group"
        );

        props.put(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class
        );

        props.put(
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            KafkaAvroDeserializer.class
        );

        // Schema Registry URL
        props.put(
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
            schemaRegistryUrl
        );

        // Use specific Avro reader schema
        props.put(
            AbstractKafkaSchemaSerDeConfig.SPECIFIC_AVRO_READER_CONFIG,
            true
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
            Employee
            > avroKafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<
                String,
                Employee
                > factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(avroConsumerFactory());

        return factory;
    }

}
```

---

# 11. Consumer Service with Avro

**File:** `AvroConsumerService.java`

```java
package edu.anant.service;

import edu.anant.avro.Employee;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class AvroConsumerService {

    @KafkaListener(
        topics = "employee-avro-topic",
        groupId = "employee-avro-group",
        containerFactory = "avroKafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, Employee> record) {

        Employee employee = record.value();

        log.info("========================================");
        log.info("AVRO EMPLOYEE RECEIVED");
        log.info("Topic      : {}", record.topic());
        log.info("Partition  : {}", record.partition());
        log.info("Offset     : {}", record.offset());
        log.info("Key        : {}", record.key());
        log.info("Employee ID: {}", employee.getId());
        log.info("Name       : {}", employee.getName());
        log.info("Email      : {}", employee.getEmail());
        log.info("Department : {}", employee.getDepartment());
        log.info("Salary     : {}", employee.getSalary());
        log.info("========================================");
    }

}
```

---

# 12. REST Controller for Testing

**File:** `AvroEmployeeController.java`

```java
package edu.anant.controller;

import edu.anant.avro.Employee;
import edu.anant.service.AvroProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/avro/employees")
@RequiredArgsConstructor
public class AvroEmployeeController {

    private final AvroProducerService producerService;

    @PostMapping
    public String sendEmployee(@RequestBody EmployeeDTO dto) {

        Employee employee = Employee.newBuilder()
            .setId(dto.getId())
            .setName(dto.getName())
            .setEmail(dto.getEmail())
            .setDepartment(dto.getDepartment())
            .setSalary(dto.getSalary())
            .build();

        producerService.send(employee);

        return "Employee sent successfully";
    }

    @PostMapping("/{key}")
    public String sendEmployeeWithKey(
            @PathVariable String key,
            @RequestBody EmployeeDTO dto
    ) {

        Employee employee = Employee.newBuilder()
            .setId(dto.getId())
            .setName(dto.getName())
            .setEmail(dto.getEmail())
            .setDepartment(dto.getDepartment())
            .setSalary(dto.getSalary())
            .build();

        producerService.send(key, employee);

        return "Employee sent with key: " + key;
    }

    // DTO for REST API
    public static class EmployeeDTO {
        private Long id;
        private String name;
        private String email;
        private String department;
        private Double salary;

        // Getters and setters
        public Long getId() { return id; }
        public void setId(Long id) { this.id = id; }

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public String getEmail() { return email; }
        public void setEmail(String email) { this.email = email; }

        public String getDepartment() { return department; }
        public void setDepartment(String department) { this.department = department; }

        public Double getSalary() { return salary; }
        public void setSalary(Double salary) { this.salary = salary; }
    }

}
```

---

# 13. Testing with Postman

## Send Avro Employee

**Request**

```http
POST http://localhost:8080/api/avro/employees
Content-Type: application/json
```

**Body**

```json
{
  "id": 101,
  "name": "Anant",
  "email": "anant@gmail.com",
  "department": "Engineering",
  "salary": 85000.0
}
```

## Expected Output

**Producer Log**

```text
AVRO EMPLOYEE SENT SUCCESSFULLY

Topic     : employee-avro-topic
Partition : 0
Offset    : 0
Employee ID: 101
Employee Name: Anant
```

**Consumer Log**

```text
AVRO EMPLOYEE RECEIVED

Topic      : employee-avro-topic
Partition  : 0
Offset     : 0
Employee ID: 101
Name       : Anant
Email      : anant@gmail.com
Department : Engineering
Salary     : 85000.0
```

---

# 14. Schema Evolution

Schema evolution allows you to change schemas over time without breaking existing consumers or producers.

## Types of Compatibility

| Type | Description |
|---|---|
| **Backward Compatibility** | New schema can read data written with old schema |
| **Forward Compatibility** | Old schema can read data written with new schema |
| **Full Compatibility** | Both backward and forward compatible |
| **Transitive Compatibility** | All versions are compatible with each other |

---

## Backward Compatibility (Most Common)

New schema can read old data.

### Rules for Backward Compatibility

| Change | Allowed? | Notes |
|---|---|---|
| Add field with default value | ✅ Yes | Most common |
| Add optional field | ✅ Yes | Use union with null |
| Remove field | ✅ Yes | Old readers ignore missing fields |
| Change field order | ✅ Yes | Order doesn't matter |
| Change field name | ❌ No | Breaks compatibility |
| Change field type | ⚠️ Careful | Must be type-promotable |

### Example: Add New Field

**Version 1 Schema**

```json
{
  "type": "record",
  "name": "Employee",
  "namespace": "edu.anant.avro",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "name", "type": "string"},
    {"name": "email", "type": "string"}
  ]
}
```

**Version 2 Schema (Backward Compatible)**

```json
{
  "type": "record",
  "name": "Employee",
  "namespace": "edu.anant.avro",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "name", "type": "string"},
    {"name": "email", "type": "string"},
    {
      "name": "department",
      "type": "string",
      "default": "Unknown"
    }
  ]
}
```

Key points:

- New field `department` has a `default` value
- Old consumers can still read new messages (they ignore `department`)
- New consumers can read old messages (they use default value)

---

## Forward Compatibility

Old schema can read new data.

### Rules for Forward Compatibility

| Change | Allowed? | Notes |
|---|---|---|
| Remove field with default | ✅ Yes | Old readers use default |
| Add optional field | ✅ Yes | Old readers ignore new fields |
| Add required field | ❌ No | Old readers will fail |

### Example: Remove Field

**Version 1 Schema**

```json
{
  "type": "record",
  "name": "Employee",
  "namespace": "edu.anant.avro",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "name", "type": "string"},
    {"name": "email", "type": "string"},
    {
      "name": "department",
      "type": ["null", "string"],
      "default": null
    }
  ]
}
```

**Version 2 Schema (Forward Compatible)**

```json
{
  "type": "record",
  "name": "Employee",
  "namespace": "edu.anant.avro",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "name", "type": "string"},
    {"name": "email", "type": "string"}
  ]
}
```

Key points:

- `department` field was optional (union with null)
- Removing optional field is forward compatible
- Old readers can still read new messages

---

## Full Compatibility

Both backward and forward compatible.

### Rules

- Only add optional fields with defaults
- Only remove optional fields
- Do not change field names or types

### Example: Add Optional Field

**Version 1 Schema**

```json
{
  "type": "record",
  "name": "Employee",
  "namespace": "edu.anant.avro",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "name", "type": "string"},
    {"name": "email", "type": "string"}
  ]
}
```

**Version 2 Schema (Fully Compatible)**

```json
{
  "type": "record",
  "name": "Employee",
  "namespace": "edu.anant.avro",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "name", "type": "string"},
    {"name": "email", "type": "string"},
    {
      "name": "phone",
      "type": ["null", "string"],
      "default": null
    }
  ]
}
```

---

# 15. Configure Schema Registry Compatibility

## Set Compatibility Level

```bash
# Set globally
curl -X PUT http://localhost:8081/config \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "BACKWARD"}'

# Set for specific subject
curl -X PUT http://localhost:8081/config/employee-value \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "FULL"}'
```

## Compatibility Levels

| Level | Description |
|---|---|
| `NONE` | No compatibility checking |
| `BACKWARD` | New schema can read old data (default) |
| `BACKWARD_TRANSITIVE` | All versions backward compatible |
| `FORWARD` | Old schema can read new data |
| `FORWARD_TRANSITIVE` | All versions forward compatible |
| `FULL` | Both backward and forward compatible |
| `FULL_TRANSITIVE` | All versions fully compatible |

---

# 16. Schema Evolution Best Practices

## Do's ✅

- Add new fields with default values
- Use optional fields (union with null)
- Test schema changes in development first
- Use backward compatibility by default
- Version your schemas explicitly
- Document schema changes

## Don'ts ❌

- Don't remove required fields
- Don't change field names
- Don't change field types (unless type-promotable)
- Don't deploy schema changes without testing
- Don't ignore compatibility warnings

---

## Type Promotion Rules

| From Type | To Type | Allowed? |
|---|---|---|
| `int` | `long` | ✅ Yes |
| `float` | `double` | ✅ Yes |
| `string` | `bytes` | ✅ Yes |
| `bytes` | `string` | ✅ Yes |
| `long` | `int` | ❌ No |
| `double` | `float` | ❌ No |
| `string` | `int` | ❌ No |

---

# 17. JSON vs Avro Comparison

| Feature | JSON | Avro |
|---|---|---|
| **Format** | Text-based | Binary |
| **Size** | Larger (field names repeated) | Smaller (schema separate) |
| **Performance** | Slower | Faster |
| **Schema** | No built-in schema | Strong schema enforcement |
| **Type Safety** | Weak | Strong |
| **Evolution** | Difficult | Built-in support |
| **Human Readable** | Yes | No (needs schema) |
| **Schema Registry** | Not required | Required |
| **Best For** | Development, debugging | Production, high-volume |

---

## Message Size Comparison

**JSON Message**

```json
{
  "id": 101,
  "name": "Anant",
  "email": "anant@gmail.com",
  "department": "Engineering",
  "salary": 85000.0
}
```

Size: ~100 bytes

**Avro Message**

```text
Binary: 101, "Anant", "anant@gmail.com", "Engineering", 85000.0
```

Size: ~40-50 bytes (50% smaller)

For millions of messages, this difference is significant.

---

# 18. Production Best Practices

## Schema Management

- Use backward compatibility by default
- Test schema changes in development/staging
- Document all schema changes
- Use meaningful subject names (e.g., `employee-value`)
- Monitor schema registry for errors

## Producer Best Practices

- Enable auto-register schemas in development
- Use manual schema registration in production
- Handle schema registration failures
- Log schema ID with messages for debugging

## Consumer Best Practices

- Configure specific Avro reader
- Handle schema evolution gracefully
- Use default values for new fields
- Monitor deserialization errors

## Schema Registry Best Practices

- Run Schema Registry in high availability mode
- Backup schema registry data
- Monitor schema registry health
- Set appropriate compatibility levels
- Use subject-level strategy for different topics

---

# 19. Common Errors and Solutions

## Error: Schema Not Found

**Problem**

```text
io.confluent.kafka.serializers.exception.KafkaSchemaRegistryException: 
Schema not found
```

**Solution**

- Ensure schema is registered in Schema Registry
- Check schema subject name matches
- Verify Schema Registry URL is correct

---

## Error: Incompatible Schema

**Problem**

```text
io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException: 
Schema being registered is incompatible with an earlier schema
```

**Solution**

- Review compatibility rules
- Add default values for new fields
- Use optional fields (union with null)
- Change compatibility level if needed

---

## Error: Schema Registry Unavailable

**Problem**

```text
java.net.ConnectException: 
Connection refused to Schema Registry
```

**Solution**

- Verify Schema Registry is running
- Check network connectivity
- Verify Schema Registry URL in configuration

---

# 20. Interview Questions

## Q1. Why use Avro over JSON?

Avro provides schema enforcement, smaller message size, faster serialization, and built-in schema evolution support.

## Q2. What is Schema Registry?

A centralized service that stores and manages Avro schemas, enabling schema evolution and compatibility checking.

## Q3. What is backward compatibility?

New schema can read data written with old schema. This is the most common compatibility mode.

## Q4. How do you add a new field in Avro schema?

Add the field with a default value to maintain backward compatibility.

```json
{
  "name": "department",
  "type": "string",
  "default": "Unknown"
}
```

## Q5. What happens if schema is incompatible?

Schema Registry rejects the new schema and throws an error. Messages cannot be sent until schema is compatible.

## Q6. What is the default compatibility level?

`BACKWARD` - New schemas can read old data.

## Q7. How does Avro reduce message size?

Avro stores only values in binary format. Field names are stored once in the schema, not in every message.

## Q8. What is SpecificRecord vs GenericRecord?

- `SpecificRecord`: Generated Java class from schema (type-safe)
- `GenericRecord`: Generic container (less type-safe, more flexible)

## Q9. How do you handle schema evolution in production?

- Use backward compatibility
- Add fields with default values
- Test changes in development first
- Monitor consumers for errors

## Q10. What is the role of Schema ID in messages?

Schema ID identifies which schema version was used to serialize the message. Consumers fetch the schema using this ID.

---

# 21. Chapter Checklist

- [x] JSON limitations in production
- [x] Avro advantages
- [x] Schema Registry setup
- [x] Avro schema definition
- [x] Java class generation from Avro
- [x] Producer configuration with Avro
- [x] Consumer configuration with Avro
- [x] Schema evolution strategies
- [x] Backward compatibility
- [x] Forward compatibility
- [x] Full compatibility
- [x] Compatibility levels
- [x] Schema evolution best practices
- [x] JSON vs Avro comparison
- [x] Production best practices
- [x] Common errors and solutions
- [x] Interview questions

---

# Next Chapter

## Chapter 15 — Kafka Streams and Stateful Processing

Topics:

- What is Kafka Streams?
- Stream vs Batch processing
- KStream vs KTable
- Stateful operations
- Aggregations
- Joins
- Windowing
- Time concepts (event time, processing time)
- State stores
- Interactive queries
- Production considerations
