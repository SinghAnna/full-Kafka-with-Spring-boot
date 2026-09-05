# Chapter 16 — Kafka Security and Authentication

> In this chapter, we will learn how to secure Kafka clusters using SSL/TLS encryption, SASL authentication, ACLs (Access Control Lists), and implement secure producers and consumers in Spring Boot applications.

---

## Learning Objectives

After completing this chapter, you will understand:

- Why Kafka security is important
- SSL/TLS encryption for Kafka
- SASL authentication mechanisms
- PLAIN authentication
- SCRAM authentication
- OAuth authentication
- ACLs (Access Control Lists)
- Authorization and permissions
- Secure producer configuration
- Secure consumer configuration
- Production security best practices
- Troubleshooting security issues

---

# 1. Why Kafka Security Matters

Kafka clusters often handle sensitive data such as:

- Financial transactions
- Personal information
- Business-critical events
- Compliance-related logs
- Authentication events

Without proper security:

- Data can be intercepted
- Unauthorized access is possible
- Compliance requirements are not met
- Data integrity cannot be guaranteed

---

## Security Layers in Kafka

```text
┌─────────────────────────────────────────┐
│           Network Security              │
│         (Firewall, VPC, VPN)            │
└─────────────────────────────────────────┘
                    ▼
┌─────────────────────────────────────────┐
│          Encryption (SSL/TLS)           │
│     (Data in transit protection)        │
└─────────────────────────────────────────┘
                    ▼
┌─────────────────────────────────────────┐
│        Authentication (SASL)            │
│      (Who can connect to Kafka?)        │
└─────────────────────────────────────────┘
                    ▼
┌─────────────────────────────────────────┐
│       Authorization (ACLs)              │
│   (What can authenticated users do?)    │
└─────────────────────────────────────────┘
```

---

## Security Goals

| Goal | Description |
|---|---|
| **Confidentiality** | Prevent unauthorized access to data |
| **Integrity** | Ensure data is not tampered with |
| **Availability** | Ensure authorized users can access data |
| **Authentication** | Verify identity of clients |
| **Authorization** | Control what authenticated users can do |
| **Audit** | Track who did what and when |

---

# 2. SSL/TLS Encryption

SSL/TLS encrypts data in transit between clients and Kafka brokers.

## SSL/TLS Architecture

```text
        PRODUCER/CONSUMER              KAFKA BROKER
              │                              │
              │   1. SSL Handshake           │
              │◄────────────────────────────►│
              │                              │
              │   2. Establish Secure        │
              │      Encrypted Channel       │
              │◄────────────────────────────►│
              │                              │
              │   3. Send/Receive Messages   │
              │      (Encrypted)             │
              │◄────────────────────────────►│
```

---

## Generate SSL Certificates

### Step 1: Create CA (Certificate Authority)

```bash
# Create CA private key
openssl genrsa -des3 -out ca-key.pem 4096

# Create CA certificate
openssl req -new -x509 -key ca-key.pem -out ca-cert.pem \
  -days 365 -subj "/C=IN/ST=UP/L=Varanasi/O=MyOrg/CN=ca.myorg.com"
```

---

### Step 2: Generate Server Keystore

```bash
# Generate server private key and keystore
keytool -keystore server.keystore.jks -alias localhost \
  -validity 365 -genkey -keyalg RSA \
  -dname "CN=kafka.myorg.com, OU=IT, O=MyOrg, L=Varanasi, ST=UP, C=IN"
```

---

### Step 3: Create Server Certificate Signing Request (CSR)

```bash
keytool -keystore server.keystore.jks -alias localhost \
  -certreq -file server-signing-request.pem
```

---

### Step 4: Sign Server Certificate with CA

```bash
openssl x509 -req -CA ca-cert.pem -CAkey ca-key.pem \
  -in server-signing-request.pem -out server-cert.pem \
  -days 365 -CAcreateserial
```

---

### Step 5: Import CA and Server Certificate into Keystore

```bash
# Import CA certificate
keytool -keystore server.keystore.jks -alias CARoot \
  -import -file ca-cert.pem

# Import server certificate
keytool -keystore server.keystore.jks -alias localhost \
  -import -file server-cert.pem
```

---

### Step 6: Create Truststore

```bash
# Create truststore and import CA certificate
keytool -keystore server.truststore.jks -alias CARoot \
  -import -file ca-cert.pem
```

---

## Kafka Broker SSL Configuration

**File:** `server.properties`

```properties
# SSL Configuration
listeners=SSL://:9093
advertised.listeners=SSL://localhost:9093

# Keystore
ssl.keystore.location=/var/ssl/private/server.keystore.jks
ssl.keystore.password=keystore-password
ssl.key.password=key-password

# Truststore
ssl.truststore.location=/var/ssl/private/server.truststore.jks
ssl.truststore.password=truststore-password

# SSL Protocol
ssl.protocol=TLSv1.3
ssl.enabled.protocols=TLSv1.3,TLSv1.2

# Client Authentication
ssl.client.auth=required
```

---

## Docker Compose with SSL

**File:** `docker-compose-ssl.yml`

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
      - "9093:9093"
    volumes:
      - ./ssl:/var/ssl/private
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENERS: SSL://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: SSL://localhost:9093
      KAFKA_SSL_KEYSTORE_LOCATION: /var/ssl/private/server.keystore.jks
      KAFKA_SSL_KEYSTORE_PASSWORD: keystore-password
      KAFKA_SSL_KEY_PASSWORD: key-password
      KAFKA_SSL_TRUSTSTORE_LOCATION: /var/ssl/private/server.truststore.jks
      KAFKA_SSL_TRUSTSTORE_PASSWORD: truststore-password
      KAFKA_SSL_CLIENT_AUTH: required
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

---

# 3. SASL Authentication

SASL (Simple Authentication and Security Layer) provides authentication for Kafka clients.

## SASL Mechanisms

| Mechanism | Description | Security |
|---|---|---|
| **PLAIN** | Simple username/password | Medium (use with SSL) |
| **SCRAM-SHA-256** | Salted Challenge Response | High |
| **SCRAM-SHA-512** | Stronger hash algorithm | Very High |
| **GSSAPI (Kerberos)** | Enterprise authentication | Very High |
| **OAuth 2.0** | Token-based authentication | High |

---

## SCRAM-SHA-256 Setup

### Step 1: Enable SCRAM in Broker

**File:** `server.properties`

```properties
# SASL Configuration
listeners=SASL_SSL://:9093
advertised.listeners=SASL_SSL://localhost:9093

# SASL Mechanism
sasl.mechanism.inter.broker.protocol=SCRAM-SHA-256
sasl.enabled.mechanisms=SCRAM-SHA-256

# SSL Configuration (required for SASL)
ssl.keystore.location=/var/ssl/private/server.keystore.jks
ssl.keystore.password=keystore-password
ssl.key.password=key-password
ssl.truststore.location=/var/ssl/private/server.truststore.jks
ssl.truststore.password=truststore-password
ssl.client.auth=required

# Authorization
authorizer.class.name=kafka.security.authorizer.AclAuthorizer
allow.everyone.if.no.acl.found=false
```

---

### Step 2: Create SCRAM Users

```bash
# Connect to Kafka with admin credentials
kafka-configs --bootstrap-server localhost:9093 \
  --command-config client.properties \
  --alter --add-config 'SCRAM-SHA-256=[password=admin-secret]' \
  --entity-type users --entity-name admin

# Create producer user
kafka-configs --bootstrap-server localhost:9093 \
  --command-config client.properties \
  --alter --add-config 'SCRAM-SHA-256=[password=producer-secret]' \
  --entity-type users --entity-name producer-user

# Create consumer user
kafka-configs --bootstrap-server localhost:9093 \
  --command-config client.properties \
  --alter --add-config 'SCRAM-SHA-256=[password=consumer-secret]' \
  --entity-type users --entity-name consumer-user
```

---

### Step 3: Create ACLs

```bash
# Allow admin to do everything
kafka-acls --bootstrap-server localhost:9093 \
  --command-config client.properties \
  --add --allow-principal User:admin \
  --operation All --cluster

# Allow producer to write to specific topics
kafka-acls --bootstrap-server localhost:9093 \
  --command-config client.properties \
  --add --allow-principal User:producer-user \
  --operation Write --topic user-events

kafka-acls --bootstrap-server localhost:9093 \
  --command-config client.properties \
  --add --allow-principal User:producer-user \
  --operation Write --topic order-events

# Allow consumer to read from specific topics
kafka-acls --bootstrap-server localhost:9093 \
  --command-config client.properties \
  --add --allow-principal User:consumer-user \
  --operation Read --topic user-events \
  --group user-events-group

kafka-acls --bootstrap-server localhost:9093 \
  --command-config client.properties \
  --add --allow-principal User:consumer-user \
  --operation Read --topic order-events \
  --group order-events-group

# List all ACLs
kafka-acls --bootstrap-server localhost:9093 \
  --command-config client.properties \
  --list
```

---

# 4. Secure Producer Configuration

## SSL + SCRAM Producer

**File:** `SecureProducerConfig.java`

```java
package edu.anant.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class SecureProducerConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.ssl.truststore.location}")
    private String truststoreLocation;

    @Value("${kafka.ssl.truststore.password}")
    private String truststorePassword;

    @Value("${kafka.sasl.username}")
    private String saslUsername;

    @Value("${kafka.sasl.password}")
    private String saslPassword;

    @Bean
    public DefaultKafkaProducerFactory<String, String> 
    secureProducerFactory() {

        Map<String, Object> props = new HashMap<>();

        // Bootstrap servers
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Serializers
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Security Protocol
        props.put(ProducerConfig.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_SSL.name());

        // SASL Configuration
        props.put(ProducerConfig.SASL_MECHANISM_CONFIG, "SCRAM-SHA-256");
        props.put("sasl.jaas.config", 
            "org.apache.kafka.common.security.scram.ScramLoginModule required " +
            "username=\"" + saslUsername + "\" " +
            "password=\"" + saslPassword + "\";"
        );

        // SSL Configuration
        props.put("ssl.truststore.location", truststoreLocation);
        props.put("ssl.truststore.password", truststorePassword);
        props.put("ssl.endpoint.identification.algorithm", "");

        // Reliability
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> secureKafkaTemplate() {

        return new KafkaTemplate<>(secureProducerFactory());
    }

}
```

---

## Application Properties for Secure Producer

**File:** `application.properties`

```properties
# Kafka Bootstrap Servers
kafka.bootstrap-servers=localhost:9093

# SSL Configuration
kafka.ssl.truststore.location=/var/ssl/private/server.truststore.jks
kafka.ssl.truststore.password=truststore-password

# SASL Configuration
kafka.sasl.username=producer-user
kafka.sasl.password=producer-secret
```

---

## Secure Producer Service

**File:** `SecureProducerService.java`

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
public class SecureProducerService {

    private static final String TOPIC = "user-events";

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void send(String key, String message) {

        log.info("Sending secure message to {}: {}", TOPIC, message);

        kafkaTemplate.send(TOPIC, key, message)
            .whenComplete((result, exception) -> {

                if (exception != null) {
                    log.error("========================================");
                    log.error("SECURE MESSAGE SEND FAILED");
                    log.error("Topic: {}", TOPIC);
                    log.error("Key  : {}", key);
                    log.error("Error: {}", exception.getMessage());
                    log.error("========================================");
                    return;
                }

                log.info("========================================");
                log.info("SECURE MESSAGE SENT SUCCESSFULLY");
                log.info("Topic     : {}", result.getRecordMetadata().topic());
                log.info("Partition : {}", result.getRecordMetadata().partition());
                log.info("Offset    : {}", result.getRecordMetadata().offset());
                log.info("Key       : {}", key);
                log.info("========================================");
            });
    }

}
```

---

# 5. Secure Consumer Configuration

## SSL + SCRAM Consumer

**File:** `SecureConsumerConfig.java`

```java
package edu.anant.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class SecureConsumerConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.ssl.truststore.location}")
    private String truststoreLocation;

    @Value("${kafka.ssl.truststore.password}")
    private String truststorePassword;

    @Value("${kafka.sasl.username}")
    private String saslUsername;

    @Value("${kafka.sasl.password}")
    private String saslPassword;

    @Bean
    public DefaultKafkaConsumerFactory<String, String> 
    secureConsumerFactory() {

        Map<String, Object> props = new HashMap<>();

        // Bootstrap servers
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Deserializers
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Consumer Group
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "secure-consumer-group");

        // Security Protocol
        props.put(ConsumerConfig.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_SSL.name());

        // SASL Configuration
        props.put(ConsumerConfig.SASL_MECHANISM_CONFIG, "SCRAM-SHA-256");
        props.put("sasl.jaas.config", 
            "org.apache.kafka.common.security.scram.ScramLoginModule required " +
            "username=\"" + saslUsername + "\" " +
            "password=\"" + saslPassword + "\";"
        );

        // SSL Configuration
        props.put("ssl.truststore.location", truststoreLocation);
        props.put("ssl.truststore.password", truststorePassword);
        props.put("ssl.endpoint.identification.algorithm", "");

        // Offset Reset
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> 
    secureKafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(secureConsumerFactory());
        factory.setConcurrency(3);

        return factory;
    }

}
```

---

## Secure Consumer Service

**File:** `SecureConsumerService.java`

```java
package edu.anant.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class SecureConsumerService {

    @KafkaListener(
        topics = "user-events",
        groupId = "secure-consumer-group",
        containerFactory = "secureKafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, String> record) {

        log.info("========================================");
        log.info("SECURE MESSAGE RECEIVED");
        log.info("Topic      : {}", record.topic());
        log.info("Partition  : {}", record.partition());
        log.info("Offset     : {}", record.offset());
        log.info("Key        : {}", record.key());
        log.info("Value      : {}", record.value());
        log.info("Timestamp  : {}", record.timestamp());
        log.info("========================================");

        // Process secure message
        processSecureMessage(record.key(), record.value());
    }

    private void processSecureMessage(String key, String value) {

        log.info("Processing secure message: {} - {}", key, value);

        // Business logic for secure message processing
    }

}
```

---

# 6. PLAIN Authentication

PLAIN is simpler but less secure than SCRAM. Always use with SSL.

## Broker Configuration for PLAIN

**File:** `server.properties`

```properties
# Listeners
listeners=SASL_SSL://:9093
advertised.listeners=SASL_SSL://localhost:9093

# SASL Mechanism
sasl.mechanism.inter.broker.protocol=PLAIN
sasl.enabled.mechanisms=PLAIN

# SSL Configuration
ssl.keystore.location=/var/ssl/private/server.keystore.jks
ssl.keystore.password=keystore-password
ssl.key.password=key-password
ssl.truststore.location=/var/ssl/private/server.truststore.jks
ssl.truststore.password=truststore-password
ssl.client.auth=required

# JAAS Configuration for PLAIN
listener.name.sasl_ssl.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
  user_admin=admin-secret \
  user_producer=producer-secret \
  user_consumer=consumer-secret;

# Authorization
authorizer.class.name=kafka.security.authorizer.AclAuthorizer
allow.everyone.if.no.acl.found=false
```

---

## Producer Configuration for PLAIN

**File:** `PlainProducerConfig.java`

```java
package edu.anant.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class PlainProducerConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.sasl.username}")
    private String saslUsername;

    @Value("${kafka.sasl.password}")
    private String saslPassword;

    @Bean
    public DefaultKafkaProducerFactory<String, String> 
    plainProducerFactory() {

        Map<String, Object> props = new HashMap<>();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Security Protocol
        props.put(ProducerConfig.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_SSL.name());

        // SASL PLAIN
        props.put(ProducerConfig.SASL_MECHANISM_CONFIG, "PLAIN");
        props.put("sasl.jaas.config", 
            "org.apache.kafka.common.security.plain.PlainLoginModule required " +
            "username=\"" + saslUsername + "\" " +
            "password=\"" + saslPassword + ";"
        );

        // SSL Configuration
        props.put("ssl.truststore.location", "/var/ssl/private/server.truststore.jks");
        props.put("ssl.truststore.password", "truststore-password");
        props.put("ssl.endpoint.identification.algorithm", "");

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> plainKafkaTemplate() {

        return new KafkaTemplate<>(plainProducerFactory());
    }

}
```

---

# 7. OAuth 2.0 Authentication

OAuth 2.0 provides token-based authentication, ideal for cloud-native applications.

## OAuth Architecture

```text
        CLIENT              OAUTH SERVER          KAFKA BROKER
          │                      │                      │
          │  1. Request Token    │                      │
          │─────────────────────►│                      │
          │                      │                      │
          │  2. Return Token     │                      │
          │◄─────────────────────│                      │
          │                      │                      │
          │  3. Connect with     │                      │
          │     Token            │                      │
          │────────────────────────────────────────────►│
          │                      │                      │
          │  4. Validate Token   │                      │
          │◄────────────────────────────────────────────│
          │                      │                      │
          │  5. Send/Receive     │                      │
          │     Messages         │                      │
          │◄────────────────────────────────────────────►│
```

---

## OAuth Producer Configuration

**File:** `OAuthProducerConfig.java`

```java
package edu.anant.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class OAuthProducerConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${oauth.token.endpoint.uri}")
    private String tokenEndpointUri;

    @Value("${oauth.client.id}")
    private String clientId;

    @Value("${oauth.client.secret}")
    private String clientSecret;

    @Bean
    public DefaultKafkaProducerFactory<String, String> 
    oauthProducerFactory() {

        Map<String, Object> props = new HashMap<>();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Security Protocol
        props.put(ProducerConfig.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_SSL.name());

        // SASL OAuth
        props.put(ProducerConfig.SASL_MECHANISM_CONFIG, "OAUTHBEARER");
        props.put("sasl.jaas.config", 
            "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
            "oauth.client.id=\"" + clientId + "\" " +
            "oauth.client.secret=\"" + clientSecret + "\" " +
            "oauth.token.endpoint.uri=\"" + tokenEndpointUri + "\";"
        );

        // SSL Configuration
        props.put("ssl.truststore.location", "/var/ssl/private/server.truststore.jks");
        props.put("ssl.truststore.password", "truststore-password");
        props.put("ssl.endpoint.identification.algorithm", "");

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> oauthKafkaTemplate() {

        return new KafkaTemplate<>(oauthProducerFactory());
    }

}
```

---

# 8. ACL (Access Control List) Management

## ACL Operations

| Operation | Description |
|---|---|
| `Read` | Read messages from topic |
| `Write` | Write messages to topic |
| `Create` | Create topic |
| `Delete` | Delete topic |
| `Alter` | Modify topic configuration |
| `Describe` | View topic configuration |
| `ClusterAction` | Perform cluster actions |
| `DescribeConfigs` | Read configuration |
| `AlterConfigs` | Modify configuration |
| `IdempotentWrite` | Use idempotent producer |

---

## ACL Commands

### Create ACL for Topic

```bash
# Allow user to write to topic
kafka-acls --bootstrap-server localhost:9093 \
  --command-config client.properties \
  --add \
  --allow-principal User:producer-user \
  --operation Write \
  --topic user-events
```

---

### Create ACL for Consumer Group

```bash
# Allow user to read from topic with specific consumer group
kafka-acls --bootstrap-server localhost:9093 \
  --command-config client.properties \
  --add \
  --allow-principal User:consumer-user \
  --operation Read \
  --topic user-events \
  --group user-events-group
```

---

### Create ACL for Cluster

```bash
# Allow admin to perform cluster operations
kafka-acls --bootstrap-server localhost:9093 \
  --command-config client.properties \
  --add \
  --allow-principal User:admin \
  --operation ClusterAction \
  --cluster
```

---

### List ACLs

```bash
# List all ACLs
kafka-acls --bootstrap-server localhost:9093 \
  --command-config client.properties \
  --list

# List ACLs for specific topic
kafka-acls --bootstrap-server localhost:9093 \
  --command-config client.properties \
  --list \
  --topic user-events
```

---

### Delete ACL

```bash
# Delete specific ACL
kafka-acls --bootstrap-server localhost:9093 \
  --command-config client.properties \
  --remove \
  --allow-principal User:producer-user \
  --operation Write \
  --topic user-events
```

---

# 9. Production Security Best Practices

## Network Security

| Practice | Description |
|---|---|
| Use private networks | Keep Kafka in VPC/VLAN |
| Firewall rules | Restrict access to Kafka ports |
| VPN for remote access | Secure remote connections |
| Network segmentation | Separate Kafka from public networks |

---

## Encryption Best Practices

| Practice | Description |
|---|---|
| Always use SSL/TLS | Encrypt all client-broker communication |
| Use strong protocols | TLSv1.3 or TLSv1.2 |
| Rotate certificates | Regularly update SSL certificates |
| Use trusted CAs | Avoid self-signed certs in production |
| Enable client auth | Require client certificates |

---

## Authentication Best Practices

| Practice | Description |
|---|---|
| Use SCRAM over PLAIN | Stronger authentication |
| Rotate passwords | Regularly update credentials |
| Use service accounts | Separate accounts per service |
| Implement OAuth for cloud | Token-based auth for microservices |
| Monitor authentication failures | Alert on suspicious activity |

---

## Authorization Best Practices

| Practice | Description |
|---|---|
| Principle of least privilege | Grant minimum necessary permissions |
| Separate producer/consumer roles | Different accounts for different operations |
| Use ACLs for all topics | Explicitly define access |
| Regular ACL audits | Review and clean up unused ACLs |
| Document ACL policies | Maintain ACL documentation |

---

## Monitoring and Auditing

| Practice | Description |
|---|---|
| Log authentication attempts | Track successful and failed logins |
| Monitor ACL violations | Alert on unauthorized access attempts |
| Audit topic access | Track who accessed what |
| Implement alerting | Notify on security incidents |
| Regular security reviews | Periodic security assessments |

---

# 10. Troubleshooting Security Issues

## Common SSL Errors

### Error: SSL Handshake Failed

```text
org.apache.kafka.common.errors.SslAuthenticationException: 
SSL handshake failed
```

**Solutions:**

- Verify certificates are valid
- Check certificate expiration dates
- Ensure truststore contains CA certificate
- Verify SSL protocol versions match
- Check hostname matches certificate CN

---

### Error: Certificate Not Trusted

```text
javax.net.ssl.SSLHandshakeException: 
sun.security.validator.ValidatorException: 
PKIX path building failed
```

**Solutions:**

- Import CA certificate into truststore
- Verify certificate chain is complete
- Check truststore password is correct
- Ensure truststore location is correct

---

## Common SASL Errors

### Error: Authentication Failed

```text
org.apache.kafka.common.errors.SaslAuthenticationException: 
Authentication failed: Invalid username or password
```

**Solutions:**

- Verify username and password
- Check user exists in Kafka
- Verify SASL mechanism matches broker
- Check JAAS configuration syntax

---

### Error: ACL Denied

```text
org.apache.kafka.common.errors.TopicAuthorizationException: 
Not authorized to access topic
```

**Solutions:**

- Verify ACL exists for user
- Check ACL permissions match operation
- Ensure principal name is correct
- Verify `allow.everyone.if.no.acl.found=false`

---

## Debug Configuration

### Enable SSL Debug Logging

```properties
# application.properties
logging.level.org.apache.kafka.common.security.ssl=DEBUG
logging.level.org.apache.kafka.clients=DEBUG
```

---

### Test Connection with Kafka Console

```bash
# Test producer connection
kafka-console-producer --bootstrap-server localhost:9093 \
  --producer.config client.properties \
  --topic user-events

# Test consumer connection
kafka-console-consumer --bootstrap-server localhost:9093 \
  --consumer.config client.properties \
  --topic user-events \
  --group test-group
```

---

# 11. Security Configuration Summary

## Complete Secure Broker Configuration

**File:** `server.properties`

```properties
# Listeners
listeners=SASL_SSL://0.0.0.0:9093
advertised.listeners=SASL_SSL://kafka.myorg.com:9093

# SASL Configuration
sasl.mechanism.inter.broker.protocol=SCRAM-SHA-256
sasl.enabled.mechanisms=SCRAM-SHA-256

# SSL Configuration
ssl.keystore.location=/var/ssl/private/server.keystore.jks
ssl.keystore.password=keystore-password
ssl.key.password=key-password
ssl.truststore.location=/var/ssl/private/server.truststore.jks
ssl.truststore.password=truststore-password
ssl.client.auth=required
ssl.protocol=TLSv1.3
ssl.enabled.protocols=TLSv1.3,TLSv1.2

# Authorization
authorizer.class.name=kafka.security.authorizer.AclAuthorizer
allow.everyone.if.no.acl.found=false
super.users=User:admin

# Logging
log4j.logger.kafka.authorizer.logger=INFO
log4j.logger.kafka.security.authorizer.AclAuthorizer=DEBUG
```

---

## Complete Secure Client Configuration

**File:** `client.properties`

```properties
# Bootstrap Servers
bootstrap.servers=kafka.myorg.com:9093

# Security Protocol
security.protocol=SASL_SSL

# SASL Configuration
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="producer-user" \
  password="producer-secret";

# SSL Configuration
ssl.truststore.location=/var/ssl/private/server.truststore.jks
ssl.truststore.password=truststore-password
ssl.endpoint.identification.algorithm=
```

---

# 12. Interview Questions

## Q1. Why is SSL/TLS important for Kafka?

SSL/TLS encrypts data in transit, preventing eavesdropping and man-in-the-middle attacks.

## Q2. What is the difference between PLAIN and SCRAM?

- PLAIN sends credentials in base64 (must use with SSL)
- SCRAM uses challenge-response, never sends password over network

## Q3. What are ACLs in Kafka?

Access Control Lists define what authenticated users can do (read, write, create topics, etc.).

## Q4. How do you enable authentication in Kafka?

Configure `listeners` with SASL_SSL, set `sasl.enabled.mechanisms`, and create users with credentials.

## Q5. What is the purpose of `allow.everyone.if.no.acl.found`?

When `false`, users must have explicit ACLs to access resources. When `true`, access is allowed if no ACL exists.

## Q6. What SASL mechanisms does Kafka support?

PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI (Kerberos), and OAuth 2.0.

## Q7. How do you rotate SSL certificates?

Generate new certificates, update keystore/truststore, restart brokers, and update clients.

## Q8. What is the difference between authentication and authorization?

- Authentication: Who are you? (verifying identity)
- Authorization: What can you do? (permissions)

## Q9. How do you troubleshoot SSL handshake failures?

Enable debug logging, verify certificates, check truststore, and test with console tools.

## Q10. What are production security best practices for Kafka?

Use SSL/TLS, SCRAM authentication, ACLs for all topics, least privilege principle, and regular audits.

---

# 13. Chapter Checklist

- [x] Why Kafka security matters
- [x] SSL/TLS encryption setup
- [x] SASL authentication mechanisms
- [x] PLAIN authentication
- [x] SCRAM authentication
- [x] OAuth authentication
- [x] ACL management
- [x] Secure producer configuration
- [x] Secure consumer configuration
- [x] Production security best practices
- [x] Troubleshooting security issues
- [x] Security configuration summary
- [x] Interview questions

---

# Next Chapter

## Chapter 17 — Kafka Testing and Debugging

Topics:

- Unit testing producers
- Unit testing consumers
- Integration testing with embedded Kafka
- Testing with TestContainers
- Mocking Kafka for tests
- Debugging common issues
- Logging best practices
- Performance testing
- Load testing strategies