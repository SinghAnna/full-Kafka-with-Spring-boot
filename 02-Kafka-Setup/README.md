# 📖 Understanding `docker-compose.yml`

Below is the Docker Compose configuration used in this project.

```yaml
services:
  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: kafka-tutorial

    ports:
      - "9093:9092"

    environment:
      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk

      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller

      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9093

      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093

      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT

      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1

      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"

    restart: unless-stopped
```

---

# 🔍 Configuration Explanation

## `services`

Defines all Docker containers that will run together.

---

## `kafka`

The name of the service.

You can start it using:

```bash
docker compose up -d
```

---

## `image: confluentinc/cp-kafka:7.5.0`

Specifies the Docker image.

* **confluentinc** → Publisher
* **cp-kafka** → Kafka image
* **7.5.0** → Fixed version

Using a fixed version ensures everyone gets the same Kafka environment.

---

## `container_name`

```yaml
container_name: kafka-tutorial
```

The custom name of the Docker container.

Without this, Docker generates a random name.

---

## `ports`

```yaml
9093:9092
```

Format:

```text
HOST_PORT : CONTAINER_PORT
```

* **9093** → Port on your computer
* **9092** → Kafka port inside the container

Since another Kafka instance is already using **9092** on the host machine, this project exposes Kafka on **9093**.

---

## `CLUSTER_ID`

Every Kafka KRaft cluster requires a unique Cluster ID.

This identifies the Kafka cluster internally.

---

## `KAFKA_NODE_ID`

```yaml
KAFKA_NODE_ID: 1
```

Unique ID of this Kafka node.

For a single-node setup, `1` is sufficient.

---

## `KAFKA_PROCESS_ROLES`

```yaml
broker,controller
```

This Kafka server performs two roles:

* Broker
* Controller

This is the standard configuration for a single-node KRaft setup.

---

## `KAFKA_LISTENERS`

Defines the ports on which Kafka listens.

* `PLAINTEXT://:9092` → Client connections
* `CONTROLLER://:9093` → Internal controller communication

---

## `KAFKA_ADVERTISED_LISTENERS`

```yaml
PLAINTEXT://localhost:9093
```

This is the address that clients (such as Spring Boot) use to connect to Kafka.

Your Spring Boot configuration should match this:

```properties
spring.kafka.bootstrap-servers=localhost:9093
```

---

## `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP`

Maps listener names to security protocols.

Here, both listeners use the `PLAINTEXT` protocol.

---

## `KAFKA_CONTROLLER_LISTENER_NAMES`

Specifies which listener is used by the Kafka controller.

---

## `KAFKA_CONTROLLER_QUORUM_VOTERS`

Defines the controller quorum.

For a single-node cluster:

```text
1@kafka:9093
```

* Node ID → `1`
* Host → `kafka`
* Controller Port → `9093`

---

## `KAFKA_INTER_BROKER_LISTENER_NAME`

Specifies which listener brokers use to communicate with each other.

In a single-node cluster, this is still required.

---

## `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR`

Sets the replication factor for the internal offsets topic.

Single-node Kafka can only use:

```yaml
1
```

---

## `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR`

Replication factor for transaction logs.

For one broker:

```yaml
1
```

---

## `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR`

Minimum number of in-sync replicas.

Single-node Kafka requires:

```yaml
1
```

---

## `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS`

Controls how long Kafka waits before assigning partitions to consumer groups.

`0` means consumers start immediately.

---

## `KAFKA_AUTO_CREATE_TOPICS_ENABLE`

```yaml
true
```

Kafka automatically creates a topic when a producer sends data to a non-existing topic.

This is convenient for learning but is often disabled in production environments.

---

## `restart: unless-stopped`

Automatically restarts the Kafka container after a system reboot or Docker restart, unless you explicitly stop it.

---

# 📝 Summary

This Docker Compose file creates a **single-node Kafka cluster in KRaft mode**.

It is ideal for:

* Learning Kafka
* Spring Boot development
* Local testing
* Interview preparation
