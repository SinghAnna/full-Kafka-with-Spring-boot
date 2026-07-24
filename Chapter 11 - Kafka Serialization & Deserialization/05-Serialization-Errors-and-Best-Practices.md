# Serialization Errors & Production Best Practices

Serialization and Deserialization are simple concepts, but incorrect configuration can cause applications to fail at runtime.

Most Kafka serialization problems are configuration issues rather than coding issues.

Understanding these common errors will save hours of debugging.

---

# SerializationException

This is the most common Kafka exception.

It occurs when the Producer cannot convert an object into bytes.

Example

```java
kafkaTemplate.send(
        "employee-topic",
        employee
);
```

Producer Configuration

```properties
spring.kafka.producer.value-serializer=StringSerializer
```

Output

```
SerializationException

Can't convert Employee into String
```

Reason

```
Employee Object

↓

StringSerializer

❌

Cannot Serialize
```

Correct

```properties
spring.kafka.producer.value-serializer=JsonSerializer
```

---

# DeserializationException

Occurs when the Consumer cannot convert received bytes back into the expected Java object.

Producer

```
Employee JSON
```

Consumer expects

```
Order
```

Result

```
DeserializationException
```

Reason

```
Producer

Employee

↓

JSON

↓

Kafka

↓

Consumer

↓

Order

❌
```

Producer and Consumer must use compatible DTOs.

---

# ClassNotFoundException

Occurs when the Consumer receives type information but cannot find the corresponding Java class.

Producer

```
Employee
```

Consumer

```
Employee class missing
```

Output

```
ClassNotFoundException
```

---

# Trusted Package Error

Spring Kafka blocks unknown classes for security.

Example

```
The class 'edu.company.Employee'

is not in the trusted packages.
```

Wrong

```properties
spring.kafka.consumer.properties.spring.json.trusted.packages=com.demo
```

Producer sends

```
edu.anant.dto.Employee
```

Correct

```properties
spring.kafka.consumer.properties.spring.json.trusted.packages=edu.anant.dto
```

or

```properties
spring.kafka.consumer.properties.spring.json.trusted.packages=*
```

For learning only.

---

# InvalidDefinitionException

Jackson cannot serialize some Java classes.

Example

```java
class Employee{

    private Object databaseConnection;

}
```

Output

```
InvalidDefinitionException
```

Reason

Jackson cannot convert unsupported objects into JSON.

---

# JsonParseException

Occurs when JSON is malformed.

Example

Wrong JSON

```json
{
"id":101,
"name":"Anant",
}
```

Notice the extra comma.

Result

```
JsonParseException
```

---

# MismatchedInputException

Occurs when JSON structure does not match the Java class.

JSON

```json
{
"id":"ABC"
}
```

Java

```java
private Integer id;
```

Output

```
MismatchedInputException
```

---

# Version Compatibility Problems

Producer DTO

```java
Employee

id

name

salary
```

Consumer DTO

```java
Employee

id

name
```

Missing fields may cause compatibility issues depending on the serializer configuration.

---

# Producer and Consumer Must Match

Correct

```
Producer

↓

Employee

↓

JsonSerializer

↓

Kafka

↓

JsonDeserializer

↓

Employee

↓

Consumer
```

Wrong

```
Producer

↓

Employee

↓

JsonSerializer

↓

Kafka

↓

StringDeserializer

↓

❌
```

---

# How to Debug Serialization Problems

Step 1

Verify Producer Serializer

```properties
spring.kafka.producer.value-serializer
```

---

Step 2

Verify Consumer Deserializer

```properties
spring.kafka.consumer.value-deserializer
```

---

Step 3

Verify DTO

Producer DTO

↓

Consumer DTO

Should be compatible.

---

Step 4

Verify Trusted Packages

```properties
spring.kafka.consumer.properties.spring.json.trusted.packages
```

---

Step 5

Check Kafka Logs

Look for

```
SerializationException

DeserializationException

JsonParseException

InvalidDefinitionException
```

---

# Production Best Practices

## Use DTOs

Avoid sending JPA Entities.

Instead

```
Entity

↓

DTO

↓

Kafka
```

---

## Keep Messages Small

Avoid sending

- Images
- Videos
- PDFs

Instead

```
Store File

↓

Database / S3

↓

Send File ID

↓

Kafka
```

---

## Version Messages

Good

```json
{
"version":"v1",
"id":101
}
```

Future versions become easier to maintain.

---

## Validate Before Sending

Always validate objects before publishing.

```
Validate

↓

Serialize

↓

Kafka
```

---

## Handle Exceptions

Always log serialization failures.

```java
.whenComplete((result, ex) -> {

    if(ex != null){

        log.error(ex.getMessage());

    }

});
```

---

## Avoid Huge Objects

Instead of

```
Employee

↓

500 Fields
```

Prefer

```
EmployeeSummary

↓

10 Fields
```

---

## Maintain Backward Compatibility

Adding new optional fields is usually safe.

Removing existing required fields may break Consumers.

---

# Production Workflow

```
DTO

↓

Validation

↓

JsonSerializer

↓

Kafka Producer

↓

Kafka Broker

↓

JsonDeserializer

↓

Validation

↓

Business Logic
```

---

# Common Mistakes

❌ Using StringSerializer for Java Objects

❌ Using StringDeserializer for JSON

❌ Missing trusted.packages

❌ Producer and Consumer using different DTOs

❌ Sending JPA Entities directly

❌ Ignoring serialization exceptions

❌ Sending extremely large JSON objects

❌ Breaking DTO compatibility

---

# Interview Questions

## Q1 Why does Kafka need Serialization?

Kafka stores only bytes, not Java objects.

---

## Q2 What is JsonSerializer?

It converts Java Objects into JSON bytes.

---

## Q3 What is JsonDeserializer?

It converts JSON bytes back into Java Objects.

---

## Q4 Why is trusted.packages required?

To prevent deserialization of untrusted classes and improve security.

---

## Q5 Which serializer is used most in Spring Boot?

JsonSerializer.

---

## Q6 Can Kafka store Java Objects directly?

No.

Kafka stores only bytes.

---

# Summary

| Component | Purpose |
|-----------|---------|
| Serializer | Object → Bytes |
| Deserializer | Bytes → Object |
| JsonSerializer | Object → JSON |
| JsonDeserializer | JSON → Object |
| Trusted Packages | Security |
| Type Headers | Java Class Information |
| Jackson | Object ↔ JSON |
| DTO | Recommended Message Format |

---

# Golden Rules

- Kafka stores only Bytes.
- Producer serializes data before sending.
- Consumer deserializes data after receiving.
- Producer and Consumer must use compatible serializers.
- Prefer DTOs over Entities.
- Keep JSON messages small.
- Validate data before publishing.
- Handle serialization exceptions.
- Configure trusted packages correctly.
- Design DTOs with backward compatibility.

---

# Next Section

➡️ **06-Spring-Boot-Implementation.md**

We'll build a complete Spring Boot application including:

- Employee DTO
- Topic Configuration
- Producer
- Consumer
- Controller
- JsonSerializer Configuration
- JsonDeserializer Configuration
- application.properties
- Live End-to-End Demo