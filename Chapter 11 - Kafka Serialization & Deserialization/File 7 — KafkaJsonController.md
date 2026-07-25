# File 7 — KafkaJsonController.java

```java
package edu.anant.controller;

import edu.anant.dto.Employee;
import edu.anant.service.producer.KafkaJsonProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/json")
@RequiredArgsConstructor
public class KafkaJsonController {

    private final KafkaJsonProducerService producerService;

    /**
     * Send Employee without Key
     */
    @PostMapping("/send")
    public String sendEmployee(
            @RequestBody Employee employee
    ) {

        producerService.send(employee);

        return "Employee sent successfully.";

    }

    /**
     * Send Employee with Key
     */
    @PostMapping("/send/{key}")
    public String sendEmployeeWithKey(
            @PathVariable String key,
            @RequestBody Employee employee
    ) {

        producerService.send(key, employee);

        return "Employee sent successfully with key : " + key;

    }

    /**
     * Bulk Send Employees
     */
    @PostMapping("/bulk/{count}")
    public String sendBulkEmployees(
            @PathVariable int count
    ) {

        for (int i = 1; i <= count; i++) {

            Employee employee = Employee.builder()
                    .id(i)
                    .firstName("Employee")
                    .lastName(String.valueOf(i))
                    .email("employee" + i + "@gmail.com")
                    .department("Engineering")
                    .designation("Software Engineer")
                    .salary(50000.0 + (i * 1000))
                    .build();

            producerService.send(employee);

        }

        return count + " Employees sent successfully.";

    }

}
```

---

# REST APIs

## 1️⃣ Send Employee (Without Key)

### Endpoint

```http
POST /api/json/send
```

### Request Body

```json
{
  "id":101,
  "firstName":"Anant",
  "lastName":"Singh",
  "email":"anant@gmail.com",
  "department":"Engineering",
  "designation":"Software Engineer",
  "salary":85000
}
```

---

## Flow

```
Postman

      │

      ▼

Controller

      │

      ▼

Producer Service

      │

      ▼

KafkaTemplate

      │

      ▼

JsonSerializer

      │

      ▼

Kafka
```

---

# 2️⃣ Send Employee With Key

### Endpoint

```http
POST /api/json/send/emp-101
```

### Request Body

```json
{
  "id":101,
  "firstName":"Anant",
  "lastName":"Singh",
  "email":"anant@gmail.com",
  "department":"Engineering",
  "designation":"Software Engineer",
  "salary":85000
}
```

---

## Flow

```
Key

↓

Hash

↓

Partition

↓

Employee
```

Using the same key ensures the message always goes to the same partition.

---

# 3️⃣ Bulk Send Employees

### Endpoint

```http
POST /api/json/bulk/10
```

Automatically sends:

```
Employee-1

Employee-2

Employee-3

...

Employee-10
```

Each Employee has a different:

- ID
- Email
- Salary

---

# Generated Employee

For `i = 5`

```java
Employee.builder()
        .id(5)
        .firstName("Employee")
        .lastName("5")
        .email("employee5@gmail.com")
        .department("Engineering")
        .designation("Software Engineer")
        .salary(55000.0)
        .build();
```

---

# Producer Output

```
EMPLOYEE SENT SUCCESSFULLY

Topic      : employee-topic

Partition  : 2

Offset     : 18

Key        : null

Employee   :

Employee(
 id=5,
 firstName=Employee,
 lastName=5,
 ...
)
```

---

# Consumer Output

```
EMPLOYEE RECEIVED SUCCESSFULLY

Topic      : employee-topic

Partition  : 2

Offset     : 18

Employee Id      : 5

First Name       : Employee

Last Name        : 5

Department       : Engineering

Salary           : 55000.0
```

---

# Complete Request Flow

```
Postman

      │

      ▼

Controller

      │

      ▼

Producer Service

      │

      ▼

KafkaTemplate

      │

      ▼

JsonSerializer

      │

      ▼

Kafka Topic

      │

      ▼

JsonDeserializer

      │

      ▼

Consumer

      │

      ▼

Console Output
```

---

# API Summary

| API | Description |
|------|-------------|
| `POST /api/json/send` | Send one Employee |
| `POST /api/json/send/{key}` | Send Employee with Key |
| `POST /api/json/bulk/{count}` | Send multiple Employees |

---

# Project Structure

```
src
│
├── config
│      ├── KafkaJsonTopicConfig.java
│      ├── KafkaJsonProducerConfig.java
│      └── KafkaJsonConsumerConfig.java
│
├── controller
│      └── KafkaJsonController.java
│
├── dto
│      └── Employee.java
│
└── service
       ├── KafkaJsonProducerService.java
       └── KafkaJsonConsumerService.java
```

---

# Next File

➡️ **application.properties**

In the next file, we'll configure:

- Kafka Bootstrap Server
- Producer JSON Serializer
- Consumer JSON Deserializer
- Consumer Group
- Trusted Packages
- Auto Offset Reset
- Complete Spring Boot Kafka JSON configuration.