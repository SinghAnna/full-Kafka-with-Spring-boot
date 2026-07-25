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