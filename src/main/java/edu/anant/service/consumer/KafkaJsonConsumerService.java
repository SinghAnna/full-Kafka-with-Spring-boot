package edu.anant.service;

import edu.anant.dto.Employee;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class KafkaJsonConsumerService {

    @KafkaListener(
            topics = "employee-topic",
            groupId = "employee-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(
            ConsumerRecord<String, Employee> record
    ) {

        Employee employee = record.value();

        log.info("========================================");
        log.info("EMPLOYEE RECEIVED SUCCESSFULLY");
        log.info("Topic      : {}", record.topic());
        log.info("Partition  : {}", record.partition());
        log.info("Offset     : {}", record.offset());
        log.info("Timestamp  : {}", record.timestamp());
        log.info("Key        : {}", record.key());
        log.info("Employee Id        : {}", employee.getId());
        log.info("First Name        : {}", employee.getFirstName());
        log.info("Last Name         : {}", employee.getLastName());
        log.info("Email             : {}", employee.getEmail());
        log.info("Department        : {}", employee.getDepartment());
        log.info("Designation       : {}", employee.getDesignation());
        log.info("Salary            : {}", employee.getSalary());
        log.info("========================================");

    }

}