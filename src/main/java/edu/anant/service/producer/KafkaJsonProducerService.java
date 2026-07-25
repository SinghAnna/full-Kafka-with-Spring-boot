package edu.anant.service.producer;

import edu.anant.dto.Employee;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaJsonProducerService {

    private final KafkaTemplate<String, Employee> kafkaTemplate;

    private static final String TOPIC = "employee-topic";

    /**
     * Send Employee without Key
     */
    public void send(Employee employee) {

        kafkaTemplate.send(TOPIC, employee)
                .whenComplete(this::printResult);

    }

    /**
     * Send Employee with Key
     */
    public void send(String key, Employee employee) {

        kafkaTemplate.send(TOPIC, key, employee)
                .whenComplete(this::printResult);

    }

    private void printResult(
            SendResult<String, Employee> result,
            Throwable ex
    ) {

        if (ex != null) {

            log.error("Message Sending Failed : {}", ex.getMessage());

            return;

        }

        log.info("========================================");
        log.info("EMPLOYEE SENT SUCCESSFULLY");
        log.info("Topic      : {}", result.getRecordMetadata().topic());
        log.info("Partition  : {}", result.getRecordMetadata().partition());
        log.info("Offset     : {}", result.getRecordMetadata().offset());
        log.info("Timestamp  : {}", result.getRecordMetadata().timestamp());
        log.info("Key        : {}", result.getProducerRecord().key());
        log.info("Employee   : {}", result.getProducerRecord().value());
        log.info("========================================");

    }

}