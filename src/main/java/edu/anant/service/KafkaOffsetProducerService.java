package edu.anant.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaOffsetProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    // Auto Commit Topic
    public void sendToAutoCommitTopic(String message) {

        kafkaTemplate.send("auto-commit-topic", message)
                .whenComplete(this::printResult);

    }

    // Manual Commit Topic
    public void sendToManualCommitTopic(String message) {

        kafkaTemplate.send("manual-commit-topic", message)
                .whenComplete(this::printResult);

    }

    // Bulk Auto Commit Messages
    public void sendBulkToAutoTopic(int count) {

        for (int i = 1; i <= count; i++) {

            String message = "Auto Message - " + i;

            kafkaTemplate.send("auto-commit-topic", message)
                    .whenComplete(this::printResult);
        }

    }

    // Bulk Manual Commit Messages
    public void sendBulkToManualTopic(int count) {

        for (int i = 1; i <= count; i++) {

            String message = "Manual Message - " + i;

            kafkaTemplate.send("manual-commit-topic", message)
                    .whenComplete(this::printResult);
        }

    }

    private void printResult(
            SendResult<String, String> result,
            Throwable ex
    ) {

        if (ex != null) {

            log.error("Failed : {}", ex.getMessage());

            return;
        }

        log.info("========================================");
        log.info("MESSAGE SENT SUCCESSFULLY");
        log.info("Topic      : {}", result.getRecordMetadata().topic());
        log.info("Partition  : {}", result.getRecordMetadata().partition());
        log.info("Offset     : {}", result.getRecordMetadata().offset());
        log.info("Timestamp  : {}", result.getRecordMetadata().timestamp());
        log.info("Message    : {}", result.getProducerRecord().value());
        log.info("========================================");
    }

}