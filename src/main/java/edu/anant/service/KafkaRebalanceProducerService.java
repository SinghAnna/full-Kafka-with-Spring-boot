package edu.anant.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaRebalanceProducerService {

    private static final String TOPIC = "rebalance-topic";

    private final KafkaTemplate<String, String> kafkaTemplate;

    /**
     * Send Single Message
     */
    public void sendMessage(String message) {

        kafkaTemplate.send(TOPIC, message)
                .whenComplete(this::printResult);

    }

    /**
     * Send Message With Key
     */
    public void sendMessage(String key, String message) {

        kafkaTemplate.send(TOPIC, key, message)
                .whenComplete(this::printResult);

    }

    /**
     * Send Bulk Messages
     */
    public void sendBulkMessages(int count) {

        log.info("========================================");
        log.info("Sending {} Messages...", count);
        log.info("========================================");

        for (int i = 1; i <= count; i++) {

            kafkaTemplate.send(
                    TOPIC,
                    "Message-" + i
            ).whenComplete(this::printResult);

        }

    }

    /**
     * Send Bulk Messages With Same Key
     */
    public void sendBulkMessagesWithKey(
            String key,
            int count
    ) {

        log.info("========================================");
        log.info("Sending {} Messages with Key [{}]", count, key);
        log.info("========================================");

        for (int i = 1; i <= count; i++) {

            kafkaTemplate.send(
                    TOPIC,
                    key,
                    "Message-" + i
            ).whenComplete(this::printResult);

        }

    }

    /**
     * Print Producer Metadata
     */
    private void printResult(
            SendResult<String, String> result,
            Throwable ex
    ) {

        if (ex != null) {

            log.error("========================================");
            log.error("MESSAGE SEND FAILED");
            log.error("Reason : {}", ex.getMessage());
            log.error("========================================");

            return;

        }

        log.info("========================================");
        log.info("MESSAGE SENT SUCCESSFULLY");
        log.info("Topic      : {}", result.getRecordMetadata().topic());
        log.info("Partition  : {}", result.getRecordMetadata().partition());
        log.info("Offset     : {}", result.getRecordMetadata().offset());
        log.info("Timestamp  : {}", result.getRecordMetadata().timestamp());
        log.info("Key        : {}", result.getProducerRecord().key());
        log.info("Message    : {}", result.getProducerRecord().value());
        log.info("========================================");

    }

}