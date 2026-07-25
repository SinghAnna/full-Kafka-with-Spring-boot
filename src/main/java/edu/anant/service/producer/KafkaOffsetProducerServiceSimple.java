
package edu.anant.service.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaOffsetProducerServiceSimple {

    private static final String AUTO_TOPIC = "auto-commit-topic";
    private static final String MANUAL_TOPIC = "manual-commit-topic";

    private final KafkaTemplate<String, String> kafkaTemplate;

    /*
     * Send Single Message
     * Auto Commit Topic
     */

    public void sendToAutoCommitTopic(String message) {

        kafkaTemplate.send(AUTO_TOPIC, message)
                .whenComplete(this::printMetadata);

    }

    /*
     * Send Single Message
     * Manual Commit Topic
     */

    public void sendToManualCommitTopic(String message) {

        kafkaTemplate.send(MANUAL_TOPIC, message)
                .whenComplete(this::printMetadata);

    }

    /*
     * Bulk Messages
     * Auto Commit
     */

    public void sendBulkToAutoTopic(int count) {

        log.info("Sending {} Messages...", count);

        for (int i = 1; i <= count; i++) {

            kafkaTemplate.send(
                    AUTO_TOPIC,
                    "Auto Message-" + i
            ).whenComplete(this::printMetadata);

        }

        log.info("{} Messages Sent Successfully", count);

    }

    /*
     * Bulk Messages
     * Manual Commit
     */

    public void sendBulkToManualTopic(int count) {

        log.info("Sending {} Messages...", count);

        for (int i = 1; i <= count; i++) {

            kafkaTemplate.send(
                    MANUAL_TOPIC,
                    "Manual Message-" + i
            ).whenComplete(this::printMetadata);

        }

        log.info("{} Messages Sent Successfully", count);

    }

    /*
     * Print Producer Metadata
     */

    private void printMetadata(
            SendResult<String, String> result,
            Throwable ex
    ) {

        if (ex != null) {

            log.error("Failed to Send Message");
            log.error(ex.getMessage());

            return;
        }

        log.info("======================================");
        log.info("MESSAGE SENT SUCCESSFULLY");
        log.info("--------------------------------------");

        log.info("Topic      : {}",
                result.getRecordMetadata().topic());

        log.info("Partition  : {}",
                result.getRecordMetadata().partition());

        log.info("Offset     : {}",
                result.getRecordMetadata().offset());

        log.info("Timestamp  : {}",
                result.getRecordMetadata().timestamp());

        log.info("Key        : {}",
                result.getProducerRecord().key());

        log.info("Message    : {}",
                result.getProducerRecord().value());

        log.info("======================================");

    }

}