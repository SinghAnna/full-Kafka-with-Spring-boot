package edu.anant.service.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class KafkaManualCommitConsumer {

    @KafkaListener(
            topics = "manual-commit-topic",
            groupId = "manual-group",
            containerFactory = "manualAckListenerContainerFactory"
    )
    public void consume(
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment
    ) {

        try {

            log.info("");
            log.info("======================================================");
            log.info("            MANUAL COMMIT CONSUMER");
            log.info("======================================================");

            log.info("Topic      : {}", record.topic());
            log.info("Partition  : {}", record.partition());
            log.info("Offset     : {}", record.offset());
            log.info("Key        : {}", record.key());
            log.info("Message    : {}", record.value());
            log.info("Timestamp  : {}", record.timestamp());

            /*
             * Business Logic
             */

            processMessage(record);

            /*
             * Commit Offset
             */

            acknowledgment.acknowledge();

            log.info("Status     : Offset Committed Successfully");

            log.info("======================================================");
            log.info("");

        } catch (Exception ex) {

            log.error("Processing Failed : {}", ex.getMessage());

        }

    }

    private void processMessage(
            ConsumerRecord<String, String> record
    ) {

        /*
         * Crash Demo
         */

        if (record.value().equalsIgnoreCase("FAIL")) {

            throw new RuntimeException("Business Exception");

        }

        /*
         * Simulate Processing
         */

        log.info("Processing Message : {}", record.value());

    }

}