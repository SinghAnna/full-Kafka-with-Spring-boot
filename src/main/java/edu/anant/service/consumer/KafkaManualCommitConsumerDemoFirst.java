package edu.anant.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class KafkaManualCommitConsumerDemoFirst {

    @KafkaListener(
            topics = "manual-commit-topic",
            groupId = "manual-group",
            containerFactory = "manualAckListenerContainerFactory"
    )
    public void consume(
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment
    ) {

        log.info("========================================");
        log.info("MANUAL COMMIT CONSUMER");
        log.info("Topic      : {}", record.topic());
        log.info("Partition  : {}", record.partition());
        log.info("Offset     : {}", record.offset());
        log.info("Key        : {}", record.key());
        log.info("Message    : {}", record.value());
        log.info("Timestamp  : {}", record.timestamp());

        acknowledgment.acknowledge();

        log.info("Offset Committed Successfully");
        log.info("========================================");
    }
}