package edu.anant.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ConsumerTwo {

    @KafkaListener(
            topics = "rebalance-topic",
            groupId = "rebalance-group"
    )
    public void consume(ConsumerRecord<String, String> record) {

        log.info("========================================");
        log.info("CONSUMER-2");
        log.info("Topic      : {}", record.topic());
        log.info("Partition  : {}", record.partition());
        log.info("Offset     : {}", record.offset());
        log.info("Key        : {}", record.key());
        log.info("Message    : {}", record.value());
        log.info("Timestamp  : {}", record.timestamp());
        log.info("========================================");

    }
}