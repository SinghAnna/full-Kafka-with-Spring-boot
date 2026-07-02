package edu.anant.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class KafkaConsumerTwo {

    @KafkaListener(
            topics = "first-topic",
            groupId = "group-2"
    )
    public void consume(String message) {

        log.info("====================================");
        log.info("Consumer-2 Received : {}", message);
        log.info("====================================");

    }

}