package edu.anant.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class KafkaConsumerService {

    @KafkaListener(
            topics = "first-topic",
            groupId = "group-1"
    )
    public void consumeMessage(String message) {

        log.info("====================================");
        log.info("Message Received : {}", message);
        log.info("====================================");

    }
}