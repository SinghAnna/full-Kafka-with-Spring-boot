package edu.anant.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String message) {

        kafkaTemplate.send("first-topic", message);

        log.info("====================================");
        log.info("Message Sent : {}", message);
        log.info("====================================");
    }
}