package edu.anant.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/consumer")
@RequiredArgsConstructor
public class KafkaConsumerController {

    @GetMapping("/status")
    public String getStatus() {
        return """
                Kafka Consumer is running successfully.

                Topic    : first-topic
                Group ID : group-1

                Note:
                Messages are consumed automatically using @KafkaListener.
                No REST API is required to consume messages.
                """;
    }
}