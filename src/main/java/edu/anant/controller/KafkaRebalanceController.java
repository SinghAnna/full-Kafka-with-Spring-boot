package edu.anant.controller;

import edu.anant.service.KafkaRebalanceProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/rebalance")
@RequiredArgsConstructor
public class KafkaRebalanceController {

    private final KafkaRebalanceProducerService producerService;

    /**
     * Send Single Message
     */
    @PostMapping
    public String sendMessage(
            @RequestBody String message
    ) {

        producerService.sendMessage(message);

        return "Message Sent Successfully";

    }

    /**
     * Send Message With Key
     */
    @PostMapping("/key/{key}")
    public String sendMessageWithKey(
            @PathVariable String key,
            @RequestBody String message
    ) {

        producerService.sendMessage(key, message);

        return "Message Sent Successfully with Key : " + key;

    }

    /**
     * Send Bulk Messages
     */
    @PostMapping("/bulk/{count}")
    public String sendBulkMessages(
            @PathVariable int count
    ) {

        producerService.sendBulkMessages(count);

        return count + " Messages Sent Successfully";

    }

    /**
     * Send Bulk Messages With Same Key
     */
    @PostMapping("/bulk/{key}/{count}")
    public String sendBulkMessagesWithKey(
            @PathVariable String key,
            @PathVariable int count
    ) {

        producerService.sendBulkMessagesWithKey(key, count);

        return count + " Messages Sent Successfully with Key : " + key;

    }

}