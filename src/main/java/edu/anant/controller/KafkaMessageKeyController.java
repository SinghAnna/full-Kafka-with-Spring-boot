package edu.anant.controller;

import edu.anant.service.KafkaMessageKeyProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/message-keys")
@RequiredArgsConstructor
public class KafkaMessageKeyController {

    private final KafkaMessageKeyProducerService producerService;

    /**
     * Send Single Message Without Key
     */
    @PostMapping("/without-key")
    public String sendWithoutKey(@RequestBody String message) {

        producerService.sendWithoutKey(message);

        return "Message Sent Successfully Without Key";
    }

    /**
     * Send Single Message With Key
     */
    @PostMapping("/with-key")
    public String sendWithKey(
            @RequestParam String key,
            @RequestBody String message
    ) {

        producerService.sendWithKey(key, message);

        return "Message Sent Successfully With Key";
    }

    /**
     * Send Bulk Messages Without Key
     *
     * Example:
     * POST /api/message-keys/bulk/without-key?count=1000
     */
    @PostMapping("/bulk/without-key")
    public String sendBulkWithoutKey(
            @RequestParam(defaultValue = "1000") int count
    ) {

        producerService.sendBulkWithoutKey(count);

        return count + " Messages Sent Without Key";
    }

    /**
     * Send Bulk Messages With Same Key
     *
     * Example:
     * POST /api/message-keys/bulk/with-key?key=Customer-101&count=1000
     */
    @PostMapping("/bulk/with-key")
    public String sendBulkWithKey(
            @RequestParam String key,
            @RequestParam(defaultValue = "1000") int count
    ) {

        producerService.sendBulkWithKey(key, count);

        return count + " Messages Sent With Key : " + key;
    }

}