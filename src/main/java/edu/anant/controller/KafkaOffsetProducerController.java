package edu.anant.controller;

import edu.anant.service.producer.KafkaOffsetProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/offset")
@RequiredArgsConstructor
public class KafkaOffsetProducerController {

    private final KafkaOffsetProducerService producerService;

    // Auto Commit
    @PostMapping("/auto")
    public String sendAuto(@RequestBody String message) {

        producerService.sendToAutoCommitTopic(message);

        return "Message Sent to Auto Commit Topic";

    }

    // Manual Commit
    @PostMapping("/manual")
    public String sendManual(@RequestBody String message) {

        producerService.sendToManualCommitTopic(message);

        return "Message Sent to Manual Commit Topic";

    }

    @PostMapping("/auto/bulk/{count}")
    public String sendAutoBulk(@PathVariable int count) {

        for (int i = 1; i <= count; i++) {

            producerService.sendToAutoCommitTopic("Auto Message-" + i);

        }

        return count + " Messages Sent";

    }

    @PostMapping("/manual/bulk/{count}")
    public String sendManualBulk(@PathVariable int count) {

        for (int i = 1; i <= count; i++) {

            producerService.sendToManualCommitTopic("Manual Message-" + i);

        }

        return count + " Messages Sent";

    }

}