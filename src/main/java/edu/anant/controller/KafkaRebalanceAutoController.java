package edu.anant.controller;

import edu.anant.service.KafkaRebalanceProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/rebalance")
@RequiredArgsConstructor
public class KafkaRebalanceController {

    private final KafkaRebalanceProducerService producerService;

    @PostMapping
    public String publish(@RequestBody String message) {

        producerService.sendMessage(message);

        return "Message Published Successfully";

    }

    @PostMapping("/bulk/{count}")
    public String publishBulk(@PathVariable int count) {

        for (int i = 1; i <= count; i++) {

            producerService.sendMessage(
                    "Rebalance Message-" + i
            );

        }

        return count + " Messages Published Successfully";

    }

}