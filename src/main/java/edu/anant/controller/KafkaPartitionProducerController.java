package edu.anant.controller;



import edu.anant.service.KafkaProducerPartitionService;
import edu.anant.service.KafkaProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/messages/partition")
@RequiredArgsConstructor
public class KafkaPartitionProducerController {

    private final KafkaProducerPartitionService kafkaProducerPartitionService;

    @PostMapping
    public String publish(@RequestBody String message) {

      kafkaProducerPartitionService.sendMessage(message);

        return "Message Published Successfully";

    }

    @PostMapping("/bulk")
    public String publish() {

        kafkaProducerPartitionService.sendBulkMessages();

        return "1000 Messages Published Successfully";

    }

}