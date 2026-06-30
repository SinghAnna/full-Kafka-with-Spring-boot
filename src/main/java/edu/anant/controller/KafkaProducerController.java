package edu.anant.controller;




import edu.anant.service.KafkaProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/messages")
@RequiredArgsConstructor
public class KafkaProducerController {

    private final KafkaProducerService producerService;

    @PostMapping
    public String publish(@RequestBody String message){

        producerService.sendMessage(message);

        return "Message Published Successfully";
    }
}