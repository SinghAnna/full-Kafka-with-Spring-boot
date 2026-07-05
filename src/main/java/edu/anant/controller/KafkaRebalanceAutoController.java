//package edu.anant.controller;
//
//import edu.anant.service.KafkaRebalanceAutoProducerService;
//import edu.anant.service.KafkaRebalanceProducerService;
//import lombok.RequiredArgsConstructor;
//import org.springframework.web.bind.annotation.*;
//
//@RestController
//@RequestMapping("/api/rebalance")
//@RequiredArgsConstructor
//public class KafkaRebalanceAutoController {
//
//    private final KafkaRebalanceAutoProducerService kafkaRebalanceAutoProducerService;
//
//    @PostMapping
//    public String publish(@RequestBody String message) {
//
//        kafkaRebalanceAutoProducerService.sendMessage(message);
//
//        return "Message Published Successfully";
//
//    }
//
//    @PostMapping("/bulk/{count}")
//    public String publishBulk(@PathVariable int count) {
//
//        for (int i = 1; i <= count; i++) {
//
//            kafkaRebalanceAutoProducerService.sendMessage(
//                    "Rebalance Message-" + i
//            );
//
//        }
//
//        return count + " Messages Published Successfully";
//
//    }
//
//}