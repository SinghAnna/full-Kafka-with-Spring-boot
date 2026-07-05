//package edu.anant.controller;
//
//import edu.anant.service.KafkaOffsetProducerService;
//import lombok.RequiredArgsConstructor;
//import org.springframework.web.bind.annotation.*;
//
//@RestController
//@RequestMapping("/api/offset")
//@RequiredArgsConstructor
//public class KafkaOffsetController {
//
//    private final KafkaOffsetProducerService producerService;
//
//    /*
//     * -----------------------------------------------------
//     * AUTO COMMIT
//     * -----------------------------------------------------
//     */
//
//    @PostMapping("/auto")
//    public String sendAutoMessage(
//            @RequestBody String message
//    ) {
//
//        producerService.sendToAutoCommitTopic(message);
//
//        return "Message Sent Successfully to Auto Commit Topic";
//
//    }
//
//    /*
//     * -----------------------------------------------------
//     * MANUAL COMMIT
//     * -----------------------------------------------------
//     */
//
//    @PostMapping("/manual")
//    public String sendManualMessage(
//            @RequestBody String message
//    ) {
//
//        producerService.sendToManualCommitTopic(message);
//
//        return "Message Sent Successfully to Manual Commit Topic";
//
//    }
//
//    /*
//     * -----------------------------------------------------
//     * BULK AUTO COMMIT
//     * -----------------------------------------------------
//     */
//
//    @PostMapping("/auto/bulk/{count}")
//    public String sendBulkAutoMessages(
//            @PathVariable int count
//    ) {
//
//        producerService.sendBulkToAutoTopic(count);
//
//        return count + " Messages Sent Successfully";
//
//    }
//
//    /*
//     * -----------------------------------------------------
//     * BULK MANUAL COMMIT
//     * -----------------------------------------------------
//     */
//
//    @PostMapping("/manual/bulk/{count}")
//    public String sendBulkManualMessages(
//            @PathVariable int count
//    ) {
//
//        producerService.sendBulkToManualTopic(count);
//
//        return count + " Messages Sent Successfully";
//
//    }
//
//}