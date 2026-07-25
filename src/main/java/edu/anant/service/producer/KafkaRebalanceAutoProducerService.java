//
//package edu.anant.service;
//
//import lombok.RequiredArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//import org.springframework.kafka.core.KafkaTemplate;
//import org.springframework.kafka.support.SendResult;
//import org.springframework.stereotype.Service;
//
//@Slf4j
//@Service
//@RequiredArgsConstructor
//public class KafkaRebalanceAutoProducerService {
//
//    private final KafkaTemplate<String, String> kafkaTemplate;
//
//    public void sendMessage(String message) {
//
//        kafkaTemplate.send("rebalance-topic", message)
//                .whenComplete(this::printResult);
//
//    }
//
//    private void printResult(
//            SendResult<String, String> result,
//            Throwable ex
//    ) {
//
//        if (ex != null) {
//
//            log.error("Message Sending Failed : {}", ex.getMessage());
//
//            return;
//        }
//
//        log.info("========================================");
//        log.info("MESSAGE SENT SUCCESSFULLY");
//        log.info("Topic      : {}", result.getRecordMetadata().topic());
//        log.info("Partition  : {}", result.getRecordMetadata().partition());
//        log.info("Offset     : {}", result.getRecordMetadata().offset());
//        log.info("Timestamp  : {}", result.getRecordMetadata().timestamp());
//        log.info("Message    : {}", result.getProducerRecord().value());
//        log.info("========================================");
//
//    }
//
//}