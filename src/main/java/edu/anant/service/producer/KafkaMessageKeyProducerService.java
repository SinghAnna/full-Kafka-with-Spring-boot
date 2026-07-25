package edu.anant.service.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaMessageKeyProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    // Without Key
    public void sendWithoutKey(String message) {

        kafkaTemplate.send("key-topic", message)
                .whenComplete(this::printResult);

    }

    // With Key
    public void sendWithKey(String key, String message) {

        kafkaTemplate.send("key-topic", key, message)
                .whenComplete(this::printResult);

    }

    private void printResult(
            SendResult<String, String> result,
            Throwable ex
    ) {

        if (ex != null) {

            log.error("Error : {}", ex.getMessage());

            return;
        }

        log.info("========================================");
        log.info("Topic      : {}",
                result.getRecordMetadata().topic());

        log.info("Partition  : {}",
                result.getRecordMetadata().partition());

        log.info("Offset     : {}",
                result.getRecordMetadata().offset());

        log.info("Timestamp  : {}",
                result.getRecordMetadata().timestamp());

        log.info("Key        : {}",
                result.getProducerRecord().key());

        log.info("Message    : {}",
                result.getProducerRecord().value());

        log.info("========================================");
    }



    public void sendBulkWithoutKey(int totalMessages) {

        for (int i = 1; i <= totalMessages; i++) {

            String message = "Message-" + i;

            kafkaTemplate.send("key-topic", message)
                    .whenComplete((result, ex) -> {

                        if (ex == null) {

                            log.info(
                                    "Message {} -> Partition {}",
                                    result.getProducerRecord().value(),
                                    result.getRecordMetadata().partition()
                            );

                        }

                    });

        }

    }



    public void sendBulkWithKey(String key, int totalMessages) {

        for (int i = 1; i <= totalMessages; i++) {

            String message = "Order-" + i;

            kafkaTemplate.send("key-topic", key, message)
                    .whenComplete((result, ex) -> {

                        if (ex == null) {

                            log.info(
                                    "{} -> Partition {}",
                                    result.getProducerRecord().value(),
                                    result.getRecordMetadata().partition()
                            );

                        }

                    });

        }



    }

}