package edu.anant.service;



import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Service
public class KafkaProducerPartitionService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaProducerPartitionService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String message) {

        CompletableFuture<SendResult<String, String>> future =
                kafkaTemplate.send("partition-topic", message);

        future.whenComplete((result, ex) -> {

            if (ex == null) {

                log.info("==========================================");
                log.info("Message Sent Successfully");
                log.info("Topic      : {}", result.getRecordMetadata().topic());
                log.info("Partition  : {}", result.getRecordMetadata().partition());
                log.info("Offset     : {}", result.getRecordMetadata().offset());
                log.info("Timestamp  : {}", result.getRecordMetadata().timestamp());
                log.info("Message    : {}", message);
                log.info("==========================================");

            } else {

                log.error("Message Sending Failed");
                log.error(ex.getMessage());

            }

        });


    }

    public void sendBulkMessages() {

        ConcurrentHashMap<Integer, AtomicInteger> partitionCount = new ConcurrentHashMap<>();

        for (int i = 1; i <= 1000; i++) {

            CompletableFuture<SendResult<String, String>> future =
                    kafkaTemplate.send("bulk-partition-topic", "Message-" + i);

            future.whenComplete((result, ex) -> {

                if (ex == null) {

                    int partition = result.getRecordMetadata().partition();

                    partitionCount
                            .computeIfAbsent(partition, k -> new AtomicInteger())
                            .incrementAndGet();
                }

            });
        }

        // Thoda wait karo taaki async sends complete ho jaye
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        System.out.println("===== Partition Distribution =====");
        partitionCount.forEach((partition, count) ->
                System.out.println("Partition " + partition + " -> " + count.get() + " messages"));
    }

}