package edu.anant.service;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

@Slf4j
@Service
public class ManualPollConsumer {

    private KafkaConsumer<String, String> consumer;

    @PostConstruct
    public void start() {

        Thread consumerThread = new Thread(this::consume);

        consumerThread.setName("manual-poll-consumer");

        consumerThread.start();

    }

    private void consume() {

        Properties props = new Properties();

        // -------------------------------
        // Kafka Broker
        // -------------------------------
        props.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9093"
        );

        // -------------------------------
        // Consumer Group
        // -------------------------------
        props.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "rebalance-group"
        );

        // -------------------------------
        // Client ID
        // -------------------------------
        props.put(
                ConsumerConfig.CLIENT_ID_CONFIG,
                "manual-poll-consumer"
        );

        // -------------------------------
        // Deserializers
        // -------------------------------
        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName()
        );

        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName()
        );

        // -------------------------------
        // Offset Reset
        // -------------------------------
        props.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
        );

        // -------------------------------
        // Auto Commit
        // -------------------------------
        props.put(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                true
        );

        consumer = new KafkaConsumer<>(props);

        // Shutdown Hook
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {

                    log.info("Shutdown Signal Received...");

                    consumer.wakeup();

                })
        );

        // Subscribe with Rebalance Listener
        consumer.subscribe(
                List.of("rebalance-topic"),
                new ConsumerRebalanceListener() {

                    @Override
                    public void onPartitionsRevoked(
                            Collection<TopicPartition> partitions) {

                        log.info("======================================");
                        log.info("REBALANCING STARTED");
                        log.info("Partitions Revoked");

                        for (TopicPartition partition : partitions) {

                            log.info(
                                    "Revoked Partition : {}",
                                    partition.partition()
                            );

                        }

                        log.info("======================================");
                    }

                    @Override
                    public void onPartitionsAssigned(
                            Collection<TopicPartition> partitions) {

                        log.info("======================================");
                        log.info("REBALANCING COMPLETED");
                        log.info("Partitions Assigned");

                        for (TopicPartition partition : partitions) {

                            log.info(
                                    "Assigned Partition : {}",
                                    partition.partition()
                            );

                        }

                        log.info("======================================");
                    }

                }
        );

        log.info("Consumer Started Successfully...");

        try {

            while (true) {

                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, String> record : records) {

                    log.info("======================================");
                    log.info("MESSAGE RECEIVED");
                    log.info("Topic      : {}", record.topic());
                    log.info("Partition  : {}", record.partition());
                    log.info("Offset     : {}", record.offset());
                    log.info("Key        : {}", record.key());
                    log.info("Value      : {}", record.value());
                    log.info("Timestamp  : {}", record.timestamp());
                    log.info("======================================");

                }

            }

        } catch (WakeupException e) {

            log.info("Consumer Wakeup Triggered.");

        } catch (Exception e) {

            log.error("Consumer Error : {}", e.getMessage());

        } finally {

            consumer.close();

            log.info("Consumer Closed Successfully.");

        }

    }

}