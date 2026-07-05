package edu.anant.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaCommitSimpleTopic {

    @Bean
    public NewTopic autoCommitTopic() {

        return TopicBuilder.name("auto-commit-topic")
                .partitions(3)
                .replicas(1)
                .build();

    }

    @Bean
    public NewTopic manualCommitTopic() {

        return TopicBuilder.name("manual-commit-topic")
                .partitions(3)
                .replicas(1)
                .build();

    }

}