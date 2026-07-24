package edu.anant.config;


import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaJsonTopicConfig {

    @Bean
    public NewTopic employeeTopic() {

        return TopicBuilder
                .name("employee-topic")
                .partitions(3)
                .replicas(1)
                .build();

    }

}