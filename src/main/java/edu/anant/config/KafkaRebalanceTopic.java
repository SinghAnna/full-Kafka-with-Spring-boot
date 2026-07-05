package edu.anant.config;


import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaRebalanceTopic {

    @Bean
    public NewTopic rebalanceTopic() {
        return TopicBuilder.name("rebalance-topic")
                .partitions(4)
                .replicas(1)
                .build();
    }
}
