package edu.anant.config;


import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaPartitionTopic {

    @Bean
    public NewTopic partitionTopic() {
        return new NewTopic("partition-topic", 3, (short) 1);
    }

    @Bean
    public NewTopic bulkPartitionTopic() {
        return new NewTopic("bulk-partition-topic", 3, (short) 1);
    }


}
