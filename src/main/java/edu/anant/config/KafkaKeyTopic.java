package edu.anant.config;


import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaKeyTopic {

    @Bean
    public NewTopic keyTopic(){
        return  new NewTopic("key-topic", 3, (short) 1);
    }
}
