package com.kafkaProject.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopic {

    public NewTopic topic(){
        return TopicBuilder.name("wikimediaRecentChange")
                .build();
    }
}
