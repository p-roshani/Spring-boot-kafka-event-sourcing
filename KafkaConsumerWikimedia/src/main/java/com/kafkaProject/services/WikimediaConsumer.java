package com.kafkaProject.services;

import com.kafkaProject.entity.WikimediaData;
import com.kafkaProject.repository.WikimediaDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class WikimediaConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(WikimediaConsumer.class);

    private WikimediaDataRepository dataRepository;

    public WikimediaConsumer(WikimediaDataRepository dataRepository) {
        this.dataRepository = dataRepository;
    }

    @KafkaListener(topics = "wikimediaRecentChange", groupId = "group1")
    public void consumeMessage(String eventMessage){

        LOGGER.info(String.format("Event message received -> %s", eventMessage));

        WikimediaData wikimediaData = new WikimediaData();
        wikimediaData.setWikiEventData(eventMessage);

        dataRepository.save(wikimediaData);

    }
}
