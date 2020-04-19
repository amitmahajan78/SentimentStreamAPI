package com.sentiment.stream.api.consumer;

import com.sentiment.stream.api.mongodb.collection.SentimentScore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

@Component
@Slf4j
public class ScoreDataKafkaListener {

    @Autowired
    MongoTemplate mongoTemplate;

    @KafkaListener(topics = "aggregated-score")
    public void listen(@Payload String scoreData, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key) {
        log.info("received key {} and value {}", key, scoreData);

        addScoresToCollection(key, scoreData);
    }

    private void addScoresToCollection(String key, String scoreData) {

        String[] keySplitter = key.split("-");

        LocalDateTime date =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(keySplitter[0])), ZoneId.systemDefault());

        Query query = new Query();
        query.addCriteria(Criteria.where("id").is(key));
        Update update = new Update();
        update.set("local_date_time", date);
        update.set("sentiment_type", keySplitter[1]);
        update.set("score", Float.parseFloat(scoreData));

        mongoTemplate.upsert(query, update, SentimentScore.class);
    }
}
