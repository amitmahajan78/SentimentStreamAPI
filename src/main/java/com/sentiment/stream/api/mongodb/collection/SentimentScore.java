package com.sentiment.stream.api.mongodb.collection;

import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;

@Getter
@Setter
@Document("sentiment_score")
public class SentimentScore {

    @Id
    private String id;
    private LocalDateTime local_date_time;
    private String sentiment_type;
    private float score;
}
