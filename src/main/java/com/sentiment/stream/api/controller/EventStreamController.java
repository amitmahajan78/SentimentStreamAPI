package com.sentiment.stream.api.controller;

import com.sentiment.stream.api.mongodb.collection.SentimentScore;
import com.sentiment.stream.api.mongodb.collection.projection.ScoreByType;
import com.sentiment.stream.api.mongodb.collection.projection.TotalScore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/v2/stream")
@Slf4j
public class EventStreamController {

    @Autowired
    MongoTemplate mongoTemplate;

    @CrossOrigin(origins = "http://localhost:3000")
    @GetMapping("/scores")
    public List<TotalScore> getTotalScore() {

        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.group("sentiment_type").sum("score").as("score"),
                Aggregation.sort(Sort.Direction.ASC, Aggregation.previousOperation(), "sentiment_type")
        );

        AggregationResults<SentimentScore> aggregationResults =
                mongoTemplate.aggregate(aggregation, SentimentScore.class, SentimentScore.class);
        List<SentimentScore> sentimentScores = aggregationResults.getMappedResults();

        List<TotalScore> totalScores = new ArrayList<>();

        sentimentScores.forEach(v -> {
            TotalScore totalScore = new TotalScore();
            totalScore.setY(v.getScore());
            totalScore.setName(v.getId());
            totalScores.add(totalScore);
        });

        return totalScores;
    }

    @CrossOrigin(origins = "http://localhost:3000")
    @GetMapping("/scores/type")
    public List<ScoreByType> getScoreByType(@RequestParam String id) {

        Query query = new Query();
        query.addCriteria(Criteria.where("sentiment_type").is(id));

        List<SentimentScore> sentimentScores = mongoTemplate.find(query, SentimentScore.class);

        List<ScoreByType> scoreByTypes = new ArrayList<>();

        sentimentScores.forEach(v -> {
            ScoreByType scoreByType = new ScoreByType();
            scoreByType.setX(v.getLocal_date_time().getHour());
            scoreByType.setY(v.getScore());

            scoreByTypes.add(scoreByType);
        });

        return scoreByTypes;
    }

}
