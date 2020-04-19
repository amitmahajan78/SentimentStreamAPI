package com.sentiment.stream.api.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafkaStreams
@Slf4j
public class SentimentStreamProcessor {

    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServers;

    @Value("${application.stream.applicationId}")
    private String applicationId;

    @Value("${application.stream.inbound.topic}")
    private String receiveSentimentData;

    @Value("${application.stream.outbound.topic}")
    private String sendingAggregatedScore;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public StreamsConfig streamsConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put("default.deserialization.exception.handler", LogAndContinueExceptionHandler.class);

        return new StreamsConfig(props);
    }

    @Bean
    public KStream<String, String> kStream(StreamsBuilder builder) {

        KStream<String, String> sentimentDataStream = builder.stream(receiveSentimentData, Consumed.with(Serdes.String(), Serdes.String()));

        sentimentDataStream.groupByKey()
                .aggregate(() -> "0.0",
                        (key, value, aggregate) -> String.valueOf((Float.valueOf(value) + Float.valueOf(aggregate))),
                        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("sentiment-score-store")
                                .withValueSerde(Serdes.String())
                                .withKeySerde(Serdes.String())
                )
                .toStream()
                .to(sendingAggregatedScore, Produced.with(Serdes.String(), Serdes.String()));


        return sentimentDataStream;
    }
}
