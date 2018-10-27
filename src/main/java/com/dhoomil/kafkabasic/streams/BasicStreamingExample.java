package com.dhoomil.kafkabasic.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

import static com.dhoomil.kafkabasic.Constants.SINK_TOPIC;
import static com.dhoomil.kafkabasic.Constants.SOURCE_TOPIC;

public class BasicStreamingExample {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "35.196.241.11:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-basic");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        // Filter topics where key ends in zero
        KStream<String, String> sourceStream = streamsBuilder.stream(SOURCE_TOPIC);
        sourceStream
                .filter((k, v) -> k.endsWith("0"))
                .mapValues(v -> v.toUpperCase())
                .to(SINK_TOPIC);

        final Topology topology = streamsBuilder.build();
        System.out.println(topology.describe());

        final KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
        System.out.printf("Starting Processing of records from topic: %s to %s", SOURCE_TOPIC, SINK_TOPIC);
        kafkaStreams.start();
    }
}
