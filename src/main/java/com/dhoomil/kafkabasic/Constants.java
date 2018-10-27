package com.dhoomil.kafkabasic;

public class Constants {
    public static final String SERVERS = "bigdata-1:9092, bigdata-2:9092, bigdata-3:9092";
    public static final String TEST_TOPIC = "test";
    public static final String SOURCE_TOPIC = "test";
    public static final String SINK_TOPIC = "test-filtered";
    public static final String AVRO_TOPIC = "test-avro";
    public static final long SLEEP_TIMER = 1000;
    public static final String SCHEMA_REGISTRY = "http://35.185.77.36:8081";
    public static final String CONSUMER_GROUP = "avro-consumers";
}
