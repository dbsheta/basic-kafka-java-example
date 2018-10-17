package com.dhoomil.kafkabasic;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class FileProducerExample {
    public static void main(String[] args) throws IOException, InterruptedException {
        // We first define the configuration for Producer
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        properties.put(ProducerConfig.RETRIES_CONFIG, 1 );
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        BasicProducerCallback callback = new BasicProducerCallback();

        BufferedReader br = new BufferedReader(new FileReader("test-source.txt"));
        String line;
        int i = 0;
        while (true) {
            line = br.readLine();
            if (line == null) {
                //wait until there is more of the file for us to read
                Thread.sleep(1000);
            } else {
                String key = String.format("Message %d", i++);
                System.out.println(line);
                ProducerRecord<String, String> record = new ProducerRecord<>("test", key, line);
                producer.send(record, callback);
            }
        }
    }
}
