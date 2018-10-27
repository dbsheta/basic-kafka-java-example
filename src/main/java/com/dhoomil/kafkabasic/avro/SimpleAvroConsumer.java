package com.dhoomil.kafkabasic.avro;

import com.dhoomil.kafkabasic.Constants;
import com.dhoomil.kafkabasic.avro.model.Employee;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.protocol.types.Field;

import java.time.Duration;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import static com.dhoomil.kafkabasic.Constants.*;

public class SimpleAvroConsumer {
    public static void main(String[] args) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");

        Collection<String> topics = Collections.singleton(AVRO_TOPIC);
        Consumer<String, Employee> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(topics);

        while (true) {
            ConsumerRecords<String, Employee> records = consumer.poll(Duration.ofMillis(500));
            if (records.count() > 0) {
                System.out.println(consumer.assignment());
                System.out.printf("Fecthed %d records\n", records.count());
                System.out.println(records.iterator().next().value().getSchema());
                for (ConsumerRecord record : records) {
                    Employee employee = (Employee) record.value();
                    System.out.println("- " + employee);
                }
            }
        }

    }
}
