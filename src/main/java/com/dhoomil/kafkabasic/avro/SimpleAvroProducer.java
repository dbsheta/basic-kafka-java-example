package com.dhoomil.kafkabasic.avro;

import com.dhoomil.kafkabasic.BasicProducerCallback;
import com.dhoomil.kafkabasic.Constants;
import com.dhoomil.kafkabasic.avro.model.Employee;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

import static com.dhoomil.kafkabasic.Constants.AVRO_TOPIC;
import static com.dhoomil.kafkabasic.Constants.SCHEMA_REGISTRY;
import static com.dhoomil.kafkabasic.Constants.SLEEP_TIMER;

public class SimpleAvroProducer {
    public static void main(String[] args) throws InterruptedException {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.SERVERS);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 2);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, "true");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY);

        BasicProducerCallback callback = new BasicProducerCallback();
        Producer<String, Employee> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++) {
            Employee employee = DataGenerator.generateEmployee();
            String key = String.valueOf(employee.getId());
            ProducerRecord<String, Employee> record = new ProducerRecord<>(AVRO_TOPIC, key, employee);
            producer.send(record, callback);
            Thread.sleep(SLEEP_TIMER);
        }
    }
}
