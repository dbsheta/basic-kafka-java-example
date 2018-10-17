package com.dhoomil.kafkabasic;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class BasicProducerCallback implements Callback {
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e == null) {
            System.out.println("Message delivered successfully to partition " + recordMetadata.partition());
        } else {
            System.out.println("Message delivery failed: " + e.getMessage());
        }
    }
}
