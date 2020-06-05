package com.github.code4kunal.kafka.kafkabasics.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaProducerDemo {
    public static void main(String[] args) {

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(KafkaProducerConfig.getProperties());

        // create a producer record
        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>("new_topic", "hello world! Lets learn kafka");

        // send data - asynchronous
        producer.send(record);

        // flush data
        producer.flush();

        // flush and close producer
        producer.close();

    }
}
