package com.github.code4kunal.kafka.consumers;

import com.github.code4kunal.kafka.producers.KafkaProducerWithCallbackDemo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;

public class KafkaConsumerDemo {

    public static void main(String[] args) {

        final String TOPIC = "new_topic";
        final Logger logger = LoggerFactory.getLogger(KafkaProducerWithCallbackDemo.class);
        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(KafkaConsumerConfig.getConsumerConfig());

        // subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(TOPIC));

        // poll for new data
        while (true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0

            for (ConsumerRecord<String, String> record : records) {
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
            }
        }
    }
}

