package com.github.code4kunal.kafka.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;

public class KafkaConsumerAssignSeekDemo {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(KafkaConsumerAssignSeekDemo.class.getName());

        final String TOPIC = "first_topic";


        // create consumer
        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<String, String>(KafkaConsumerConfig.getConsumerAssignseekConfig());

        // assign and seek are mostly used to replay data or fetch a specific message

        // assign
        TopicPartition partitionToReadFrom = new TopicPartition(TOPIC, 0);
        long offsetToReadFrom = 15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        // seek
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;

        // poll for new data
        while(keepOnReading){
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0

            for (ConsumerRecord<String, String> record : records){
                numberOfMessagesReadSoFar += 1;
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
                if (numberOfMessagesReadSoFar >= numberOfMessagesToRead){
                    keepOnReading = false; // to exit the while loop
                    break; // to exit the for loop
                }
            }
        }

        logger.info("Exiting the application");

    }
}