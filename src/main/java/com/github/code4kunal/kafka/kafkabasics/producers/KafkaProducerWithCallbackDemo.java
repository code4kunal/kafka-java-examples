package com.github.code4kunal.kafka.kafkabasics.producers;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaProducerWithCallbackDemo {

    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(KafkaProducerWithCallbackDemo.class);

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(KafkaProducerConfig.getProperties());

        for (int i = 0; i < 5; i++) {

            // create a producer record
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>("new_topic", "Hey there! " +
                            "Testing topics with callbacks! Record no - " + Integer.toString(i));

            // send data - asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception ex) {
                    // executes every time a record is successfully sent or an exception is thrown
                    if (ex == null) { // no exception
                        // the record was successfully sent
                        logger.info("Received new metadata. \n" +
                                "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else { //exception occurred
                        logger.error("Error while producing", ex);
                    }
                }
            });
        }

        // flush data
        producer.flush();
        // flush and close producer
        producer.close();
    }
}

