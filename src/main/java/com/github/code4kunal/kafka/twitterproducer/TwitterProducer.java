package com.github.code4kunal.kafka.twitterproducer;


import com.github.code4kunal.kafka.kafkabasics.producers.KafkaProducerConfig;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    Logger logger  = LoggerFactory.getLogger(TwitterProducer.class.getName());

    private static final String CONSUMER_KEY = "GJ0tXV1suysRa6oatgtpI9yiQ";
    private static final String CONSUMER_SECRET = "sZQPYvUafX43lAjlHeN4fNTjRr7ZoftgGTgd7akupo3XA1XMDQ";
    private static final String ACCESS_TOKEN  = "1241685096207249408-CKnyvfOYkPAP32HhwGALIvkKKatRzJ";
    private static final String ACCESS_TOKEN_SECRET = "TnxF8hC8vFshm6kl0sFxOqgYwloPK86I9QNIOSqx2P2Qv";

    public TwitterProducer() {}

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        logger.info("Setting Up!!");
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        Client client  = scaffoldTwitterClient(msgQueue);
        client.connect();

        KafkaProducer<String, String> producer = createKafkaProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(() ->{
            logger.info("Stopping Application.....!");
            logger.info(("Shutting down twitter client!"));
            client.stop();
            logger.info("Shutting down kafka producer!");
            producer.close();
        }));
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(msg!=null){
                logger.info(msg);
                producer.send(new ProducerRecord<String, String>("tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception ex) {
                        if(ex!=null){
                            logger.error("Error sending message", ex);
                        }
                    }
                });
            }
        }
        logger.info("Ending Application!!");
    }

    private KafkaProducer<String,String> createKafkaProducer() {
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(KafkaProducerConfig.getProperties());

       return producer;

    }

    public Client scaffoldTwitterClient(BlockingQueue<String> msgQueue) {

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("java", "covid19", "coronavirus", "usa", "politics");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET);

        ClientBuilder builder = new ClientBuilder()
                .name("Mt-Twitter-client")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();

        return hosebirdClient;
    }

}
