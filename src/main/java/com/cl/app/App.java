package com.cl.app;

import com.kafka.TopicConsumerBruteForce;
import com.kafka.TopicConsumerDynamicMessage;
import com.kafka.TopicConsumerPOJO;
import com.kafka.TopicProducerPOJO;
import com.kafka.TopicProducerBruteForce;
import com.kafka.CLKafkaDBImpl;

public class App {
    public static void main (String[] args) throws Exception {

        // This is preferred
        // new Thread(new TopicConsumerPOJO()).start();
        // new Thread(new TopicProducerPOJO()).start();
        // new TopicConsumerPOJO().startConsumer();
        // new TopicProducerPOJO().startProducer();

        // new TopicConsumerDynamicMessage().startConsumer();

        // new Thread(new TopicConsumerBruteForce()).start();

        // new Thread(new TopicProducerBruteForce()).start();

        // new TopicConsumerBruteForce().startConsumer();

        new CLKafkaDBImpl().printRecordPg();
    }

}
