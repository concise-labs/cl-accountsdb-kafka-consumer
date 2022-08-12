package com.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.ProtobufMessageType;
import javaprotobuf.UpdateAccountEventOuterClass;

import software.amazon.awssdk.services.glue.model.DataFormat;

public class TopicConsumerPOJO implements Runnable {
    Logger logger = LoggerFactory.getLogger(TopicConsumerPOJO.class.getName());

    static String BOOTSTRAP_SERVERS = "kafka-server-endpoint"; // replace with the actual endpoint
    static String GROUP_ID = "test-consumer";  
    
    static String ACCOUNTS_UPDATE_TOPIC = "solana.mainnet.account_updates";
    static String TRANSACTIONS_UPDATE_TOPIC = "solana.mainnet.transaction_updates";


    private Properties getConsumerConfig(){
        Properties properties = new Properties();
    
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GlueSchemaRegistryKafkaDeserializer.class.getName());

        // properties.setProperty(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.PROTOBUF.name());
        properties.setProperty(AWSSchemaRegistryConstants.AWS_REGION, "region");
        // properties.setProperty(AWSSchemaRegistryConstants.REGISTRY_NAME, "registry-name");
        // properties.setProperty(AWSSchemaRegistryConstants.SCHEMA_NAME, "UpdateAccountEvent.proto");
        properties.setProperty(AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE, ProtobufMessageType.POJO.getName());
        // properties.setProperty(AWSSchemaRegistryConstants.CACHE_TIME_TO_LIVE_MILLIS, "1"); // If not passed, uses 86400000 (24 Hours)
        // properties.setProperty(AWSSchemaRegistryConstants.CACHE_SIZE, "0");
        // properties.setProperty(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, "true");

        //DO NOT COMMIT so we can re-read the same messages
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return properties;
    }

    public void startConsumer() {

        //creating consumer
        KafkaConsumer<String, UpdateAccountEventOuterClass.UpdateAccountEvent> consumer = new KafkaConsumer<String, UpdateAccountEventOuterClass.UpdateAccountEvent>(getConsumerConfig());  
        //Subscribing
        Collection<String> topics = Arrays.asList(ACCOUNTS_UPDATE_TOPIC);
        
        consumer.subscribe(topics);
        
        try {
            while(true){
            //for (int i=0; i<100; i++){
                // System.out.println("POJO entered while loop...");
                ConsumerRecords<String, UpdateAccountEventOuterClass.UpdateAccountEvent> records = consumer.poll(Duration.ofMillis(1000));
                System.out.println("Got records....");

                for (final ConsumerRecord<String, UpdateAccountEventOuterClass.UpdateAccountEvent> record : records) {
                    UpdateAccountEventOuterClass.UpdateAccountEvent accountEvent = record.value();
                    System.out.println("Account Event Key: " + record.key() + ", Value: " + accountEvent.toString());
                    logger.info("Key: " + record.key() + ", Value: " + accountEvent.toString());
                }
            }
        } catch (Exception e) {
            // logger.info("POJO problem parsing record " , e);
            System.out.println("POJO problem parsing record " + e.getMessage() + e);
        } finally{
            consumer.close();
        } 


    }

    public void run() {
        startConsumer();
    }
}
