package com.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import javaprotobuf.UpdateAccountEventOuterClass;

import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.ProtobufMessageType;
import software.amazon.awssdk.services.glue.model.DataFormat;

public class TopicProducerPOJO implements Runnable {
    Logger logger = LoggerFactory.getLogger(TopicProducerPOJO.class.getName());

    static String BOOTSTRAP_SERVERS = "kafka-server-endpoint"; // replace with the actual endpoint
    static String GROUP_ID = "test-producer";  
    
    static String ACCOUNTS_UPDATE_TOPIC = "solana.mainnet.account_updates";
    static String TRANSACTIONS_UPDATE_TOPIC = "solana.mainnet.transaction_updates";


    private Properties getProducerConfig() {
        Properties properties = new Properties();
    
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GlueSchemaRegistryKafkaSerializer.class.getName());

        properties.setProperty(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.PROTOBUF.name());
        properties.setProperty(AWSSchemaRegistryConstants.AWS_REGION, "region");
        properties.setProperty(AWSSchemaRegistryConstants.REGISTRY_NAME, "registry-name");
        properties.setProperty(AWSSchemaRegistryConstants.SCHEMA_NAME, "UpdateAccountEvent.proto");
        properties.setProperty(AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE, ProtobufMessageType.POJO.getName());
        // properties.setProperty(AWSSchemaRegistryConstants.CACHE_TIME_TO_LIVE_MILLIS, "1"); // If not passed, uses 86400000 (24 Hours)
        // properties.setProperty(AWSSchemaRegistryConstants.CACHE_SIZE, "0");
        // properties.setProperty(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, "true");

        return properties;
    }

    public void startProducer() {

        //creating producer
        KafkaProducer<String, UpdateAccountEventOuterClass.UpdateAccountEvent> producer = new KafkaProducer<String, UpdateAccountEventOuterClass.UpdateAccountEvent>(getProducerConfig());  
        
        try {
            for (int id = 1; id < 100; id++) {
                String key = "pubkey12345-" + id;
                UpdateAccountEventOuterClass.UpdateAccountEvent accountUpdateEvent = UpdateAccountEventOuterClass.UpdateAccountEvent.newBuilder()
                    .setSlot(id)
                    .setPubkey(ByteString.copyFrom(key.getBytes()))
                    .setLamports(1000)
                    .setOwner(ByteString.copyFrom(key.getBytes()))
                    .setExecutable(false)
                    .setRentEpoch(id)
                    .setData(ByteString.copyFrom(key.getBytes()))
                    .setWriteVersion(id)
                    .build();
                ProducerRecord<String, UpdateAccountEventOuterClass.UpdateAccountEvent> record = new ProducerRecord<String, UpdateAccountEventOuterClass.UpdateAccountEvent>(ACCOUNTS_UPDATE_TOPIC, key, accountUpdateEvent);
                producer.send(record, new ProducerCallback());
                // producer.send(record);
                System.out.println("producer.send successful...");
            }
        } catch (Exception e) {
            // logger.info("POJO problem publishing record " , e);
            // System.out.println("POJO problem publishing record " + e.getMessage() + e);
            System.out.println("POJO problem publishing record...");
            System.out.println(e.getMessage());
            e.printStackTrace();
        } finally{
            producer.close();
        } 
    }

    public void run() {
        startProducer();
    }

    private class ProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetaData, Exception e){
            if (e == null) {
                System.out.println("Received new metadata. \n" +
                        "Topic:" + recordMetaData.topic() + "\n" +
                        "Partition: " + recordMetaData.partition() + "\n" +
                        "Offset: " + recordMetaData.offset() + "\n" +
                        "Timestamp: " + recordMetaData.timestamp());
            }
            else {
                System.out.println("There's been an error from the Producer side");
                e.printStackTrace();
            }
        }
    }
}