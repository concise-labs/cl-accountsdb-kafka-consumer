package com.kafka;

import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.ProtocolStringList;

import software.amazon.awssdk.services.glue.model.DataFormat;

public class TopicConsumerDynamicMessage {
    Logger logger = LoggerFactory.getLogger(TopicConsumerDynamicMessage.class.getName());

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

        properties.setProperty(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.PROTOBUF.name());
        properties.setProperty(AWSSchemaRegistryConstants.AWS_REGION, "region");
        properties.setProperty(AWSSchemaRegistryConstants.REGISTRY_NAME, "registry-name");
        properties.setProperty(AWSSchemaRegistryConstants.SCHEMA_NAME, "UpdateAccountEvent.proto");  
        properties.setProperty(AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE, ProtobufMessageType.DYNAMIC_MESSAGE.getName());

        //DO NOT COMMIT so we can re-read the same messages
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return properties;
    }

    public void startConsumer(){

        //creating consumer
        KafkaConsumer<String, DynamicMessage> consumer = new KafkaConsumer<String, DynamicMessage>(getConsumerConfig());  
        
        //Subscribing
        Collection<String> topics = Arrays.asList(ACCOUNTS_UPDATE_TOPIC);
        
        consumer.subscribe(topics);
        
        // publish and consume
        
        try {
            getDescriptor();
        } catch (Exception e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
            System.out.println(e1.getMessage());
            
        }        

        try {
            while(true){
            //for (int i=0; i<100; i++){
                System.out.println("DynamicRecord Entered while loop...");  
                ConsumerRecords<String, DynamicMessage> records = consumer.poll(Duration.ofMillis(100));
                System.out.println("Got records....");

                for (final ConsumerRecord<String, DynamicMessage> record : records) {
                    for (Descriptors.FieldDescriptor field : record.value().getAllFields().keySet()) {
                        logger.info(field.getName() + ": " + record.value().getField(field));
                        System.out.println(field.getName() + ": " + record.value().getField(field));
                    }
                }
            }
        } catch (Exception e) {
            logger.info("DynamicRecord problem parsing record " , e);
            System.out.println("DynamicRecord problem parsing record " + e.getMessage() + e);
        } finally{
            consumer.close();
        } 


    }

    private Descriptor getDescriptor() throws Exception {
        InputStream inStream = TopicConsumerDynamicMessage.class.getClassLoader().getResourceAsStream("proto/events.desc");

        if(inStream != null){
            System.out.println("Stream .. is not null");

        }

        DescriptorProtos.FileDescriptorSet fileDescSet = DescriptorProtos.FileDescriptorSet.parseFrom(inStream);
        Map<String, DescriptorProtos.FileDescriptorProto> fileDescProtosMap = new HashMap<String, DescriptorProtos.FileDescriptorProto>();
        List<DescriptorProtos.FileDescriptorProto> fileDescProtos = fileDescSet.getFileList();
        for (DescriptorProtos.FileDescriptorProto fileDescProto : fileDescProtos) {
            fileDescProtosMap.put(fileDescProto.getName(), fileDescProto);
        }
        DescriptorProtos.FileDescriptorProto fileDescProto = fileDescProtosMap.get("events.proto");
        FileDescriptor[] dependencies = getProtoDependencies(fileDescProtosMap, fileDescProto);
        FileDescriptor fileDesc = FileDescriptor.buildFrom(fileDescProto, dependencies);
        Descriptor desc = fileDesc.findMessageTypeByName("UpdateAccountEvent");
        return desc;
    }
    
    public static FileDescriptor[] getProtoDependencies(Map<String, FileDescriptorProto> fileDescProtos, 
                      FileDescriptorProto fileDescProto) throws Exception {
    
        if (fileDescProto.getDependencyCount() == 0)
            return new FileDescriptor[0];
    
        ProtocolStringList dependencyList = fileDescProto.getDependencyList();
        String[] dependencyArray = dependencyList.toArray(new String[0]);
        int noOfDependencies = dependencyList.size();
    
        FileDescriptor[] dependencies = new FileDescriptor[noOfDependencies];
        for (int i = 0; i < noOfDependencies; i++) {
            FileDescriptorProto dependencyFileDescProto = fileDescProtos.get(dependencyArray[i]);
            FileDescriptor dependencyFileDesc = FileDescriptor.buildFrom(dependencyFileDescProto, 
                             getProtoDependencies(fileDescProtos, dependencyFileDescProto));
            dependencies[i] = dependencyFileDesc;
        }
        return dependencies;
    }
    
}
