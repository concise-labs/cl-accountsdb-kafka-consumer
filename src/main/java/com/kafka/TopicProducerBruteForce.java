package com.kafka;

import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.ProtocolStringList;

public class TopicProducerBruteForce implements Runnable {
    Logger logger = LoggerFactory.getLogger(TopicProducerBruteForce.class.getName());

    static String BOOTSTRAP_SERVERS = "kafka-server-endpoint"; // replace with the actual endpoint
    
    static String ACCOUNTS_UPDATE_TOPIC = "solana.mainnet.account_updates";
    static String TRANSACTIONS_UPDATE_TOPIC = "solana.mainnet.transaction_updates";


    private Properties getProducerConfig(){
        Properties properties = new Properties();
    
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }

    public void startProducer() throws Exception {

        //creating producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(getProducerConfig());

        Descriptor desc = getDescriptor();
        
        // publish
        try {
            for (long id = 1; id < 100; id++) {
                String key = "pubkey12345-" + id;
                DynamicMessage accountUpdateEvent = DynamicMessage.newBuilder(desc)
                    .setField(desc.findFieldByName("slot"), id)
                    .setField(desc.findFieldByName("pubkey"), ByteString.copyFrom(key.getBytes()))
                    .setField(desc.findFieldByName("lamports"), 1000L)
                    .setField(desc.findFieldByName("owner"), ByteString.copyFrom(key.getBytes()))
                    .setField(desc.findFieldByName("executable"), false)
                    .setField(desc.findFieldByName("rent_epoch"), id)
                    .setField(desc.findFieldByName("data"), ByteString.copyFrom(key.getBytes()))
                    .setField(desc.findFieldByName("write_version"), id)
                    .build();
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(ACCOUNTS_UPDATE_TOPIC, key, new String(accountUpdateEvent.toByteArray()));
                producer.send(record, new ProducerCallback());
                System.out.println("producer.send successful...");
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            System.out.println("POJO problem publishing record...");
            System.out.println(e.getMessage());
            e.printStackTrace();
        } finally{
            producer.close();
        }
    }

    public void run() {
        try {
            startProducer();
        } catch (Exception e) {
            e.printStackTrace();
        }
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

    private Descriptor getDescriptor() throws Exception {
        InputStream inStream = TopicProducerBruteForce.class.getClassLoader().getResourceAsStream("proto/UpdateAccountEvent.desc");

        if(inStream != null){
            System.out.println("Stream .. is not null");

        }

        DescriptorProtos.FileDescriptorSet fileDescSet = DescriptorProtos.FileDescriptorSet.parseFrom(inStream);
        Map<String, DescriptorProtos.FileDescriptorProto> fileDescProtosMap = new HashMap<String, DescriptorProtos.FileDescriptorProto>();
        List<DescriptorProtos.FileDescriptorProto> fileDescProtos = fileDescSet.getFileList();
        for (DescriptorProtos.FileDescriptorProto fileDescProto : fileDescProtos) {
            fileDescProtosMap.put(fileDescProto.getName(), fileDescProto);
        }
        DescriptorProtos.FileDescriptorProto fileDescProto = fileDescProtosMap.get("UpdateAccountEvent.proto");
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
