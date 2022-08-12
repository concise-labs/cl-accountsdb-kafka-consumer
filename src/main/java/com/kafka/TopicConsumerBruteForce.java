package com.kafka;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.lang.Boolean;
import java.lang.Long;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.utils.Bytes;
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

import org.bitcoinj.core.Base58;

public class TopicConsumerBruteForce implements Runnable {
    Logger logger = LoggerFactory.getLogger(TopicConsumerBruteForce.class.getName());

    static String BOOTSTRAP_SERVERS = "kafka-server-endpoint"; // replace with the actual endpoint
    static String GROUP_ID = "test-consumer";  
    
    static String ACCOUNTS_UPDATE_TOPIC = "solana.mainnet.account_updates";
    static String TRANSACTIONS_UPDATE_TOPIC = "solana.mainnet.transaction_updates";


    private Properties getConsumerConfig() {
        Properties properties = new Properties();
    
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());

        //DO NOT COMMIT so we can re-read the same messages
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return properties;
    }

    public void startConsumer() {

        //creating consumer
        KafkaConsumer<Bytes, Bytes> consumer = new KafkaConsumer<Bytes, Bytes>(getConsumerConfig());  
                
        //Subscribing
        Collection<String> topics = Arrays.asList(ACCOUNTS_UPDATE_TOPIC);
        
        consumer.subscribe(topics);
        
        // publish and consume
        
        try {
            while(true){
            //for (int i=0; i<100; i++){
                // System.out.println("String brute force Entered while loop...");
                ConsumerRecords<Bytes, Bytes> records = consumer.poll(Duration.ofMillis(100));
                // System.out.println("Got records....");

                for(ConsumerRecord<Bytes, Bytes> record: records){  
                    // System.out.println("Record Key: " + record.key() + ", Value: " + record.value());
                    // logger.info("Record Key: " + record.key() + ", Value: " + record.value());

                    //try to make it a Dynamic Message [BruteForce!!]
                    byte[] data = record.value().get();
                    DynamicMessage m = DynamicMessage.parseFrom(getDescriptor(), data);
                    AccountUpdateEvent accountUpdateEvent = new AccountUpdateEvent();

                    for (Descriptors.FieldDescriptor field : m.getAllFields().keySet()) {
                        // logger.info(field.getName() + ": " + m.getField(field));
                        System.out.println("Printing key and value... " + field.getName() + ": " + m.getField(field));
                        String parsedField = new String(field.getName());
                        // String parsedValue = ((ByteString) m.getField(field)).toString();

                        switch(parsedField) {
                            case "slot":
                                accountUpdateEvent.setSlot(Long.parseLong(String.valueOf(m.getField(field))));
                                break;
                            case "pubkey":
                                byte[] barray1 = ((ByteString) m.getField(field)).toByteArray();
                                accountUpdateEvent.setPubkey(Base58.encode(barray1));
                                break;
                            case "lamports":
                                accountUpdateEvent.setLamports(Long.parseLong(String.valueOf(m.getField(field))));
                                break;
                            case "owner":
                                byte[] barray2 = ((ByteString) m.getField(field)).toByteArray();
                                accountUpdateEvent.setOwner(Base58.encode(barray2));
                                break;
                            case "executable":
                                accountUpdateEvent.setExecutable(Boolean.parseBoolean(String.valueOf(m.getField(field))));
                                break;
                            case "rentEpoch":
                                accountUpdateEvent.setRentEpoch(Long.parseLong(String.valueOf(m.getField(field))));
                                break;
                            case "data":
                                ByteString dataBS = (ByteString) m.getField(field);
                                accountUpdateEvent.setData(dataBS.toString());
                                accountUpdateEvent.setDataAsBytes(dataBS.toByteArray());
                                break;
                            case "writeVersion":
                                accountUpdateEvent.setWriteVersion(Long.parseLong(String.valueOf(m.getField(field))));
                                break;
                            default:
                                // code block
                        }

                        // if (parsedField.equals("pubkey") || parsedField.equals("owner")) {
                        //     String fieldValueBS = ((ByteString) m.getField(field)).toString();
                        //     byte[] fieldValueBA = ((ByteString) m.getField(field)).toByteArray();
                        //     System.out.println("Printing owner pubkey... " + fieldValueBS + " && " + Base58.encode(fieldValueBA));
                        // } else if (parsedField.equals("data")) {
                        //     ByteString fieldValueBS = (ByteString) m.getField(field);
                        //     byte[] fieldValueBA = fieldValueBS.toByteArray();
                        //     System.out.println("Printing data as bytearray ... " + fieldValueBA);
                        //     System.out.println("Printing data as utf8 string ... " + fieldValueBS.toStringUtf8());
                        //     // for (int i=0; i < fieldValueBA.length; i++) {
                        //     //     System.out.print(fieldValueBA[i]);
                        //     // }
                        //     // byte[] first22bytes = fieldValueBS.substring(0, 22).toByteArray();
                        //     // byte[] second22bytes = fieldValueBS.substring(22, 44).toByteArray();
                        //     // byte[] last22bytes = fieldValueBS.substring(44, 66).toByteArray();
                        //     // System.out.println("Printing first22bytes ... " + first22bytes);
                        //     // System.out.println("Printing second22bytes ... " + second22bytes);
                        //     // System.out.println("Printing last22bytes ... " + last22bytes);
                        // }
                    }
                    System.out.println("Adding the parsed raw data to postgres...");
                    new CLKafkaDBImpl().addRecord(accountUpdateEvent);
                    // logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }
            }
        } catch (Exception e) {
            // logger.info("String brute force Deserialize problem parsing record " , e);
            System.out.println("String brute force problem parsing record " + e.getMessage() + e);
            e.printStackTrace();
        } finally{
            consumer.close();
        }
    }

    public void run() {
        startConsumer();
    }

    private Descriptor getDescriptor() throws Exception {
        InputStream inStream = TopicConsumerBruteForce.class.getClassLoader().getResourceAsStream("proto/UpdateAccountEvent.desc");

        if(inStream != null) {
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
