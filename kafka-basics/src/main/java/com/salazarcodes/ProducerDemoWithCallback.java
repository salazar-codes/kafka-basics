package com.salazarcodes;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        System.out.println("Hello World!!");

        // create Producer Properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:9092"); // connect to localhost

        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());

        // Partitioner
        properties.setProperty("batch:size","400");
        // properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,"1"); - Work around for send to different partitions
        // properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j = 0; j < 10; j++) {

            for (int i = 0; i < 30; i++) {
                // create a Producer record
                ProducerRecord<String,String> producerRecord = new ProducerRecord<>("demo_java","Hello world" + i);

                // send data - asynchronous
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        // executes every time a record successfully sent or an exception is thrown
                        if(exception == null){
                            log.info("Received new metadata \n" +
                                    "Topic: " + metadata.topic() + "\n" +
                                    "Partition:"+ metadata.partition() +"\n" +
                                    "Offset:" +  metadata.offset() +"\n" +
                                    "Timestamp: " + metadata.timestamp());
                        }else{
                            log.error("Error while producing", exception);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }

        // tell the producer to send all data and block until done -- synchronous
        producer.flush();

        // flush and close the producer - call flush too
        producer.close();
    }
}
