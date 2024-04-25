package com.vaibhav.kafka.demo;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallbacks {

    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallbacks.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("hello world!");

        //set Kafka properties
        Properties properties = new Properties();

        //localhost Kafka
//        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //Upstash Kafka properties
        properties.setProperty("bootstrap.servers", "thorough-monkfish-9261-us1-kafka.upstash.io:9092");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required"
                + "  username=\"dGhvcm91Z2gtbW9ua2Zpc2gtOTI2MSQzGm7sq3Db5BMhyoVX-u9KwNLT-y39IkE\""
                + "  password=\"NTg2OGY5YzctMzk1OC00Mzk4LTlmZTUtYmZlNzgwOGZjZWFi\";");

        //set serializer property for msg key and value
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("compression.type", "snappy");
        //properties.setProperty("batch.size", "400");
        //properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        //create the KafkaProducer class
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create producer record - msg to be sent
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_topic1",3,"key1", "hello world");

        //send the data - Asynchronous
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    System.out.println("Topic Name:" + recordMetadata.topic()
                                    + "\nPartition:" + recordMetadata.partition()
                                    + "\nOffset:" + recordMetadata.offset()
                                    + "\ntimestamp:" + recordMetadata.timestamp());
                }
                else {
                    System.out.println("error has occurred" + e);
                }
            }
        });

        //tell the producer to send all data and block - synchronous
        producer.flush();

        //flush and close the producer
        producer.close();

    }
}
