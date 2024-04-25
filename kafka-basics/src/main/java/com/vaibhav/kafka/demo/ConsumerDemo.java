package com.vaibhav.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());


    public static void main(String[] args) {
        logger.info("I am a Kafka Consumer");

        String group_id = "my-java-application-group";
        String topic = "demo_topic";

        //set Kafka properties
        Properties properties = new Properties();

        //localhost Kafka
//        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //Upstash Kafka properties
        properties.setProperty("bootstrap.servers", "thorough-monkfish-9261-us1-kafka.upstash.io:9092");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required" +
                "  username=\"dGhvcm91Z2gtbW9ua2Zpc2gtOTI2MSQzGm7sq3Db5BMhyoVX-u9KwNLT-y39IkE\"" +
                "  password=\"NTg2OGY5YzctMzk1OC00Mzk4LTlmZTUtYmZlNzgwOGZjZWFi\";");

        //set deserializer property for msg key and value
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", group_id);
        properties.setProperty("auto.offset.reset", "latest");

        //create kafka consumer - implements AutoClosable so try with resources is used here
        try (KafkaConsumer<String, String> consumer =
                     new KafkaConsumer<String, String>(properties);) {

            //get a reference to main thread
            final Thread mainThread = Thread.currentThread();

            //Add a shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    logger.info("Detected a shutdown, Let's exit by calling consumer.wakeup()...");
                    consumer.wakeup();

                    //join the main thread to allow execution of the code in the main thread
                    try {
                        mainThread.join();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    } finally {
                        logger.info("Back from main thread");
                    }
                }
            });

            try {
                //subscribe to a topic
                consumer.subscribe(Collections.singleton(topic));

                //poll for data
                while (true) {
                    logger.info("polling for data");

                    ConsumerRecords<String, String> records =
                            consumer.poll(Duration.ofMillis(1000));

                    for (ConsumerRecord record : records) {
                        logger.info("Key: " + record.key() + "Value: " + record.value());
                        logger.info("partition: " + record.partition() + "Offset: " + record.offset());
                    }
                }
            } catch (WakeupException w) {
                logger.info("Consumer is starting to shutdown");
            } catch (Exception e) {
                logger.error("unexpected error in consumer" + e);
            } finally {
                consumer.close(); //close the consumer and commit the offset
                logger.info("The consumer is now gracefully shutdown");
            }

        }


    }
}
