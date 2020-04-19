package com.github.ygannyu.simplekafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemo {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

        // create Producer properties
        Properties properties = new Properties();
        String bootstrapServers = "127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            // create a producer record
            String topic = "first_topic";
            String value = "hello world" + i;
            String key = "id_" + i;

            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            logger.info("Key: " + key); // log the key
            // send data - asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // execute every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        logger.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            }).get(); // block the .send() to make it synchronous - don't do it in prod
        }

        producer.flush();
        producer.close();
    }
}
