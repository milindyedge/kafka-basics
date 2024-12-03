package com.mly;

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class);
    public static void main(String[] args) {
        log.info("I am a Kafka consumer!");
        String groupId = "my-java-application";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("group.id",groupId);

        properties.setProperty("auto.offset.reset","earliest");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(List.of("demo_java"));

        while(true){
            log.info("Polling");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            records.forEach(record -> {
                log.info("Key: "+record.key()+" Value: "+record.value());
                log.info("Partition: "+record.partition()+" Offset: "+record.offset());
            });
        }

    }
}
