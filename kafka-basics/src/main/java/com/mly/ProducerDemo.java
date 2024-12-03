package com.mly;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) throws InterruptedException {

        log.info("Hello world!");

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size","400");

        //create the producer

        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        //create producer record
        //loop this statement 10 times to send 10 messages
        for(int j=0;j<30 ;j++){
            for(int i=0; i<10; i++) {
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world! " + i);

                //send data

                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        //executes every time a record is successfully sent or an exception is thrown
                        if(e == null){
                            //record was successfully sent
                            log.info("Receieved a new metadata \n"+
                                    "Topic: "+recordMetadata.topic()+"\n"+
                                    "Partition: "+recordMetadata.partition()+"\n"+
                                    "Offset: "+recordMetadata.offset()+"\n"+
                                    "Timestamp: "+recordMetadata.timestamp());
                        }
                    }
                });
            }
        }


        //flush and close the producer
        producer.flush();

        producer.close();
    }
}
