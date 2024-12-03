package com.mly.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikiMediaChangeEventHandler implements EventHandler {

    KafkaProducer<String,String> producer ;
    String topic;

    private final Logger log = LoggerFactory.getLogger(WikiMediaChangeEventHandler.class.getSimpleName());

    public WikiMediaChangeEventHandler(KafkaProducer<String, String> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }
    @Override
    public void onOpen() throws Exception {

    }

    @Override
    public void onClosed()  {
        producer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent)  {
        log.info("Message received: {}", messageEvent.getData());
        producer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String s) throws Exception {

    }

    @Override
    public void onError(Throwable throwable) {
        log.error("Error occurred: {}", throwable.getMessage());
    }
}
