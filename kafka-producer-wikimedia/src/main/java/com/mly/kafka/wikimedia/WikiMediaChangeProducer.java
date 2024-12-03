package com.mly.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikiMediaChangeProducer {

    public static  void main(String[] args) throws InterruptedException {
        System.out.println("Hello World!");

        String bootStrapServer = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootStrapServer);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));

        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        String topic = "wikimedia.recentchange";

        EventHandler eventHandler = new WikiMediaChangeEventHandler(producer,topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        EventSource.Builder builder = new EventSource.Builder( eventHandler,URI.create(url));
        EventSource source = builder.build();

        source.start();

        TimeUnit.MINUTES.sleep(10);

    }
}
