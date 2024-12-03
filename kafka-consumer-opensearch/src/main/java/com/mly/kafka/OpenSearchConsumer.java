package com.mly.kafka;

import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.*;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

    public static void main(String[] args) throws IOException {
        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());
        System.out.println("Hello World!");
        //first create opensearch client
        RestHighLevelClient client = getRestHighLevelClient();

        //create index in opensearch if it does not index already
        createIndex(client, log);

        //create kakfa client
        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();
        kafkaConsumer.subscribe(Collections.singleton("wikimedia.recentchange"));

        while(true){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(java.time.Duration.ofMillis(100));
            //poll for new data
            int recordCount = records.count();
            log.info("Received " + recordCount + " records");
            for(ConsumerRecord<String, String> record: records){
                //insert data into opensearch
                log.info("Key: " + record.key() + ", Value: " + record.value());
                IndexRequest indexRequest = new IndexRequest("wikimedia").source(record.value(), XContentType.JSON);
                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                log.info("Id: " + indexResponse.getId());
            }
        }


    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        String bootStrapServer = "127.0.0.1:9092";
        String groupId = "consumer-opensearch";
        //create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        //create consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        return kafkaConsumer;
    }

    private static void createIndex(RestHighLevelClient client, Logger log) throws IOException {
        Boolean indexExists = client.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);
        if(!indexExists) {
            CreateIndexRequest request = new CreateIndexRequest("wikimedia");
            client.indices().create(request, RequestOptions.DEFAULT);
            log.info("wikimedia index has been created.");
        }else{
            log.info("wikimedia index already exists.");
        }
    }

    private static RestHighLevelClient getRestHighLevelClient() {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")));
        return client;
    }
}
