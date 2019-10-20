package com.github.xiaoxixi.hello;

import com.alibaba.fastjson.JSON;
import com.github.xiaoxixi.constants.BizConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class HelloKafkaConsumer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.99:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // 消费群组
        // value 必须和kafka中的consumer.properties保持一致
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        try {
            System.out.println("waiting receive message.....");
            consumer.subscribe(Collections.singleton(BizConstants.TOPIC_HELLO));
            while(true) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                System.out.println("record's size:" + records.count());
                for (ConsumerRecord<String, String> record: records) {
                    String receiveMsg = String.format("received message offset:%d, topic:%s, partition:%d, key:%s, value:%s",
                            record.offset(), record.topic(), record.partition(), record.key(), record.value());
                    System.out.println(receiveMsg);
                }
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
