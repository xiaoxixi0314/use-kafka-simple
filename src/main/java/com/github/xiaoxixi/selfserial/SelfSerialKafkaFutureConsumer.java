package com.github.xiaoxixi.selfserial;

import com.alibaba.fastjson.JSON;
import com.github.xiaoxixi.constants.BizConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class SelfSerialKafkaFutureConsumer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.99:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DemoUserDeserializer.class);

        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test2");

        KafkaConsumer<String, DemoUser> consumer = new KafkaConsumer<String, DemoUser>(properties);

        try {
            System.out.println("waiting message...");
            consumer.subscribe(Arrays.asList(BizConstants.TOPIC_USER));
            while (true) {
                Thread.sleep(1000);
                System.out.println("sleep 1s...");
                ConsumerRecords<String, DemoUser> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, DemoUser> record : records) {
                    String consumerResult = String.format("topic:%s, partition:%d, key:%s, value:%s, offset:%d",
                            record.topic(),
                            record.partition(),
                            record.key(),
                            JSON.toJSON(record.value()),
                            record.offset());
                    System.out.println(consumerResult);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
