package com.github.xiaoxixi.selfpartition;

import com.github.xiaoxixi.constants.BizConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class SelfPatitionConsumer {

    public static void main(String[] args) {
        try {
            Properties properties = new Properties();
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.99:9092");
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

            properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test2");

            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Arrays.asList(BizConstants.TOPIC_HELLO));
            System.out.println("waiting message ....");
            while(true) {
                System.out.println("will sleep 1s ...");
                Thread.sleep(1000);
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record: records) {
                    String msg = String.format("key:%s, value:%s, topic:%s, offset:%d", record.key(), record.value(), record.topic(), record.offset());
                    System.out.println(msg);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
