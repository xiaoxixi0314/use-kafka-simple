package com.github.xiaoxixi.configconsumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConfigConsumer {

    public static void main(String[] args) {

        try {
            Properties properties = new Properties();
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.99:9092");
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

            properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test1");

            // 从没有消费过的偏移量开始消费，即最新的没有消费过的偏移量开始， 默认latest
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            // 消费者是否默认提交偏移量，默认true, 实际中会设置成false，业务处理完成后手动提交
            properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
            // 控制每次poll方法返回最大记录的数量，默认500
            properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
            // 分区分配给消费者的策略，默认为range（将主题分区按消费者数量均匀分组，每组分给不同的消费者）， 另外一种RoundRobin（平均分）
            properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, Collections.singleton(RangeAssignor.class));


            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for(ConsumerRecord<String, String> record : records) {
                    String value = record.value();
                    String key = record.value();
                    String topic = record.topic();
                    int partition = record.partition();
                    long offset = record.offset();
                    System.out.println(value);
                }
            }



        } catch (Exception e) {

        }


    }
}
