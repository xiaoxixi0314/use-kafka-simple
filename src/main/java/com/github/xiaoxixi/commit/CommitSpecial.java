package com.github.xiaoxixi.commit;

import com.github.xiaoxixi.constants.BizConstants;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 手动和异步同时使用，最大限度的保证成功提交偏移量
 */
public class CommitSpecial {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.99:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // 禁止自动提交，由客户端手动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        try {
            int count = 0;
            consumer.subscribe(Collections.singleton(BizConstants.TOPiC_COMMIT));
            Map<TopicPartition, OffsetAndMetadata> currentOffset;
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    // do your biz work
                    currentOffset = new HashMap<>();
                    currentOffset.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1, "no meta"));
                    count++;
                    // 每消费11条提交一次
                    if (count % 11 == 0) {
                        consumer.commitAsync(currentOffset, null);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                consumer.commitSync();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                consumer.close();
            }
        }
    }
}
