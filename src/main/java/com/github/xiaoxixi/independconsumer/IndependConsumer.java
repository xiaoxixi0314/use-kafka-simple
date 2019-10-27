package com.github.xiaoxixi.independconsumer;

import com.github.xiaoxixi.constants.BizConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 独立消费者，
 * 没有群组的消费者
 */
public class IndependConsumer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.99:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // 拿到主题所有的分区信息，独立消费者不需要订阅主题，只需要分配主题中的分区即可
        List<PartitionInfo> partitions = consumer.partitionsFor(BizConstants.TOPIC_INDEPEND_CONSUMER);

        List<TopicPartition> listNeedHandlePartition = new ArrayList<>();

        for (PartitionInfo partition : partitions) {
            if (partition == null) {
                continue;
            }
            listNeedHandlePartition.add(new TopicPartition(partition.topic(), partition.partition()));
        }
        // 这里订阅了所有的分区
        consumer.assign(listNeedHandlePartition);
        System.out.println("waitting message.....");
        try {
            ConsumerRecords<String,String> records;
            while (true) {
                records = consumer.poll(Duration.ofMillis(500));
                Thread.sleep(1000);
                System.out.println("sleep 1s...");
                for(ConsumerRecord<String, String> record: records) {
                    String logMessage = String.format("topic:%s, partition:%d, offset:%d, key:%s, value:%s",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    System.out.println(logMessage);
                }
            }

        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
