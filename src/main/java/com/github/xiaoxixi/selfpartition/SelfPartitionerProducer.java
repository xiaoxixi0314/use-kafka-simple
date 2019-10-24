package com.github.xiaoxixi.selfpartition;

import com.github.xiaoxixi.constants.BizConstants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


/**
 * 使用自定义分区器将数据分区
 */
public class SelfPartitionerProducer {

    public static void main(String[] args) {

        try{
            Properties properties = new Properties();
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.99:9092");
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

            // 使用自定义分区器
            properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, SelfPartitioner.class);

            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

            ProducerRecord<String, String> record;
            for (int i = 0; i < 5; i++) {
                record = new ProducerRecord<>(BizConstants.TOPIC_HELLO, String.valueOf(i), "self partition" + i);
                producer.send(record);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
