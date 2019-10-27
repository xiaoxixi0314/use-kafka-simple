package com.github.xiaoxixi.independconsumer;

import com.github.xiaoxixi.constants.BizConstants;
import com.github.xiaoxixi.selfpartition.SelfPartitioner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


/**
 * 使用自定义分区器将数据分区
 */
public class IndependProducer {

    public static void main(String[] args) {

        try{
            Properties properties = new Properties();
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.99:9092");
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

            ProducerRecord<String, String> record;
            for (int i = 0; i < 5; i++) {
                record = new ProducerRecord<>(BizConstants.TOPIC_INDEPEND_CONSUMER, String.valueOf(i), "self partition" + i);
                producer.send(record);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
