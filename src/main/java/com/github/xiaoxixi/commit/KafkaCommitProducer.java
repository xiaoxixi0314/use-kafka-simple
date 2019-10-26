package com.github.xiaoxixi.commit;

import com.github.xiaoxixi.constants.BizConstants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

public class KafkaCommitProducer {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.98:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        try {
            ProducerRecord<String, String> record;
            for (int i = 0; i < 4; i++) {
                record = new ProducerRecord<>(BizConstants.TOPiC_COMMIT, UUID.randomUUID().toString(), "hello, commit test," + i);
                producer.send(record);
            }
            producer.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
