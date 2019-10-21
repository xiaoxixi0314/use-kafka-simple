package com.github.xiaoxixi.selfserial;

import com.github.xiaoxixi.constants.BizConstants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * 同步发送方式
 * Future get阻塞
 */
public class SelfSerialKafkaFutureProducer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.99:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DemoUserSerializer.class);


        KafkaProducer<String, DemoUser> producer = new KafkaProducer<>(properties);
        try {
            ProducerRecord<String, DemoUser> record;
            DemoUser user;
            for (int i=0; i < 4; i++) {
                user = new DemoUser();
                user.setId(i);
                user.setName("xiaoxixi" + i);
                record = new ProducerRecord<>(BizConstants.TOPIC_USER, String.valueOf(i), user);
                Future<RecordMetadata> result = producer.send(record);
                RecordMetadata metadata = result.get(); // 同步发送，阻塞在获取结果这里
                if (!Objects.isNull(metadata)) {
                    String sendResult = String.format("topic:%s, offset:%d, partition:%d",
                            metadata.topic(),
                            metadata.offset(),
                            metadata.partition());
                    System.out.println("send result:" + sendResult);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }

    }


}
