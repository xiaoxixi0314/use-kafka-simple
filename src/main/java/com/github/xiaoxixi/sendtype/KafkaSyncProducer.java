package com.github.xiaoxixi.sendtype;

import com.github.xiaoxixi.constants.BizConstants;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Objects;
import java.util.Properties;

/**
 * 异步发送
 */
public class KafkaSyncProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.99:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        try {
            ProducerRecord<String, String> record;
            for (int i = 0; i < 4; i++) {
                record = new ProducerRecord<>(BizConstants.TOPIC_HELLO, String.valueOf(i), "hello" + String.valueOf(i));
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (!Objects.isNull(exception)) {
                            exception.printStackTrace();
                            return;
                        }
                        if (!Objects.isNull(metadata)) {
                            System.out.println("send success");
                            String sendResult = String.format("topic:%s, offset:%d, partition:%d",
                                    metadata.topic(),
                                    metadata.offset(),
                                    metadata.partition());
                            System.out.println("send result:" + sendResult);
                        }
                    }
                });
            }
        }catch (Exception e){

        }finally {
            producer.close();
        }
    }
}
