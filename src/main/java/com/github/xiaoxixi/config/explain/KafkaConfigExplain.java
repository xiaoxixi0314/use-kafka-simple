package com.github.xiaoxixi.config.explain;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaConfigExplain {

    public static void main(String[] args) {
        Properties properties = new Properties();

        // 必须属性
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.99:9092");
        // key value 序列化器
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // 重要属性
        // 0|1|all
        // 0:发送即忘记，不管发送结果，
        // 1：leader分区是否收到，默认值，
        // all：首领分区收到并且所有副本分区已经成功写入才会返回返送成功
        properties.put(ProducerConfig.ACKS_CONFIG, "0");
        // 一个批次可使用的内存大小，默认16384（16k），批次发送时最大大小，超过这个大小必须发送了
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        // 批次等待时间，默认0，来一条就发一条
        // 设置成50就是等待50ms后聚成一批后批次发送，一般和BATCH_SIZE_CONFIG配合使用
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 50);
        // 生产者发送消息的最大大小，默认1M
        properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 1 *1024*1024);

        // 缓存区大小
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 32*1024*1024);
        // 重试次数，默认是Integer.MAX_VALUE
        properties.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        // 阻塞最大时间，超时则会抛出异常，默认60s
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60 * 1000);


        // 通常使用同步发送+此参数用来保证消息的顺序，设置成1表示生产者尝试发送一批消息时，
        // 就不会有其他消息发送给broker， 可以理解为此时此生产者独占broker
        // 这个属性的默认值是5， 但设置成1会严重影响生产者的性能
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

    }
}
