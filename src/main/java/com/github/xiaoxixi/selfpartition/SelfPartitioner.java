package com.github.xiaoxixi.selfpartition;


import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

/**
 * 自定义分区器
 * 可自定义将数据分入到哪个分区
 */
public class SelfPartitioner implements Partitioner {
    /**
     * 实现分区方法
     * @param topic
     * @param key
     * @param keyBytes
     * @param value
     * @param valueBytes
     * @param cluster
     * @return
     */
    @Override
    public int partition(String topic,
                         Object key,
                         byte[] keyBytes,
                         Object value,
                         byte[] valueBytes,
                         Cluster cluster) {

        List<PartitionInfo> partitions =cluster.partitionsForTopic(topic);
        int partitionCount = partitions.size();
        int partition = ((String)value).hashCode()% partitionCount;
        // 返回最终分区
        return partition;
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
