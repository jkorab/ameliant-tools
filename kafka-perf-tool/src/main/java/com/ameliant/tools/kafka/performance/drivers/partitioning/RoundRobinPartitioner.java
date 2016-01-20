package com.ameliant.tools.kafka.performance.drivers.partitioning;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Partitioner that round-robin distributes messages across all cluster partitions for the given topic.
 * This is a naive load-spreading strategy, that does not consider the message keys at all.
 * @author jkorab
 */
public class RoundRobinPartitioner implements Partitioner {

    private final AtomicInteger messageCount = new AtomicInteger(0);

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();

        return messageCount.incrementAndGet() % numPartitions;
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> map) {}
}
