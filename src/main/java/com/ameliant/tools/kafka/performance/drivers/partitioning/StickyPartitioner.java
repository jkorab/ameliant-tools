package com.ameliant.tools.kafka.performance.drivers.partitioning;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import static org.apache.kafka.common.utils.Utils.murmur2;

/**
 * Partitioner that assigns partitions in a round-robin fashion to keys across all cluster partitions for
 * the given topic. All messages after the first one for that unique key will be assigned to the same partition.
 * Messages without keys will be sent to the 0th partition.
 * When broker partitions are:
 * <ul>
 *     <li>added - the strategy will add new partitions into rotation for new keys</li>
 *     <li>removed - keys for the missing partition will be reassigned a new partition</li>
 * </ul>
 *
 * BE AWARE:
 * Partitioning is based on round-robin sequence and message send time. This will cause problems when
 * there are two nodes running at the same time with this strategy.
 *
 * @author jkorab
 */
public class StickyPartitioner implements Partitioner {

    private final AtomicInteger uniqueKeyCount = new AtomicInteger(0);
    private final Map<Object, Integer> partitionMap = new LinkedHashMap<>();

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if ((keyBytes == null) || (keyBytes.length == 0)) {
            return 0;
        }

        int keyHash = Math.abs(murmur2(keyBytes));

        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();

        Integer partition = null;
        if (partitionMap.containsKey(keyHash)) {
            partition = partitionMap.get(keyHash);
            if (partition >= numPartitions) {
                // partition existed, but was removed
                partition = null;
            }
        }

        if (partition == null) {
            partition = uniqueKeyCount.incrementAndGet() % numPartitions;
            partitionMap.put(keyHash, partition);
        }
        return partition;
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> map) {}
}
