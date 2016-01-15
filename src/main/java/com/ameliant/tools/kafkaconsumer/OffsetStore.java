package com.ameliant.tools.kafkaconsumer;

import org.apache.kafka.common.TopicPartition;

/**
 * An offset store maintains the last successfully read position of topic partitions.
 *
 * You may have multiple consumer groups, each consuming from one or more topic partitions.
 *
 * @author jkorab
 */
public interface OffsetStore {

    void markConsumed(TopicPartition topicPartition, String groupId, long offset);

    long getLastConsumed(TopicPartition topicPartition, String groupId);

}
