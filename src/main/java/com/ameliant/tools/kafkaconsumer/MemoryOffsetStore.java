package com.ameliant.tools.kafkaconsumer;

import org.apache.commons.lang.Validate;
import org.apache.kafka.common.TopicPartition;
import static org.jooq.lambda.tuple.Tuple.tuple;
import org.jooq.lambda.tuple.Tuple2;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author jkorab
 */
public class MemoryOffsetStore implements OffsetStore {

    private ConcurrentHashMap<Tuple2<TopicPartition, String>, Long> topicPartitionGroupOffsets =
            new ConcurrentHashMap<>();

    @Override
    public void markConsumed(TopicPartition topicPartition, String groupId, long offset) {
        Validate.notNull(topicPartition, "topicPartition is null");
        Validate.notEmpty(groupId, "groupId is empty");
        Validate.isTrue(offset >= 0, "offset cannot be negative");
        topicPartitionGroupOffsets.put(tuple(topicPartition, groupId), offset);
    }

    @Override
    public Optional<Long> getLastConsumed(TopicPartition topicPartition, String groupId) {
        Validate.notNull(topicPartition, "topicPartition is null");
        Validate.notEmpty(groupId, "groupId is empty");

        Long lastConsumed = topicPartitionGroupOffsets.get(tuple(topicPartition, groupId));
        return (lastConsumed == null) ? Optional.<Long>empty() : Optional.of(lastConsumed);
    }
}
