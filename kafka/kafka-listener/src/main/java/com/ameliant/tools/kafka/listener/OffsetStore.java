package com.ameliant.tools.kafka.listener;

import org.apache.kafka.common.TopicPartition;

import java.util.Optional;

/**
 * An offset store maintains the last successfully read position of topic partitions.
 * Use of an offset store addresses the problem of keeping track of previously consumed messages in a batch
 * polling scenario, where the cursor for the message group may need to be rewound on system restart.
 *
 * There is some overlap with an idempotent store as both keep track of previously seen messages.
 * The critical difference being that the latter does not contain enough information to allow a cursor rewind.
 *
 * TODO integrating the two. Processing messages polled, when one of the the partitions is rebalanced will cause issues.
 * TODO determine whether messages are polled from multiple partitions at the same time.
 *
 * It is used as a pessimistic store, keeping track of the last message to have been processed - not fetched.
 * The two constructs vary as n messages may have been fetched via a poll, but are only then sequentially
 * processed. The cursor for the consumer group, in the mean-time has already been moved forward by n places.
 *
 * On consumer startup, when partitions are allocated, the last successfully position for the consumer's group id
 * is fetched and the cursor rewound back to it.
 *
 * @author jkorab
 */
public interface OffsetStore {

    void markConsumed(TopicPartition topicPartition, String groupId, long offset);

    Optional<Long> getLastConsumed(TopicPartition topicPartition, String groupId);

}
