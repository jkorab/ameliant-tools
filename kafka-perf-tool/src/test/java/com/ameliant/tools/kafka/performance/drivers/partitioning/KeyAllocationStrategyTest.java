package com.ameliant.tools.kafka.performance.drivers.partitioning;

import com.ameliant.tools.kafka.performance.config.KeyAllocationStrategyDefinition;
import com.ameliant.tools.kafka.performance.config.KeyAllocationType;
import org.junit.Test;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

/**
 * @author jkorab
 */
public class KeyAllocationStrategyTest {

    private KeyAllocationStrategy getStrategy(KeyAllocationType type, int uniqueKeys) {
        return new KeyAllocationStrategy(new KeyAllocationStrategyDefinition(type, uniqueKeys));
    }

    @Test
    public void testFairAllocation() {
        KeyAllocationStrategy s0 = getStrategy(KeyAllocationType.fair, 2);
        assertThat(s0.getKey(0), equalTo(s0.getKey(0))); // consistency
        assertThat(s0.getKey(0), equalTo(s0.getKey(2))); // modulo around uniqueKeys
        assertThat(s0.getKey(0), not(equalTo(s0.getKey(1))));

        KeyAllocationStrategy s1 = getStrategy(KeyAllocationType.fair, 2);
        assertThat(s0.getKey(0), equalTo(s1.getKey(0)));
        assertThat(s0.getKey(1), equalTo(s1.getKey(1)));
    }

    @Test
    public void testRandomAllocation() {
        KeyAllocationStrategy s0 = getStrategy(KeyAllocationType.random, 2);
        assertThat(s0.getKey(0), equalTo(s0.getKey(0))); // consistency
        assertThat(s0.getKey(0), equalTo(s0.getKey(2))); // modulo around uniqueKeys
        assertThat(s0.getKey(0), not(equalTo(s0.getKey(1))));

        KeyAllocationStrategy s1 = getStrategy(KeyAllocationType.random, 2);
        assertThat(s0.getKey(0), not(equalTo(s1.getKey(0))));
        assertThat(s0.getKey(1), not(equalTo(s1.getKey(1))));
    }
}