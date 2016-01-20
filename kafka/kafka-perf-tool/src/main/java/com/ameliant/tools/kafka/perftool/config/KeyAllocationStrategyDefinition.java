package com.ameliant.tools.kafka.perftool.config;

/**
 * @author jkorab
 */
public class KeyAllocationStrategyDefinition {

    /**
     * How keys are allocated to each message.
     */
    private KeyAllocationType type;

    /**
     * How many unique keys are used.
     */
    private int uniqueKeys;

    // for Jackson
    public KeyAllocationStrategyDefinition() {}

    public KeyAllocationStrategyDefinition(KeyAllocationType type, int uniqueKeys) {
        this.type = type;
        this.uniqueKeys = uniqueKeys;
    }

    public KeyAllocationType getType() {
        return type;
    }

    public void setType(KeyAllocationType type) {
        this.type = type;
    }

    public int getUniqueKeys() {
        return uniqueKeys;
    }

    public void setUniqueKeys(int uniqueKeys) {
        this.uniqueKeys = uniqueKeys;
    }
}
