package com.ameliant.tools.kafkaperf.config;

/**
 * @author jkorab
 */
public class KeyAllocationStrategyDefinition {

    private KeyAllocationType type;
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
