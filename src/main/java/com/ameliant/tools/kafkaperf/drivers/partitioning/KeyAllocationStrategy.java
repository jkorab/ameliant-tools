package com.ameliant.tools.kafkaperf.drivers.partitioning;

import com.ameliant.tools.kafkaperf.config.KeyAllocationStrategyDefinition;
import com.ameliant.tools.kafkaperf.config.KeyAllocationType;
import org.apache.commons.lang.Validate;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Assigns keys based on a message number. The set of keys is generated at object instantiation, and may be either
 * {@see KeyAllocationType.fair} where it will be the same every time, or {@see KeyAllocationType.random} where each
 * instance of this class will contain a different set of keys.
 * @author jkorab
 */
public class KeyAllocationStrategy {

    private final KeyAllocationType keyAllocationType;
    protected final int uniqueKeys;
    protected final List<byte[]> keys = new ArrayList<>();

    public KeyAllocationStrategy(KeyAllocationStrategyDefinition strategyDefinition) {
        uniqueKeys = strategyDefinition.getUniqueKeys();
        Validate.isTrue(uniqueKeys > 0, "uniqueKeys must be greater than 0");

        keyAllocationType = strategyDefinition.getType();
        Validate.notNull(keyAllocationType, "keyAllocationType is null");

        Random random = new Random();
        for (int i = 0; i < this.uniqueKeys; i++) {
            if (keyAllocationType == KeyAllocationType.fair) {
                keys.add(asByteArray(i));
            } else {
                keys.add(asByteArray(random.nextInt()));
            }
        }
    }

    @Override
    public String toString() {
        return "KeyAllocationStrategy{" +
                "keyAllocationType=" + keyAllocationType +
                ", uniqueKeys=" + uniqueKeys +
                '}';
    }

    /**
     * Convert the int into an array of two bytes.
     */
    private byte[] asByteArray(int i) {
        byte _0 = (byte) (i & 0xFF);
        byte _1 = (byte) ((i >> 8) & 0xFF);
        return new byte[]{_0, _1};
    }

    /**
     * Gets a key for a message number. The key is guaranteed to be the same for this object instance every time
     * this method is called.
     * @param messageNum The message number.
     * @return A message key.
     */
    public byte[] getKey(int messageNum) {
        return keys.get(messageNum % uniqueKeys);
    }

}
