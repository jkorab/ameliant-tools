package com.ameliant.tools.kafkaperf.config;

import org.apache.commons.lang.Validate;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for merging Kafka config maps such that the child overrides the parent.
 * @author jkorab
 */
class ConfigMerger {

    /**
     * Merges two maps, with entries in the child overriding those in the parent.
     * @param parent The parent map.
     * @param child The child map.
     * @return A merged map.
     */
    static Map<String, Object> merge(Map<String, Object> parent, Map<String, Object> child) {
        Validate.notNull(parent, "parent is null");
        Validate.notNull(child, "child is null");

        Map<String, Object> merged = new HashMap<>();
        merged.putAll(parent);
        merged.putAll(child);
        return merged;
    }

}
