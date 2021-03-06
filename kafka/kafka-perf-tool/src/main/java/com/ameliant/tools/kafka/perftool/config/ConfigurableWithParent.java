package com.ameliant.tools.kafka.perftool.config;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang.Validate;

import java.util.HashMap;
import java.util.Map;

/**
 * @author jkorab
 */
public abstract class ConfigurableWithParent extends Configurable {

    private Configurable parent;

    @JsonIgnore
    private Map<String, Object> _mergedConfig; // lazily initialised

    public Configurable getParent() {
        return parent;
    }

    @JsonBackReference
    public void setParent(Configurable parent) {
        this.parent = parent;
    }

    /**
     * Get the merged config of this object and its parent. This method caches the result, so any changes to this config will not show up.
     * @return The merged config of this object and its parent.
     */
    @Override
    public Map<String, Object> getKafkaConfig() {
        Map<String, Object> config = getConfig();
        if (parent == null) {
            return config;
        }

        if (_mergedConfig == null) { // lazy init
            _mergedConfig = merge(parent.getKafkaConfig(), config);
        }
        return _mergedConfig;
    }

    /**
     * Merges two maps, with entries in the child overriding those in the parent.
     * @param parentConfig The parent map.
     * @param childConfig The child map.
     * @return A merged map.
     */
    private Map<String, Object> merge(Map<String, Object> parentConfig, Map<String, Object> childConfig) {
        Validate.notNull(parentConfig, "parent is null");
        Validate.notNull(childConfig, "child is null");

        Map<String, Object> merged = new HashMap<>();
        merged.putAll(parentConfig);
        merged.putAll(childConfig);
        return merged;
    }

    @Override
    public String getTopic() {
        return ((parent == null) || (parent.getTopic() == null))
                ? super.getTopic() : parent.getTopic();
    }

}
