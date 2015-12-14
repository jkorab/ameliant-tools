package com.ameliant.tools.kafkaperf.config;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author jkorab
 */
public abstract class Configurable {

    private Map<String, Object> config = new HashMap<>();

    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

    /**
     * Decorateable method for obtaining the provided config.
     * @return The config for this object.
     */
    @JsonIgnore
    public Map<String, Object> getKafkaConfig() {
        return Collections.unmodifiableMap(config);
    }

}
