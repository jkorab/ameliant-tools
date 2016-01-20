package com.ameliant.tools.kafka.perftool.config;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author jkorab
 */
public abstract class Configurable {

    private Map<String, Object> config = new HashMap<>();

    private String topic;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

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
