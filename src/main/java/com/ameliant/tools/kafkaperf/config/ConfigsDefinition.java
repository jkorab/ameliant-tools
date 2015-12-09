package com.ameliant.tools.kafkaperf.config;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.HashMap;
import java.util.Map;

/**
 * Shared Kafka configuration properties that mean you don't have to keep repeating config
 * for multiple consumers/producers.
 *
 * Configuration properties are overridden in such a way that most specific (lowest-level) config
 * overrides the least specific (global) as follows:
 * <ol>
 *     <li>specific consumer/producer</li>
 *     <li>consumers/producers</li>
 *     <li>global</li>
 * </ol>
 *
 * As such, a config property defined within a specific consumer/producer overrides the global config.
 * @author jkorab
 */
public class ConfigsDefinition {

    private Map<String, Object> global = new HashMap<>();
    private Map<String, Object> consumers = new HashMap<>();
    private Map<String, Object> producers = new HashMap<>();

    public Map<String, Object> getConsumers() {
        return consumers;
    }

    public void setConsumers(Map<String, Object> consumers) {
        this.consumers = consumers;
    }

    public Map<String, Object> getGlobal() {
        return global;
    }

    public void setGlobal(Map<String, Object> global) {
        this.global = global;
    }

    public Map<String, Object> getProducers() {
        return producers;
    }

    public void setProducers(Map<String, Object> producers) {
        this.producers = producers;
    }

    @JsonIgnore
    public Map<String, Object> getConsumerOverGlobal() {
        return ConfigMerger.merge(global, consumers);
    }

    @JsonIgnore
    public Map<String, Object> getProducerOverGlobal() {
        return ConfigMerger.merge(global, producers);
    }

}
