package com.ameliant.tools.kafkaperf.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author jkorab
 */
public class TestProfileDefinition {

    /**
     * Maximum test duration in seconds. Applies to concurrent tests only.
     */
    private int maxDuration = 30;
    /**
     * Whether producers and consumers execute concurrently. If false, producers will executed before consumers.
     */
    private boolean concurrent = true;

    private Map<String, Object> config = new HashMap<>();
    private ProducersDefinition producers = new ProducersDefinition();
    private ConsumersDefinition consumers = new ConsumersDefinition();

    public boolean isConcurrent() {
        return concurrent;
    }

    public void setConcurrent(boolean concurrent) {
        this.concurrent = concurrent;
    }

    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

    public ConsumersDefinition getConsumers() {
        return consumers;
    }

    public void setConsumers(ConsumersDefinition consumers) {
        this.consumers = consumers;
    }

    public int getMaxDuration() {
        return maxDuration;
    }

    public void setMaxDuration(int maxDuration) {
        this.maxDuration = maxDuration;
    }

    public ProducersDefinition getProducers() {
        return producers;
    }

    public void setProducers(ProducersDefinition producers) {
        this.producers = producers;
    }

}
