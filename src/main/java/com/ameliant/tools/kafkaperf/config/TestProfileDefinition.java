package com.ameliant.tools.kafkaperf.config;

import java.util.ArrayList;
import java.util.List;

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

    private ConfigsDefinition configs = new ConfigsDefinition();

    private List<ProducerDefinition> producers = new ArrayList<>();
    private List<ConsumerDefinition> consumers = new ArrayList<>();

    public List<ConsumerDefinition> getConsumers() {
        return consumers;
    }

    public void setConsumers(List<ConsumerDefinition> consumers) {
        this.consumers = consumers;
    }

    public List<ProducerDefinition> getProducers() {
        return producers;
    }

    public void setProducers(List<ProducerDefinition> producers) {
        this.producers = producers;
    }

    public int getMaxDuration() {
        return maxDuration;
    }

    public void setMaxDuration(int maxDuration) {
        this.maxDuration = maxDuration;
    }

    public ConfigsDefinition getConfigs() {
        return configs;
    }

    public void setConfigs(ConfigsDefinition configs) {
        this.configs = configs;
    }

    public boolean isConcurrent() {
        return concurrent;
    }

    public void setConcurrent(boolean concurrent) {
        this.concurrent = concurrent;
    }
}
