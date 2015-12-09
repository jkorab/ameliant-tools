package com.ameliant.tools.kafkaperf.config;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jkorab
 */
public class TestProfileDefinition {

    private int maxDuration = 30; // seconds

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
}
