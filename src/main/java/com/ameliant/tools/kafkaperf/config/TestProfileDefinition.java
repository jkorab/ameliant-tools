package com.ameliant.tools.kafkaperf.config;

import com.fasterxml.jackson.annotation.JsonManagedReference;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author jkorab
 */
public class TestProfileDefinition extends Configurable {

    /**
     * Maximum test duration in seconds. Applies to concurrent tests only.
     */
    private int maxDuration = 30;
    /**
     * Whether producers and consumers execute concurrently. If false, producers will executed before consumers.
     */
    private boolean concurrent = true;

    @JsonManagedReference
    private ProducersDefinition producers = new ProducersDefinition();

    @JsonManagedReference
    private ConsumersDefinition consumers = new ConsumersDefinition();

    public boolean isConcurrent() {
        return concurrent;
    }

    public void setConcurrent(boolean concurrent) {
        this.concurrent = concurrent;
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
