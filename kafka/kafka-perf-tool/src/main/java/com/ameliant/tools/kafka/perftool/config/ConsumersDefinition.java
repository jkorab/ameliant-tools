package com.ameliant.tools.kafka.perftool.config;

import com.fasterxml.jackson.annotation.JsonManagedReference;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jkorab
 */
public class ConsumersDefinition extends ConfigurableWithParent {

    @JsonManagedReference
    private List<ConsumerDefinition> instances = new ArrayList<>();

    public List<ConsumerDefinition> getInstances() {
        return instances;
    }

    public void setInstances(List<ConsumerDefinition> instances) {
        this.instances = instances;
    }

}
