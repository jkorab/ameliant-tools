package com.ameliant.tools.kafkaperf.config;

import com.fasterxml.jackson.annotation.JsonManagedReference;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jkorab
 */
public class ProducersDefinition extends ConfigurableWithParent {

    @JsonManagedReference
    private List<ProducerDefinition> instances = new ArrayList<>();

    public List<ProducerDefinition> getInstances() {
        return instances;
    }

    public void setInstances(List<ProducerDefinition> instances) {
        this.instances = instances;
    }

}
