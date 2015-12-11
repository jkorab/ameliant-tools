package com.ameliant.tools.kafkaperf.config;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
