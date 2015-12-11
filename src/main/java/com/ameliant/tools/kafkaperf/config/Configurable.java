package com.ameliant.tools.kafkaperf.config;

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

}
