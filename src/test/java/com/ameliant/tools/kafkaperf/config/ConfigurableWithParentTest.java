package com.ameliant.tools.kafkaperf.config;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author jkorab
 */
public class ConfigurableWithParentTest {

    public static final String PROP_1 = "prop1";
    public static final String PROP_2 = "prop2";

    private class TestConfigurable extends ConfigurableWithParent {}

    @Test
    public void testGetMergedConfig_noParent() {
        TestConfigurable child = new TestConfigurable();
        {
            Map<String, Object> config = new HashMap<>();
            config.put(PROP_1, 1);
            child.setConfig(config);
        }
        Map<String, Object> mergedConfig = child.getMergedConfig();
        assertThat(mergedConfig.get(PROP_1), equalTo(1));
        assertThat(mergedConfig.get(PROP_2), nullValue());
    }

    @Test
    public void testGetMergedConfig_withParent() {
        TestConfigurable parent = new TestConfigurable();
        {
            Map<String, Object> config = new HashMap<>();
            config.put(PROP_1, 1);
            config.put(PROP_2, 2);
            parent.setConfig(config);
        }

        TestConfigurable child = new TestConfigurable();
        {
            Map<String, Object> config = new HashMap<>();
            config.put(PROP_1, 3);
            child.setConfig(config);
        }
        child.setParent(parent);

        Map<String, Object> mergedConfig = child.getMergedConfig();
        assertThat(mergedConfig.get(PROP_1), equalTo(3)); // overrides the same value in the parent
        assertThat(mergedConfig.get(PROP_2), equalTo(2));
    }
}
