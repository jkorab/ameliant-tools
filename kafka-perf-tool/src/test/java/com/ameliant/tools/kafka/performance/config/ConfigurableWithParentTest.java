package com.ameliant.tools.kafka.performance.config;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author jkorab
 */
public class ConfigurableWithParentTest {

    public static final String PROP_0 = "prop0";
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
        Map<String, Object> mergedConfig = child.getKafkaConfig();
        assertThat(mergedConfig.get(PROP_1), equalTo(1));
        assertThat(mergedConfig.get(PROP_2), nullValue());
    }

    @Test
    public void testGetMergedConfig_withParent() {
        TestConfigurable l0 = new TestConfigurable();
        {
            Map<String, Object> config = new HashMap<>();
            config.put(PROP_1, 1);
            config.put(PROP_2, 2);
            l0.setConfig(config);
        }

        TestConfigurable l1 = new TestConfigurable();
        {
            Map<String, Object> config = new HashMap<>();
            config.put(PROP_1, 3);
            l1.setConfig(config);
        }
        l1.setParent(l0);

        Map<String, Object> mergedConfig = l1.getKafkaConfig();
        assertThat(mergedConfig.get(PROP_1), equalTo(3)); // overrides the same value in the parent
        assertThat(mergedConfig.get(PROP_2), equalTo(2));
    }


    @Test
    public void testGetMergedConfig_with2LevelsParent() {
        TestConfigurable l0 = new TestConfigurable();
        {
            Map<String, Object> config = new HashMap<>();
            config.put(PROP_0, 0);
            l0.setConfig(config);
        }

        TestConfigurable l1 = new TestConfigurable();
        l1.setParent(l0);
        {
            Map<String, Object> config = new HashMap<>();
            config.put(PROP_1, 1);
            config.put(PROP_2, 2);
            l1.setConfig(config);
        }

        TestConfigurable l2 = new TestConfigurable();
        l2.setParent(l1);
        {
            Map<String, Object> config = new HashMap<>();
            config.put(PROP_1, 3);
            l2.setConfig(config);
        }

        Map<String, Object> mergedConfig = l2.getKafkaConfig();
        assertThat(mergedConfig.get(PROP_0), equalTo(0));
        assertThat(mergedConfig.get(PROP_1), equalTo(3)); // overrides the same value in the parent
        assertThat(mergedConfig.get(PROP_2), equalTo(2));
    }
}
