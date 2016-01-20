package com.ameliant.tools.kafka.perftool.samples;

import com.ameliant.tools.kafka.perftool.config.TestProfileDefinition;
import com.ameliant.tools.support.DirectoryUtils;
import com.ameliant.tools.support.PayloadDetector;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

import java.io.File;
import java.io.IOException;

/**
 * @author jkorab
 */
public class SampleProfileValidationTest {

    @Test
    public void testProfilesValid() {
        // can be run within an IDE or via Maven
        File testDirectory = DirectoryUtils.locateDirectory("src/test/resources/test-profiles");
        assertThat(testDirectory, notNullValue());
        for (File file : testDirectory.listFiles()) {
            ObjectMapper mapper = PayloadDetector.isYamlFile(file.getName()) ? new ObjectMapper(new YAMLFactory())
                : new ObjectMapper();
            TestProfileDefinition testProfileDefinition = null;
            try {
                testProfileDefinition = mapper.readValue(file, TestProfileDefinition.class);
            } catch (IOException e) {
                throw new IllegalArgumentException("Unable to read in " + file.getName(), e);
            }
            assertThat(testProfileDefinition, notNullValue());
        }
    }

}
