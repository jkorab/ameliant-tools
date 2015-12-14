package com.ameliant.tools.kafkaperf;

import com.ameliant.tools.kafkaperf.config.TestProfileDefinition;
import com.ameliant.tools.kafkaperf.drivers.TestProfileRunner;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Performance test main class. The tool allows you to run multiple consumers and producers
 * at the same time against one or more Kafka clusters.
 *
 * This class takes a JSON config file location as an argument, which it translates into an
 * object graph representing the test profile ({@link com.ameliant.tools.kafkaperf.config.TestProfileDefinition}).
 *
 * The test profile is then executed by a {@link com.ameliant.tools.kafkaperf.drivers.TestProfileRunner},
 * which runs each of the producers and consumers in their own threads.
 *
 * @author jkorab
 */
public class KafkaPerf {

    private final static Logger LOG = LoggerFactory.getLogger(KafkaPerf.class);

    public static void main(String[] args) {
        CommandLineParser parser = new DefaultParser();

        Options options = new Options();
        options.addOption(Option.builder("c")
                .longOpt("config")
                .desc("config file that defines the test profile(s) to run")
                .hasArg()
                .argName("FILE")
                .required(true)
                .build());

        boolean displayHelp = false;
        String errorMessage = null;

        try {
            String config = parser.parse(options, args).getOptionValue("config");
            try {
                ObjectMapper mapper = isYamlFile(config) ? new ObjectMapper(new YAMLFactory())
                    : new ObjectMapper();
                TestProfileDefinition testProfileDefinition = mapper.readValue(new File(config), TestProfileDefinition.class);
                TestProfileRunner testProfileRunner = new TestProfileRunner(testProfileDefinition);

                if (LOG.isDebugEnabled()) {
                    LOG.debug(mapper.writeValueAsString(testProfileDefinition));
                }
                testProfileRunner.run(); // TODO implement test reporting
            } catch (JsonMappingException | JsonParseException e) {
                errorMessage = "Unable to parse " + config + ": " + e.getOriginalMessage();
                displayHelp = true;
            } catch (IOException e) {
                errorMessage = "Unable to load " + config + ": " + e.getMessage();
                displayHelp = true;
            }
        } catch (ParseException e) {
            displayHelp = true;
        }

        if (displayHelp) {
            HelpFormatter formatter = new HelpFormatter();
            if (errorMessage != null) {
                System.out.println(errorMessage + System.lineSeparator());
            }
            formatter.printHelp("kafka-perf-test", options);
        }

    }

    private static boolean isYamlFile(String fileName) {
        assert (fileName != null);
        return (fileName.endsWith("yml") || fileName.endsWith("yaml"));
    }
}
