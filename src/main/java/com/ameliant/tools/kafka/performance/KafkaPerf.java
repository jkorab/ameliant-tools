package com.ameliant.tools.kafka.performance;

import com.ameliant.tools.kafka.performance.config.TestProfileDefinition;
import com.ameliant.tools.kafka.performance.drivers.TestProfileRunner;
import com.ameliant.tools.kafka.performance.util.PayloadDetector;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
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
 * object graph representing the test profile ({@link com.ameliant.tools.kafka.performance.config.TestProfileDefinition}).
 *
 * The test profile is then executed by a {@link com.ameliant.tools.kafka.performance.drivers.TestProfileRunner},
 * which runs each of the producers and consumers in their own threads.
 *
 * @author jkorab
 */
public class KafkaPerf {

    private final static Logger LOG = LoggerFactory.getLogger(KafkaPerf.class);

    public static final String CONFIG = "config";
    public static final String OUTPUT_FORMAT = "output-format";

    public static void main(String[] args) {
        CommandLineParser parser = new DefaultParser();

        Options options = new Options();
        options.addOption(Option.builder("c")
                .longOpt(CONFIG)
                .desc("config file that defines the test profile(s) to run")
                .hasArg()
                .argName("FILE")
                .required(true)
                .build());

        options.addOption(Option.builder("o")
                .longOpt(OUTPUT_FORMAT)
                .desc("the format of the parsed config to echo to console")
                .hasArg()
                .argName("yaml|json")
                .required(false)
                .build());

        boolean displayHelp = false;
        String errorMessage = null;

        try {
            CommandLine commandLine = parser.parse(options, args);
            String config = commandLine.getOptionValue(CONFIG);
            try {
                ObjectMapper mapper = PayloadDetector.isYamlFile(config) ? new ObjectMapper(new YAMLFactory())
                    : new ObjectMapper();
                TestProfileDefinition testProfileDefinition = mapper.readValue(new File(config), TestProfileDefinition.class);
                TestProfileRunner testProfileRunner = new TestProfileRunner(testProfileDefinition);

                echoTestProfileDefinition(testProfileDefinition, commandLine.getOptionValue(OUTPUT_FORMAT));
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

    private static void echoTestProfileDefinition(TestProfileDefinition testProfileDefinition, String outputFormat)
            throws JsonProcessingException {
        if (outputFormat != null) {
            ObjectMapper mapper = PayloadDetector.isYaml(outputFormat) ? new ObjectMapper(new YAMLFactory())
                    : new ObjectMapper();

            LOG.info(System.lineSeparator() +
                    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(testProfileDefinition));
        }
    }

}
