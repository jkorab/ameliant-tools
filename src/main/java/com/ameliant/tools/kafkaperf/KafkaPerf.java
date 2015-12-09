package com.ameliant.tools.kafkaperf;

import com.ameliant.tools.kafkaperf.config.TestProfileDefinition;
import com.ameliant.tools.kafkaperf.drivers.TestProfileRunner;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Hello world!
 *
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
                ObjectMapper mapper = new ObjectMapper();
                TestProfileDefinition testProfileDefinition = mapper.readValue(new File(config), TestProfileDefinition.class);
                TestProfileRunner driver = new TestProfileRunner(testProfileDefinition);

                if (LOG.isDebugEnabled()) {
                    LOG.debug(mapper.writeValueAsString(testProfileDefinition));
                }
                driver.run(); // TODO implement test reporting
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
}
