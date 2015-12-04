package com.ameliant.tools.kafkaperf.resources;

import static com.ameliant.tools.kafkaperf.util.DirectoryUtils.*;

import com.ameliant.tools.kafkaperf.util.AvailablePortFinder;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.SystemTime$;
import org.apache.commons.io.FileUtils;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Some;
import scala.Some$;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * @author jkorab
 */
public class EmbeddedKafkaBroker extends ExternalResource {
    private Logger log = LoggerFactory.getLogger(this.getClass());

    private final Properties properties;
    private final int port;
    private KafkaServer kafkaServer;
    private File logDirectory;

    public static class Builder {

        private String hostname = "localhost";
        private int port = AvailablePortFinder.getNextAvailable();
        private int brokerId = 1;
        private boolean enableZookeeper = false;
        private Long logFlushIntervalMessages;

        public Builder brokerId(int brokerId) {
            this.brokerId = brokerId;
            return this;
        }

        public Builder enableZookeeper(boolean enableZookeeper) {
            this.enableZookeeper = enableZookeeper;
            return this;
        }

        public Builder hostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder logFlushIntervalMessages(long logFlushIntervalMessages) {
            this.logFlushIntervalMessages = logFlushIntervalMessages;
            return this;
        }

        private String zookeeperConnect = "";

        public Builder zookeeperConnect(String zookeeperConnect) {
            this.zookeeperConnect = zookeeperConnect;
            return this;
        }

        public EmbeddedKafkaBroker build() {
            Properties props = new Properties();
            props.setProperty("hostname", hostname);
            props.setProperty("port", Integer.toString(port));
            props.setProperty("broker.id", Integer.toString(brokerId));
            props.setProperty("enable.zookeeper", Boolean.toString(enableZookeeper));
            props.setProperty("zookeeper.connect", zookeeperConnect);

            if (logFlushIntervalMessages != null) {
                props.setProperty("log.flush.interval.messages", logFlushIntervalMessages.toString());
            }

            return new EmbeddedKafkaBroker(props, port);
        }

    }

    public EmbeddedKafkaBroker(Properties properties, Integer port) {
        this.properties = properties;
        this.port = port;
    }

    public int getPort() {
        return port;
    }

    @Override
    protected void before() throws Throwable {
        logDirectory = tempDir(perTest("kafka-log"));
        properties.setProperty("log.dir", logDirectory.getCanonicalPath());
        kafkaServer = new KafkaServer(new KafkaConfig(properties), SystemTime$.MODULE$, Some$.MODULE$.apply("kafkaServer"));
        kafkaServer.startup();
    }

    @Override
    protected void after() {
        kafkaServer.shutdown();

        try {
            log.info("Deleting {}", logDirectory);
            FileUtils.deleteDirectory(logDirectory);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        kafkaServer.awaitShutdown();
    }
}
