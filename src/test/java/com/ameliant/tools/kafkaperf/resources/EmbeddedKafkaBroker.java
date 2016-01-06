package com.ameliant.tools.kafkaperf.resources;

import static com.ameliant.tools.kafkaperf.util.DirectoryUtils.*;

import com.ameliant.tools.kafkaperf.util.AvailablePortFinder;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.SystemTime$;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.Validate;
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
        private Long logFlushIntervalMessages;

        public Builder brokerId(int brokerId) {
            this.brokerId = brokerId;
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

        private int numPartitions = 1;

        public Builder numPartitions(int numPartitions) {
            Validate.isTrue(numPartitions > 0, "numPartitions must be greater than 0");
            this.numPartitions = numPartitions;
            return this;
        }

        public EmbeddedKafkaBroker build() {
            Properties props = new Properties();
            props.setProperty(KafkaConfig.HostNameProp(), hostname);
            props.setProperty(KafkaConfig.PortProp(), Integer.toString(port));
            props.setProperty(KafkaConfig.BrokerIdProp(), Integer.toString(brokerId));
            props.setProperty(KafkaConfig.ZkConnectProp(), zookeeperConnect);
            props.setProperty(KafkaConfig.NumPartitionsProp(), Integer.toString(numPartitions));

            if (logFlushIntervalMessages != null) {
                props.setProperty(KafkaConfig.LogFlushIntervalMessagesProp(), logFlushIntervalMessages.toString());
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
        properties.setProperty(KafkaConfig.LogDirProp(), logDirectory.getCanonicalPath());
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
