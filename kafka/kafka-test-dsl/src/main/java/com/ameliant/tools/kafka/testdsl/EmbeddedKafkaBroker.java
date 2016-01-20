package com.ameliant.tools.kafka.testdsl;

import static com.ameliant.tools.support.DirectoryUtils.*;

import kafka.admin.AdminUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.SystemTime$;
import kafka.utils.ZkUtils;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.security.JaasUtils;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Some$;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * @author jkorab
 */
public class EmbeddedKafkaBroker extends ExternalResource {
    private Logger log = LoggerFactory.getLogger(this.getClass());

    private final BrokerDefinition brokerDefinition;
    private KafkaServer kafkaServer;
    private File logDirectory;

    public static BrokerBuilder builder() {
        return new BrokerBuilder();
    }

    EmbeddedKafkaBroker(BrokerDefinition brokerDefinition) {
        this.brokerDefinition = brokerDefinition;
    }

    public int getPort() {
        return brokerDefinition.getPort();
    }

    public String getConnectionString() {
        return "127.0.0.1:" + getPort();
    }

    @Override
    protected void before() throws Throwable {
        logDirectory = tempDir(perTest("kafka-log"));
        Properties properties = brokerDefinition.getProperties();
        properties.setProperty(KafkaConfig.LogDirProp(), logDirectory.getCanonicalPath());
        kafkaServer = new KafkaServer(new KafkaConfig(properties),
                SystemTime$.MODULE$, Some$.MODULE$.apply("kafkaServer"));
        kafkaServer.startup();

        List<TopicDefinition> topicDefinitions = brokerDefinition.getTopicDefinitions();
        if (!topicDefinitions.isEmpty()) {
            ZkUtils zkUtils = ZkUtils.apply(brokerDefinition.getZookeeperConnect(), 30000, 30000,
                    JaasUtils.isZkSecurityEnabled());
            for (TopicDefinition topicDefinition : topicDefinitions) {
                String name = topicDefinition.getName();
                log.info("Creating topic {}", name);
                AdminUtils.createTopic(zkUtils,
                        name,
                        topicDefinition.getPartitions(),
                        topicDefinition.getReplicationFactor(),
                        topicDefinition.getProperties());
            }
        }
    }

    @Override
    protected void after() {
        kafkaServer.shutdown();
        kafkaServer.awaitShutdown();

        try {
            log.info("Deleting {}", logDirectory);
            FileUtils.deleteDirectory(logDirectory);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        kafkaServer.awaitShutdown();
    }

}
