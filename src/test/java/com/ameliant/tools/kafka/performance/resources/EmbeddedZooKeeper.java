package com.ameliant.tools.kafka.performance.resources;

import static com.ameliant.tools.kafka.performance.util.DirectoryUtils.*;
import com.ameliant.tools.kafka.performance.util.AvailablePortFinder;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @author jkorab
 */
public class EmbeddedZooKeeper extends ExternalResource {

    private Logger log = LoggerFactory.getLogger(this.getClass()); // TODO

    private final int port;
    private ZooKeeperServer zooKeeperServer;
    private ServerCnxnFactory cnxnFactory;
    private File snapshotDir;
    private File logDir;

    public EmbeddedZooKeeper() {
        this.port = AvailablePortFinder.getNextAvailable();
    }

    public int getPort() {
        return port;
    }

    @Override
    protected void before() throws Throwable {
        snapshotDir = tempDir(perTest("zk-snapshot"));
        logDir = tempDir(perTest("zk-log"));
        log.info("Setting up ZK Server with snapshotDir:{}, logDir:{}", snapshotDir, logDir);

        int tickTime = 500;
        try {
            zooKeeperServer = new ZooKeeperServer(snapshotDir, logDir, tickTime);
            cnxnFactory = new NIOServerCnxnFactory();
            cnxnFactory.configure(new InetSocketAddress("127.0.0.1", port), 0);
            cnxnFactory.startup(zooKeeperServer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void after() {
        cnxnFactory.shutdown();
        zooKeeperServer.shutdown();

        try {
            log.info("Deleting {}", snapshotDir);
            FileUtils.deleteDirectory(snapshotDir);
            log.info("Deleting {}", logDir);
            FileUtils.deleteDirectory(logDir);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
