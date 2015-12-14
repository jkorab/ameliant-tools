package com.ameliant.tools.kafkaperf.config;

/**
 * @author jkorab
 */
public class ConsumerDefinition extends ConfigurableWithParent {

    private long messagesToReceive = 10000;
    private long pollTimeout = 1000;
    private int reportReceivedEvery = 1000;
    private int receiveDelay = 0;

    private long testRunTimeout = Long.MAX_VALUE;

    public long getMessagesToReceive() {
        return messagesToReceive;
    }

    public void setMessagesToReceive(long messagesToReceive) {
        this.messagesToReceive = messagesToReceive;
    }

    public long getPollTimeout() {
        return pollTimeout;
    }

    public void setPollTimeout(long pollTimeout) {
        this.pollTimeout = pollTimeout;
    }

    public long getTestRunTimeout() {
        return testRunTimeout;
    }

    public void setTestRunTimeout(long testRunTimeout) {
        this.testRunTimeout = testRunTimeout;
    }

    public int getReportReceivedEvery() {
        return reportReceivedEvery;
    }

    public void setReportReceivedEvery(int reportReceivedEvery) {
        this.reportReceivedEvery = reportReceivedEvery;
    }

    public int getReceiveDelay() {
        return receiveDelay;
    }

    public void setReceiveDelay(int receiveDelay) {
        this.receiveDelay = receiveDelay;
    }
}
