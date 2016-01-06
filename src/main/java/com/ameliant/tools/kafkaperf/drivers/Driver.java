package com.ameliant.tools.kafkaperf.drivers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jkorab
 */
public abstract class Driver implements Runnable {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * Flag marking whether shutdown has been requested.
     */
    private boolean shutdownRequested = false;

    public void requestShutdown() {
        shutdownRequested = true;
    }

    protected boolean isShutdownRequested() {
        return shutdownRequested;
    }

    @Override
    public void run() {
        try {
            drive();
        } catch (Exception ex) {
            log.error("Caught exception: {}", ex);
            throw ex;
        }
    }

    protected abstract void drive();
}
