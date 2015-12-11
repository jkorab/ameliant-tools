package com.ameliant.tools.kafkaperf.drivers;

/**
 * @author jkorab
 */
public abstract class Driver implements Runnable {

    private boolean shuttingDown = false;

    public void markShuttingDown() {
        shuttingDown = true;
    }

    protected boolean isShuttingDown() {
        return shuttingDown;
    }
}
