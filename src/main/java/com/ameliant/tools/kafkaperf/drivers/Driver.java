package com.ameliant.tools.kafkaperf.drivers;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author jkorab
 */
public interface Driver extends Runnable {

    AtomicBoolean shutDown = new AtomicBoolean(false);

    default void flagShutdown() {
        shutDown.set(true);
    }
}
