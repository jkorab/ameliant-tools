package com.ameliant.tools.kafka.perftool.coordination;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author jkorab
 */
public class AwaitsStartup implements Runnable {

    private final CountDownLatch latch;
    private final Runnable runnable;

    public AwaitsStartup(Runnable runnable, CountDownLatch latch) {
        this.runnable = runnable;
        this.latch = latch;
    }

    @Override
    public void run() {
        try {
            if (latch.await(10, TimeUnit.SECONDS)) {
                runnable.run();
            } else {
                throw new IllegalStateException("Timeout reached waiting for startup signal");
            }
        } catch (InterruptedException iex) {
            throw new RuntimeException(iex);
        }
    }
}
