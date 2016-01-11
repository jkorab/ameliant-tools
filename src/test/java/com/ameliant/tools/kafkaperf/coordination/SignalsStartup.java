package com.ameliant.tools.kafkaperf.coordination;

import com.ameliant.tools.kafkaperf.drivers.ConsumerDriver;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author jkorab
 */
public class SignalsStartup implements Runnable {

    private final CountDownLatch latch;
    private final Runnable runnable;

    public SignalsStartup(Runnable runnable, CountDownLatch latch) {
        this.runnable = runnable;
        this.latch = latch;
    }

    @Override
    public void run() {
        latch.countDown();
        runnable.run();
    }
}
