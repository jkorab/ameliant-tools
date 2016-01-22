An implementation of a simplified listener-based interface to Apache Kafka that deals with the heavy lifting required 
to emulate JMS once-only delivery of messages.

This is a *Work In Progress*, and should not be used for any production code.

This construct keeps tabs on its own cursor position through a configurable ``OffsetStore`` instance, compensating for 
the ``KafkaConsumer`` API's batch polling functionality by rewinding the cursor to the last known successfully 
processed read on startup. The only ``OffsetStore`` implementation at the moment is a ``MemoryOffsetStore``, which keeps
state within a single VM only, and not between restarts, making it useful for testing only.

An instance may be constructed via the following constructor:

```java

    public KafkaMessageListenerContainer(Properties kafkaConfig,
                                         OffsetStore offsetStore,
                                         String topic,
                                         BiConsumer<K, V> messageListener);
                                         
```

A ``KafkaMessageListenerContainer`` is started through a call to ``init()``, and shut down through ``close()``.

As Kafka has no concept of redelivery or dead-letter queues, ``KafkaMessageListenerContainer`` also has the ability 
to pass any exceptions thrown to an exception handler, allowing you to add dead-letter channel functionality:

```java

    // this signature will change
    public void setExceptionHandler(BiConsumer<Tuple2<K, V>, Exception> exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
    }

```

There is a fluent builder associated with this class that enables you to define the container inline. Sample usage:

```java
        
        final CountDownLatch latch = new CountDownLatch(1); // receive one message and shut down

        KafkaMessageListenerContainer.Builder<byte[], byte[]> builder = 
                new KafkaMessageListenerContainer.Builder<byte[], byte[]>()
                .kafkaConfig(configs)
                .offsetStore(offsetStore)
                .topic(TOPIC)
                .messageListener((key, value) -> {
                    log.info("Received message [{}, {}]", key, value);
                    latch.countDown()
                });

        try (KafkaMessageListenerContainer<byte[], byte[]> container = builder.build()) {
            container.init();
            if (!latch.await(20, TimeUnit.SECONDS)) {
                fail("Timeout expired waiting on latch");
            }
        }

```
